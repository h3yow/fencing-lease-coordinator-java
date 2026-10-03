package com.github.h3yow.fencingleasecoordinatorjava.tests;

import com.github.h3yow.fencingleasecoordinatorjava.config.LeaseProperties;
import com.github.h3yow.fencingleasecoordinatorjava.dto.AcquireRequest;
import com.github.h3yow.fencingleasecoordinatorjava.dto.LeaseResponse;
import com.github.h3yow.fencingleasecoordinatorjava.dto.ReleaseRequest;
import com.github.h3yow.fencingleasecoordinatorjava.error.LeaseErrorCode;
import com.github.h3yow.fencingleasecoordinatorjava.error.LeaseException;
import com.github.h3yow.fencingleasecoordinatorjava.service.DefaultLeaseCoordinator;
import com.github.h3yow.fencingleasecoordinatorjava.service.SystemMillisClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Waiting-acquire semantics. These tests use the real {@link SystemMillisClock}
 * because waiting involves real blocking; waits are kept short (100ms-3s).
 */
class LeaseWaitTest {

    private static final Logger log = LoggerFactory.getLogger(LeaseWaitTest.class);

    private InMemoryLeaseStore store;
    private LeaseProperties props;
    private DefaultLeaseCoordinator coordinator;
    private ExecutorService pool;

    @BeforeEach
    void setUp() {
        store = new InMemoryLeaseStore();
        props = new LeaseProperties();
        props.setDefaultTtlMillis(1000);
        props.setMinTtlMillis(10);
        props.setMaxTtlMillis(3_600_000);
        props.setMaxWaitMillis(5_000);
        props.setMaxWaitQueueSize(64);
        coordinator = new DefaultLeaseCoordinator(store, props, new SystemMillisClock());
        pool = Executors.newCachedThreadPool();
    }

    private static String rid() {
        return UUID.randomUUID().toString();
    }

    private static AcquireRequest acquire(String requestId, String resource, String holder,
                                          Long ttl, Long waitMillis) {
        AcquireRequest r = new AcquireRequest();
        r.setRequestId(requestId);
        r.setResourceId(resource);
        r.setHolderId(holder);
        r.setTtlMillis(ttl);
        r.setWaitMillis(waitMillis);
        return r;
    }

    private static ReleaseRequest release(String requestId, String resource, String holder, Long token) {
        ReleaseRequest r = new ReleaseRequest();
        r.setRequestId(requestId);
        r.setResourceId(resource);
        r.setHolderId(holder);
        r.setToken(token);
        return r;
    }

    private Future<LeaseResponse> submit(AcquireRequest req) {
        return pool.submit(() -> coordinator.acquire(req));
    }

    private void awaitQueueSize(String resource, int expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (coordinator.waitQueueSize(resource) != expected) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("queue size for " + resource + " never became " + expected
                        + " (actual " + coordinator.waitQueueSize(resource) + ")");
            }
            Thread.sleep(10);
        }
    }

    private static void logResponse(String scenario, LeaseResponse r) {
        log.info("[{}] requestId={} action={} holder={} token={} currentToken={} success={} replay={} "
                        + "errorCode={} decision='{}'",
                scenario, r.requestId(), r.action(), r.holderId(), r.token(), r.currentToken(),
                r.success(), r.replay(), r.errorCode(), r.decision());
    }

    @Test
    @DisplayName("多个消费者并发等待同一资源: 按 FIFO 顺序依次获得, 令牌严格递增")
    void waitersAreGrantedInFifoOrderWithMonotonicTokens() throws Exception {
        String resource = "res-wait-fifo";
        LeaseResponse a = coordinator.acquire(acquire(rid(), resource, "A", 10_000L, null));
        logResponse("fifo-A-acquire", a);
        assertEquals(1L, a.token());

        // staggered submission + queue-size barriers => deterministic enqueue order W1,W2,W3
        Future<LeaseResponse> w1 = submit(acquire("w1", resource, "W1", 10_000L, 3_000L));
        awaitQueueSize(resource, 1);
        Future<LeaseResponse> w2 = submit(acquire("w2", resource, "W2", 10_000L, 3_000L));
        awaitQueueSize(resource, 2);
        Future<LeaseResponse> w3 = submit(acquire("w3", resource, "W3", 10_000L, 3_000L));
        awaitQueueSize(resource, 3);

        LeaseResponse relA = coordinator.release(release(rid(), resource, "A", 1L));
        logResponse("fifo-release-A", relA);
        LeaseResponse g1 = w1.get(5, TimeUnit.SECONDS);
        logResponse("fifo-W1-granted", g1);
        assertTrue(g1.success());
        assertEquals(2L, g1.token(), "first waiter gets token 2 after release");
        assertFalse(w2.isDone(), "second waiter must still be queued while W1 holds the lease");
        assertFalse(w3.isDone(), "third waiter must still be queued while W1 holds the lease");

        LeaseResponse rel1 = coordinator.release(release(rid(), resource, "W1", 2L));
        logResponse("fifo-release-W1", rel1);
        LeaseResponse g2 = w2.get(5, TimeUnit.SECONDS);
        logResponse("fifo-W2-granted", g2);
        assertTrue(g2.success());
        assertEquals(3L, g2.token(), "second waiter gets token 3, strictly increasing");

        LeaseResponse rel2 = coordinator.release(release(rid(), resource, "W2", 3L));
        logResponse("fifo-release-W2", rel2);
        LeaseResponse g3 = w3.get(5, TimeUnit.SECONDS);
        logResponse("fifo-W3-granted", g3);
        assertTrue(g3.success());
        assertEquals(4L, g3.token());

        LeaseResponse state = coordinator.inspect(resource);
        logResponse("fifo-final-state", state);
        assertEquals(4L, state.token());
        assertEquals("W3", state.holderId());
    }

    @Test
    @DisplayName("等待超时: WAIT_TIMEOUT 可区分且可重放, 超时方不会事后抢走租约")
    void waitTimeoutIsDistinctReplayableAndDoesNotSteal() throws Exception {
        String resource = "res-wait-timeout";
        LeaseResponse a = coordinator.acquire(acquire(rid(), resource, "A", 10_000L, null));
        logResponse("timeout-A-acquire", a);

        String waitReq = "wait-req-timeout";
        long started = System.currentTimeMillis();
        LeaseResponse timedOut = coordinator.acquire(acquire(waitReq, resource, "W", 1_000L, 200L));
        long elapsed = System.currentTimeMillis() - started;
        logResponse("timeout-W", timedOut);
        assertFalse(timedOut.success());
        assertEquals(LeaseErrorCode.WAIT_TIMEOUT, timedOut.errorCode());
        assertEquals(408, timedOut.errorCode().httpStatus());
        assertTrue(elapsed >= 150, "should actually wait, elapsed=" + elapsed);
        assertTrue(elapsed < 3_000, "must not hang, elapsed=" + elapsed);

        LeaseResponse replay = coordinator.acquire(acquire(waitReq, resource, "W", 1_000L, 200L));
        logResponse("timeout-W-replay", replay);
        assertTrue(replay.replay());
        assertFalse(replay.success());
        assertEquals(LeaseErrorCode.WAIT_TIMEOUT, replay.errorCode());
        assertEquals(timedOut.reason(), replay.reason());

        LeaseException conflict = assertThrows(LeaseException.class,
                () -> coordinator.acquire(acquire(waitReq, resource, "W-other", 1_000L, 200L)));
        log.warn("timeout-reuse-different-content code={} message='{}'",
                conflict.getErrorCode(), conflict.getMessage());
        assertEquals(LeaseErrorCode.REQUEST_CONFLICT, conflict.getErrorCode());
        assertEquals(409, conflict.getErrorCode().httpStatus());

        LeaseResponse state = coordinator.inspect(resource);
        logResponse("timeout-final-state", state);
        assertEquals(1L, state.token(), "timed-out waiter must never grab the lease later");
        assertEquals("A", state.holderId());

        LeaseResponse immediate = coordinator.acquire(acquire(rid(), resource, "W", 1_000L, null));
        logResponse("timeout-W-immediate-after", immediate);
        assertFalse(immediate.success());
        assertEquals(LeaseErrorCode.CONFLICT, immediate.errorCode());
    }

    @Test
    @DisplayName("排队人数满: QUEUE_FULL 可区分且可重放, 释放后队首被唤醒")
    void queueFullIsDistinctAndHeadIsWokenOnRelease() throws Exception {
        props.setMaxWaitQueueSize(2);
        String resource = "res-wait-full";
        LeaseResponse a = coordinator.acquire(acquire(rid(), resource, "A", 10_000L, null));
        logResponse("full-A-acquire", a);

        Future<LeaseResponse> w1 = submit(acquire("full-w1", resource, "W1", 10_000L, 3_000L));
        awaitQueueSize(resource, 1);
        Future<LeaseResponse> w2 = submit(acquire("full-w2", resource, "W2", 10_000L, 3_000L));
        awaitQueueSize(resource, 2);

        String fullReq = "full-w3";
        long started = System.currentTimeMillis();
        LeaseResponse full = coordinator.acquire(acquire(fullReq, resource, "W3", 1_000L, 1_000L));
        long elapsed = System.currentTimeMillis() - started;
        logResponse("full-W3-rejected", full);
        assertFalse(full.success());
        assertEquals(LeaseErrorCode.QUEUE_FULL, full.errorCode());
        assertEquals(429, full.errorCode().httpStatus());
        assertTrue(elapsed < 500, "queue-full must be immediate, elapsed=" + elapsed);

        LeaseResponse replay = coordinator.acquire(acquire(fullReq, resource, "W3", 1_000L, 1_000L));
        logResponse("full-W3-replay", replay);
        assertTrue(replay.replay());
        assertEquals(LeaseErrorCode.QUEUE_FULL, replay.errorCode());

        coordinator.release(release(rid(), resource, "A", 1L));
        LeaseResponse g1 = w1.get(5, TimeUnit.SECONDS);
        logResponse("full-W1-granted", g1);
        assertTrue(g1.success());
        assertEquals(2L, g1.token());

        coordinator.release(release(rid(), resource, "W1", 2L));
        LeaseResponse g2 = w2.get(5, TimeUnit.SECONDS);
        logResponse("full-W2-granted", g2);
        assertTrue(g2.success());
        assertEquals(3L, g2.token());
    }

    @Test
    @DisplayName("持有者到期后等待者被唤醒并获得更高令牌")
    void waiterIsWokenOnExpiryWithAdvancedToken() throws Exception {
        String resource = "res-wait-expiry";
        LeaseResponse a = coordinator.acquire(acquire(rid(), resource, "A", 200L, null));
        logResponse("expiry-A-acquire", a);
        assertEquals(1L, a.token());

        long started = System.currentTimeMillis();
        LeaseResponse granted = coordinator.acquire(acquire("expiry-w", resource, "W", 1_000L, 3_000L));
        long elapsed = System.currentTimeMillis() - started;
        logResponse("expiry-W-granted", granted);
        assertTrue(granted.success());
        assertEquals(2L, granted.token(), "token must advance past the expired holder");
        assertTrue(elapsed >= 150, "should wait until expiry, elapsed=" + elapsed);
        assertTrue(elapsed < 3_000, "must not wait the whole budget, elapsed=" + elapsed);

        LeaseResponse state = coordinator.inspect(resource);
        logResponse("expiry-final-state", state);
        assertEquals(2L, state.token());
        assertEquals("W", state.holderId());
    }

    @Test
    @DisplayName("等待请求的幂等重放: 成功结果一致, 不会重复获取或推高令牌")
    void waitingAcquireReplayReturnsSameGrant() throws Exception {
        String resource = "res-wait-idem";
        LeaseResponse a = coordinator.acquire(acquire(rid(), resource, "A", 10_000L, null));
        logResponse("idem-A-acquire", a);

        String waitReq = "idem-wait";
        Future<LeaseResponse> w = submit(acquire(waitReq, resource, "W", 10_000L, 3_000L));
        awaitQueueSize(resource, 1);
        coordinator.release(release(rid(), resource, "A", 1L));

        LeaseResponse granted = w.get(5, TimeUnit.SECONDS);
        logResponse("idem-W-granted", granted);
        assertTrue(granted.success());
        assertEquals(2L, granted.token());
        assertFalse(granted.replay());

        LeaseResponse replay = coordinator.acquire(acquire(waitReq, resource, "W", 10_000L, 3_000L));
        logResponse("idem-W-replay", replay);
        assertTrue(replay.replay());
        assertTrue(replay.success());
        assertEquals(2L, replay.token(), "replay must not acquire again or push the token");

        LeaseResponse state = coordinator.inspect(resource);
        logResponse("idem-final-state", state);
        assertEquals(2L, state.token());
        assertEquals("W", state.holderId());
    }

    @Test
    @DisplayName("等待期间存储抖动: 归一化为 TEMPORARILY_UNAVAILABLE 且不污染幂等记录")
    void storageFaultDuringWaitIsTransientAndNotRecorded() throws Exception {
        String resource = "res-wait-fault";
        String waitReq = "fault-wait";
        store.faultOnPersist = true;

        LeaseException ex = assertThrows(LeaseException.class,
                () -> coordinator.acquire(acquire(waitReq, resource, "W", 1_000L, 500L)));
        log.warn("fault-during-wait code={} message='{}'", ex.getErrorCode(), ex.getMessage());
        assertEquals(LeaseErrorCode.TEMPORARILY_UNAVAILABLE, ex.getErrorCode());
        assertEquals(503, ex.getErrorCode().httpStatus());
        assertTrue(store.findIdempotent(waitReq).isEmpty(),
                "transient failure must not pollute the idempotency log");
        assertEquals(0, coordinator.waitQueueSize(resource),
                "failed waiter must leave the queue");

        store.faultOnPersist = false;
        LeaseResponse retry = coordinator.acquire(acquire(waitReq, resource, "W", 1_000L, 500L));
        logResponse("fault-retry-after-recovery", retry);
        assertTrue(retry.success(), "same requestId must be retryable after a transient fault");
        assertFalse(retry.replay());
        assertEquals(1L, retry.token());
    }

    @Test
    @DisplayName("不传 waitMillis 的老调用保持即时 CONFLICT 行为")
    void immediateAcquireSemanticsUnchanged() {
        String resource = "res-wait-compat";
        LeaseResponse a = coordinator.acquire(acquire(rid(), resource, "A", 10_000L, null));
        assertTrue(a.success());

        LeaseResponse b = coordinator.acquire(acquire(rid(), resource, "B", 1_000L, null));
        logResponse("compat-immediate-conflict", b);
        assertFalse(b.success());
        assertEquals(LeaseErrorCode.CONFLICT, b.errorCode());
        assertNull(b.token());
    }
}
