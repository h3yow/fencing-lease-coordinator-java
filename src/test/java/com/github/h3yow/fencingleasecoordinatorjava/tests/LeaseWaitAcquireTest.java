package com.github.h3yow.fencingleasecoordinatorjava.tests;

import com.github.h3yow.fencingleasecoordinatorjava.config.LeaseProperties;
import com.github.h3yow.fencingleasecoordinatorjava.dto.AcquireRequest;
import com.github.h3yow.fencingleasecoordinatorjava.dto.LeaseResponse;
import com.github.h3yow.fencingleasecoordinatorjava.dto.ReleaseRequest;
import com.github.h3yow.fencingleasecoordinatorjava.dto.WriteRequest;
import com.github.h3yow.fencingleasecoordinatorjava.error.LeaseErrorCode;
import com.github.h3yow.fencingleasecoordinatorjava.error.LeaseException;
import com.github.h3yow.fencingleasecoordinatorjava.service.DefaultLeaseCoordinator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LeaseWaitAcquireTest {

    private static final Logger log = LoggerFactory.getLogger(LeaseWaitAcquireTest.class);

    private InMemoryLeaseStore store;
    private MutableMillisClock clock;
    private LeaseProperties props;
    private DefaultLeaseCoordinator coordinator;

    @BeforeEach
    void setUp() {
        store = new InMemoryLeaseStore();
        clock = new MutableMillisClock();
        props = new LeaseProperties();
        props.setDefaultTtlMillis(1000);
        props.setMinTtlMillis(10);
        props.setMaxTtlMillis(3_600_000);
        props.setMaxWaitMillis(30_000);
        props.setMaxWaitersPerResource(8);
        props.setMaxTotalWaiters(64);
        coordinator = new DefaultLeaseCoordinator(store, props, clock);
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

    private static void logResponse(String scenario, LeaseResponse r) {
        log.info("[{}] requestId={} action={} holder={} token={} currentToken={} success={} replay={} "
                        + "errorCode={} decision='{}'",
                scenario, r.requestId(), r.action(), r.holderId(), r.token(), r.currentToken(),
                r.success(), r.replay(), r.errorCode(), r.decision());
    }

    private void awaitQueueSize(String resource, int expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (coordinator.pendingWaiters(resource) != expected) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("queue of " + resource + " did not reach size " + expected
                        + " (actual " + coordinator.pendingWaiters(resource) + ")");
            }
            Thread.sleep(10);
        }
    }

    @Test
    @DisplayName("多个消费者并发等待同一资源: 按 FIFO 顺序依次拿到, 令牌严格递增")
    void concurrentWaitersWinInFifoOrderWithMonotonicTokens() throws Exception {
        String resource = "res-wait-fifo";
        int n = 4;

        LeaseResponse owner = coordinator.acquire(acquire(rid(), resource, "owner", 60_000L, null));
        logResponse("fifo-owner-acquire", owner);
        assertTrue(owner.success());
        assertEquals(1L, owner.token());

        ExecutorService pool = Executors.newFixedThreadPool(n);
        List<Future<LeaseResponse>> futures = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            final String holder = "waiter-" + i;
            final String requestId = "fifo-req-" + i;
            futures.add(pool.submit(() -> coordinator.acquire(
                    acquire(requestId, resource, holder, 60_000L, 20_000L))));
            awaitQueueSize(resource, i + 1); // enforce deterministic enqueue order
        }

        long expectedToken = owner.token();
        String currentHolder = "owner";
        for (int i = 0; i < n; i++) {
            LeaseResponse rel = coordinator.release(
                    release(rid(), resource, currentHolder, expectedToken));
            logResponse("fifo-release-round-" + i, rel);
            assertTrue(rel.success());

            LeaseResponse granted = futures.get(i).get(5, TimeUnit.SECONDS);
            logResponse("fifo-granted-round-" + i, granted);
            expectedToken++;
            assertTrue(granted.success(), "waiter-" + i + " should win round " + i);
            assertEquals("waiter-" + i, granted.holderId(), "grants must follow FIFO enqueue order");
            assertEquals(expectedToken, granted.token(), "token must advance monotonically");
            currentHolder = granted.holderId();
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));

        LeaseResponse state = coordinator.inspect(resource);
        logResponse("fifo-final-state", state);
        assertEquals(expectedToken, state.token());
        assertEquals(0, coordinator.pendingWaiters(resource), "queue must be drained at the end");
    }

    @Test
    @DisplayName("等待超时: 返回可与 CONFLICT/存储故障区分的 WAIT_TIMEOUT, 结果可幂等重放, 超时方不会再抢到租约")
    void waitTimeoutIsDistinctRecordedAndReplayable() throws Exception {
        String resource = "res-wait-timeout";
        LeaseResponse owner = coordinator.acquire(acquire(rid(), resource, "owner", 60_000L, null));
        assertTrue(owner.success());

        String requestId = "timeout-req-1";
        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<LeaseResponse> waiting = pool.submit(() -> coordinator.acquire(
                acquire(requestId, resource, "impatient", 1_000L, 300L)));
        awaitQueueSize(resource, 1);

        Thread.sleep(200); // let it park a little; mock clock does not advance by itself
        clock.advance(400);  // push mock time past the wait deadline

        LeaseResponse timedOut = waiting.get(5, TimeUnit.SECONDS);
        logResponse("timeout-first", timedOut);
        pool.shutdown();

        assertFalse(timedOut.success());
        assertEquals(LeaseErrorCode.WAIT_TIMEOUT, timedOut.errorCode());
        assertNotEquals(LeaseErrorCode.CONFLICT, timedOut.errorCode());
        assertNotEquals(LeaseErrorCode.TEMPORARILY_UNAVAILABLE, timedOut.errorCode());
        assertNull(timedOut.token());
        assertEquals(owner.token(), timedOut.currentToken());
        assertFalse(timedOut.replay());

        // Same requestId + same content -> identical recorded WAIT_TIMEOUT, replay=true
        LeaseResponse replay = coordinator.acquire(acquire(requestId, resource, "impatient", 1_000L, 300L));
        logResponse("timeout-replay", replay);
        assertFalse(replay.success());
        assertTrue(replay.replay());
        assertEquals(LeaseErrorCode.WAIT_TIMEOUT, replay.errorCode());
        assertEquals(timedOut.reason(), replay.reason());
        assertEquals(timedOut.decision(), replay.decision());

        // The timed-out waiter must not grab the lease afterwards, even once it is free.
        LeaseResponse rel = coordinator.release(release(rid(), resource, "owner", owner.token()));
        assertTrue(rel.success());
        LeaseResponse afterRelease = coordinator.acquire(
                acquire(requestId, resource, "impatient", 1_000L, 300L));
        logResponse("timeout-after-release-same-requestId", afterRelease);
        assertFalse(afterRelease.success());
        assertEquals(LeaseErrorCode.WAIT_TIMEOUT, afterRelease.errorCode(),
                "recorded timeout must be replayed; the stale waiter can never win retroactively");

        LeaseResponse state = coordinator.inspect(resource);
        assertEquals(owner.token(), state.token(), "no grant may happen for the timed-out request");
        assertEquals(0, coordinator.pendingWaiters(resource));
    }

    @Test
    @DisplayName("排队人数满: 新等待请求以 QUEUE_FULL 明确拒绝, 且不污染幂等记录")
    void queueFullIsDistinctAndNotRecorded() throws Exception {
        props.setMaxWaitersPerResource(2);
        coordinator = new DefaultLeaseCoordinator(store, props, clock);

        String resource = "res-wait-full";
        LeaseResponse owner = coordinator.acquire(acquire(rid(), resource, "owner", 60_000L, null));
        assertTrue(owner.success());

        ExecutorService pool = Executors.newFixedThreadPool(2);
        Future<LeaseResponse> w0 = pool.submit(() -> coordinator.acquire(
                acquire("full-w0", resource, "waiter-0", 60_000L, 20_000L)));
        awaitQueueSize(resource, 1);
        Future<LeaseResponse> w1 = pool.submit(() -> coordinator.acquire(
                acquire("full-w1", resource, "waiter-1", 60_000L, 20_000L)));
        awaitQueueSize(resource, 2);

        LeaseException full = assertThrows(LeaseException.class, () -> coordinator.acquire(
                acquire("full-late", resource, "late-comer", 60_000L, 20_000L)));
        log.warn("[queue-full] requestId=full-late code={} message='{}'", full.getErrorCode(), full.getMessage());
        assertEquals(LeaseErrorCode.QUEUE_FULL, full.getErrorCode());
        assertNotEquals(LeaseErrorCode.WAIT_TIMEOUT, full.getErrorCode());
        assertNotEquals(LeaseErrorCode.TEMPORARILY_UNAVAILABLE, full.getErrorCode());

        // QUEUE_FULL is transient and must not be recorded: the same requestId may retry later.
        LeaseResponse rel0 = coordinator.release(release(rid(), resource, "owner", owner.token()));
        assertTrue(rel0.success());
        LeaseResponse g0 = w0.get(5, TimeUnit.SECONDS);
        logResponse("queue-full-w0-granted", g0);
        assertTrue(g0.success());
        assertEquals(2L, g0.token());

        LeaseResponse rel1 = coordinator.release(release(rid(), resource, "waiter-0", g0.token()));
        assertTrue(rel1.success());
        LeaseResponse g1 = w1.get(5, TimeUnit.SECONDS);
        logResponse("queue-full-w1-granted", g1);
        assertTrue(g1.success());
        assertEquals(3L, g1.token());
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));

        LeaseResponse rel2 = coordinator.release(release(rid(), resource, "waiter-1", g1.token()));
        assertTrue(rel2.success());

        LeaseResponse retry = coordinator.acquire(acquire("full-late", resource, "late-comer", 60_000L, 20_000L));
        logResponse("queue-full-retry-after-space", retry);
        assertTrue(retry.success(), "QUEUE_FULL must not poison the idempotency log; retry executes");
        assertFalse(retry.replay());
        assertEquals(4L, retry.token());
        assertEquals(0, coordinator.pendingWaiters(resource));
    }

    @Test
    @DisplayName("持有者主动释放后等待者被快速唤醒, 令牌在原有基础上递增")
    void waiterIsWokenByReleaseWithIncrementedToken() throws Exception {
        String resource = "res-wait-wake";
        LeaseResponse owner = coordinator.acquire(acquire(rid(), resource, "A", 60_000L, null));
        assertTrue(owner.success());
        assertEquals(1L, owner.token());

        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<LeaseResponse> waiting = pool.submit(() -> coordinator.acquire(
                acquire("wake-req-1", resource, "B", 5_000L, 20_000L)));
        awaitQueueSize(resource, 1);

        long start = System.currentTimeMillis();
        LeaseResponse rel = coordinator.release(release(rid(), resource, "A", owner.token()));
        assertTrue(rel.success());

        LeaseResponse granted = waiting.get(5, TimeUnit.SECONDS);
        long elapsed = System.currentTimeMillis() - start;
        pool.shutdown();

        logResponse("wake-granted", granted);
        log.info("[wake] waiter woken and granted in {}ms (release-driven wake, no busy polling)", elapsed);
        assertTrue(granted.success());
        assertEquals("B", granted.holderId());
        assertEquals(2L, granted.token(), "token must increment on top of the previous holder");
        assertTrue(elapsed < 2_000, "release should wake the waiter promptly, not after the full budget");
        assertEquals(0, coordinator.pendingWaiters(resource));
    }

    @Test
    @DisplayName("持有租约到期后等待者被唤醒接管, 无需主动释放")
    void waiterIsWokenByLeaseExpiry() throws Exception {
        String resource = "res-wait-expiry";
        LeaseResponse owner = coordinator.acquire(acquire(rid(), resource, "A", 500L, null));
        assertTrue(owner.success());

        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<LeaseResponse> waiting = pool.submit(() -> coordinator.acquire(
                acquire("expiry-req-1", resource, "B", 5_000L, 20_000L)));
        awaitQueueSize(resource, 1);

        clock.advance(600); // lease of A expires; nobody releases

        LeaseResponse granted = waiting.get(5, TimeUnit.SECONDS);
        pool.shutdown();
        logResponse("expiry-granted", granted);
        assertTrue(granted.success(), "waiter should notice clock-driven expiry and take over");
        assertEquals("B", granted.holderId());
        assertEquals(2L, granted.token());
        assertEquals(0, coordinator.pendingWaiters(resource));
    }

    @Test
    @DisplayName("等待请求的幂等: 并发重复提交共享同一次等待且结果一致; 内容变更被 REQUEST_MISMATCH 拒绝")
    void waitingAcquireIdempotencyAndMismatch() throws Exception {
        String resource = "res-wait-idem";
        LeaseResponse owner = coordinator.acquire(acquire(rid(), resource, "owner", 60_000L, null));
        assertTrue(owner.success());

        String requestId = "idem-wait-1";
        ExecutorService pool = Executors.newFixedThreadPool(2);
        // Two concurrent submissions of the SAME requestId join the same queue slot.
        Future<LeaseResponse> f1 = pool.submit(() -> coordinator.acquire(
                acquire(requestId, resource, "B", 5_000L, 20_000L)));
        Future<LeaseResponse> f2 = pool.submit(() -> coordinator.acquire(
                acquire(requestId, resource, "B", 5_000L, 20_000L)));
        awaitQueueSize(resource, 1); // deduplicated: only one queue slot for the requestId

        LeaseResponse rel = coordinator.release(release(rid(), resource, "owner", owner.token()));
        assertTrue(rel.success());

        LeaseResponse r1 = f1.get(5, TimeUnit.SECONDS);
        LeaseResponse r2 = f2.get(5, TimeUnit.SECONDS);
        pool.shutdown();
        logResponse("idem-wait-dup-1", r1);
        logResponse("idem-wait-dup-2", r2);
        assertTrue(r1.success());
        assertTrue(r2.success());
        assertEquals(r1.token(), r2.token(), "duplicate submissions must observe the identical grant");
        assertEquals(2L, r1.token());
        assertNotEquals(r1.replay(), r2.replay(), "exactly one submission computes, the other replays");

        // Sequential replay after completion: identical result, replay=true
        LeaseResponse replay = coordinator.acquire(acquire(requestId, resource, "B", 5_000L, 20_000L));
        logResponse("idem-wait-replay", replay);
        assertTrue(replay.success());
        assertTrue(replay.replay());
        assertEquals(r1.token(), replay.token());

        // Same requestId with different content must be rejected, not silently replayed.
        LeaseException mismatch = assertThrows(LeaseException.class, () -> coordinator.acquire(
                acquire(requestId, resource, "C", 5_000L, 20_000L)));
        log.warn("[idem-mismatch] requestId={} code={} message='{}'",
                requestId, mismatch.getErrorCode(), mismatch.getMessage());
        assertEquals(LeaseErrorCode.REQUEST_MISMATCH, mismatch.getErrorCode());

        // Fingerprint covers write payload as well.
        LeaseResponse write = coordinator.write(writeReq("idem-write-1", resource, "B", 2L, "v1"));
        logResponse("idem-write-first", write);
        assertTrue(write.success());
        LeaseException writeMismatch = assertThrows(LeaseException.class, () -> coordinator.write(
                writeReq("idem-write-1", resource, "B", 2L, "v2")));
        assertEquals(LeaseErrorCode.REQUEST_MISMATCH, writeMismatch.getErrorCode());
        LeaseResponse writeReplay = coordinator.write(writeReq("idem-write-1", resource, "B", 2L, "v1"));
        assertTrue(writeReplay.replay());
        assertEquals("v1", store.getResourceData(resource).orElseThrow(),
                "mismatched retry must not overwrite stored data");
    }

    @Test
    @DisplayName("不传 waitMillis 的老调用保持即时行为: 占用时立即 CONFLICT")
    void legacyAcquireWithoutWaitStaysImmediate() {
        String resource = "res-wait-legacy";
        LeaseResponse owner = coordinator.acquire(acquire(rid(), resource, "A", 60_000L, null));
        assertTrue(owner.success());

        long start = System.currentTimeMillis();
        LeaseResponse immediate = coordinator.acquire(acquire(rid(), resource, "B", 1_000L, null));
        long elapsed = System.currentTimeMillis() - start;
        logResponse("legacy-immediate-conflict", immediate);
        assertFalse(immediate.success());
        assertEquals(LeaseErrorCode.CONFLICT, immediate.errorCode());
        assertTrue(elapsed < 1_000, "legacy acquire must not wait");
        assertEquals(0, coordinator.pendingWaiters(resource));
    }

    private static WriteRequest writeReq(String requestId, String resource, String holder,
                                         Long token, String value) {
        WriteRequest r = new WriteRequest();
        r.setRequestId(requestId);
        r.setResourceId(resource);
        r.setHolderId(holder);
        r.setToken(token);
        r.setValue(value);
        return r;
    }
}
