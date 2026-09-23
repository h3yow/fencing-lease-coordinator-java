package com.github.h3yow.fencingleasecoordinatorjava.tests;

import com.github.h3yow.fencingleasecoordinatorjava.config.LeaseProperties;
import com.github.h3yow.fencingleasecoordinatorjava.dto.AcquireRequest;
import com.github.h3yow.fencingleasecoordinatorjava.dto.LeaseResponse;
import com.github.h3yow.fencingleasecoordinatorjava.dto.ReleaseRequest;
import com.github.h3yow.fencingleasecoordinatorjava.dto.RenewRequest;
import com.github.h3yow.fencingleasecoordinatorjava.dto.WriteRequest;
import com.github.h3yow.fencingleasecoordinatorjava.error.LeaseErrorCode;
import com.github.h3yow.fencingleasecoordinatorjava.service.DefaultLeaseCoordinator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LeaseCoordinatorTest {

    private static final Logger log = LoggerFactory.getLogger(LeaseCoordinatorTest.class);

    private InMemoryLeaseStore store;
    private MutableMillisClock clock;
    private DefaultLeaseCoordinator coordinator;

    @BeforeEach
    void setUp() {
        store = new InMemoryLeaseStore();
        clock = new MutableMillisClock();
        LeaseProperties props = new LeaseProperties();
        props.setDefaultTtlMillis(1000);
        props.setMinTtlMillis(10);
        props.setMaxTtlMillis(3_600_000);
        coordinator = new DefaultLeaseCoordinator(store, props, clock);
    }

    private static String rid() {
        return UUID.randomUUID().toString();
    }

    private static AcquireRequest acquire(String requestId, String resource, String holder, Long ttl) {
        AcquireRequest r = new AcquireRequest();
        r.setRequestId(requestId);
        r.setResourceId(resource);
        r.setHolderId(holder);
        r.setTtlMillis(ttl);
        return r;
    }

    private static RenewRequest renew(String requestId, String resource, String holder, Long token, Long ttl) {
        RenewRequest r = new RenewRequest();
        r.setRequestId(requestId);
        r.setResourceId(resource);
        r.setHolderId(holder);
        r.setToken(token);
        r.setTtlMillis(ttl);
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

    private static WriteRequest write(String requestId, String resource, String holder, Long token, String value) {
        WriteRequest r = new WriteRequest();
        r.setRequestId(requestId);
        r.setResourceId(resource);
        r.setHolderId(holder);
        r.setToken(token);
        r.setValue(value);
        return r;
    }

    private static void logResponse(String scenario, LeaseResponse r) {
        log.info("[{}] requestId={} action={} token={} currentToken={} success={} replay={} errorCode={} decision='{}'",
                scenario, r.requestId(), r.action(), r.token(), r.currentToken(),
                r.success(), r.replay(), r.errorCode(), r.decision());
    }

    @Test
    @DisplayName("并发申请同一资源: 仅一个成功, 令牌单调, 其余以 CONFLICT 拒绝")
    void concurrentAcquireOnlyOneWinnerAndMonotonicToken() throws Exception {
        int n = 16;
        String resource = "res-concurrent";
        ExecutorService pool = Executors.newFixedThreadPool(n);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(n);
        ConcurrentLinkedQueue<LeaseResponse> responses = new ConcurrentLinkedQueue<>();
        AtomicInteger ok = new AtomicInteger();
        AtomicInteger conflict = new AtomicInteger();

        for (int i = 0; i < n; i++) {
            final String holder = "holder-" + i;
            pool.submit(() -> {
                try {
                    start.await();
                    LeaseResponse r = coordinator.acquire(acquire(rid(), resource, holder, 5_000L));
                    responses.add(r);
                    if (r.success()) {
                        ok.incrementAndGet();
                    } else if (r.errorCode() == LeaseErrorCode.CONFLICT) {
                        conflict.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS), "all acquire attempts should finish");
        pool.shutdown();

        responses.forEach(r -> logResponse("concurrent-acquire", r));

        assertEquals(1, ok.get(), "exactly one concurrent acquire must succeed");
        assertEquals(n - 1, conflict.get(), "all losing attempts must be CONFLICT");

        LeaseResponse winner = responses.stream().filter(LeaseResponse::success).findFirst().orElseThrow();
        assertEquals(1L, winner.token(), "first token for a resource must be 1");

        LeaseResponse state = coordinator.inspect(resource);
        logResponse("concurrent-acquire-final-state", state);
        assertEquals(1L, state.token(), "stored token must match the unique grant");
        assertEquals(winner.holderId(), state.holderId());
    }

    @Test
    @DisplayName("租约到期后被新持有者接管, 令牌递增")
    void expiredLeaseCanBeTakenOverWithHigherToken() {
        String resource = "res-takeover";

        LeaseResponse first = coordinator.acquire(acquire(rid(), resource, "A", 500L));
        logResponse("takeover-A-acquire", first);
        assertTrue(first.success());
        assertEquals(1L, first.token());

        LeaseResponse aRenewBeforeExpiry = coordinator.renew(renew(rid(), resource, "A", 1L, 500L));
        logResponse("takeover-A-renew-ok", aRenewBeforeExpiry);
        assertTrue(aRenewBeforeExpiry.success());

        clock.advance(600);

        LeaseResponse aRenewAfterExpiry = coordinator.renew(renew(rid(), resource, "A", 1L, 500L));
        logResponse("takeover-A-renew-expired", aRenewAfterExpiry);
        assertFalse(aRenewAfterExpiry.success());
        assertEquals(LeaseErrorCode.LEASE_EXPIRED, aRenewAfterExpiry.errorCode());

        LeaseResponse b = coordinator.acquire(acquire(rid(), resource, "B", 500L));
        logResponse("takeover-B-acquire", b);
        assertTrue(b.success());
        assertEquals(2L, b.token(), "takeover must advance the fencing token");

        int reaped = coordinator.reapExpired();
        log.info("[takeover] reaper marked {} lease(s) expired", reaped);

        LeaseResponse state = coordinator.inspect(resource);
        logResponse("takeover-final-state", state);
        assertEquals(2L, state.token());
        assertEquals("B", state.holderId());
    }

    @Test
    @DisplayName("过期/被抢占旧持有者续约与写入被拒, 且原因可区分")
    void staleHolderRenewAndWriteAreRejectedWithDistinctReasons() {
        String resource = "res-fencing";

        LeaseResponse a = coordinator.acquire(acquire(rid(), resource, "A", 300L));
        logResponse("fencing-A-acquire", a);
        long tokenA = a.token();

        clock.advance(400);

        LeaseResponse aWriteExpired = coordinator.write(write(rid(), resource, "A", tokenA, "stale-data"));
        logResponse("fencing-A-write-expired", aWriteExpired);
        assertFalse(aWriteExpired.success());
        assertEquals(LeaseErrorCode.LEASE_EXPIRED, aWriteExpired.errorCode());
        assertNull(store.getResourceData(resource).orElse(null), "expired holder must not write");

        LeaseResponse b = coordinator.acquire(acquire(rid(), resource, "B", 1_000L));
        logResponse("fencing-B-acquire", b);
        assertEquals(tokenA + 1, b.token());

        LeaseResponse aRenewFenced = coordinator.renew(renew(rid(), resource, "A", tokenA, 500L));
        logResponse("fencing-A-renew-fenced", aRenewFenced);
        assertFalse(aRenewFenced.success());
        assertEquals(LeaseErrorCode.FENCED, aRenewFenced.errorCode(),
                "old holder renewing after takeover must be FENCED, not expired");
        assertEquals(b.token(), aRenewFenced.currentToken());

        LeaseResponse aWriteFenced = coordinator.write(write(rid(), resource, "A", tokenA, "stale-data"));
        logResponse("fencing-A-write-fenced", aWriteFenced);
        assertFalse(aWriteFenced.success());
        assertEquals(LeaseErrorCode.FENCED, aWriteFenced.errorCode());
        assertNull(store.getResourceData(resource).orElse(null), "fenced holder must not write");

        LeaseResponse bWrite = coordinator.write(write(rid(), resource, "B", b.token(), "fresh-data"));
        logResponse("fencing-B-write-ok", bWrite);
        assertTrue(bWrite.success());
        assertEquals(1L, bWrite.version());

        LeaseResponse aReleaseFenced = coordinator.release(release(rid(), resource, "A", tokenA));
        logResponse("fencing-A-release-fenced", aReleaseFenced);
        assertFalse(aReleaseFenced.success());
        assertEquals(LeaseErrorCode.FENCED, aReleaseFenced.errorCode());

        LeaseResponse unknownResource = coordinator
                .renew(renew(rid(), "res-missing", "X", 99L, 500L));
        logResponse("fencing-unknown-resource", unknownResource);
        assertFalse(unknownResource.success());
        assertEquals(LeaseErrorCode.LEASE_UNKNOWN, unknownResource.errorCode());

        LeaseResponse state = coordinator.inspect(resource);
        logResponse("fencing-final-state", state);
        assertEquals("fresh-data", store.getResourceData(resource).orElseThrow());
    }

    @Test
    @DisplayName("同一 requestId 重放: 成功与失败结果均与首次一致, replay=true")
    void idempotentReplayReturnsIdenticalResult() {
        String resource = "res-idem";
        String requestId = rid();

        LeaseResponse first = coordinator.acquire(acquire(requestId, resource, "A", 1_000L));
        logResponse("idem-first", first);
        assertTrue(first.success());
        assertFalse(first.replay());

        LeaseResponse replay = coordinator.acquire(acquire(requestId, resource, "A", 1_000L));
        logResponse("idem-replay", replay);
        assertTrue(replay.success());
        assertTrue(replay.replay());
        assertEquals(first.token(), replay.token());
        assertEquals(first.decision(), replay.decision());

        String rejectedId = rid();
        LeaseResponse rejectedFirst = coordinator.acquire(acquire(rejectedId, resource, "B", 1_000L));
        logResponse("idem-reject-first", rejectedFirst);
        assertFalse(rejectedFirst.success());
        assertEquals(LeaseErrorCode.CONFLICT, rejectedFirst.errorCode());

        LeaseResponse rejectedReplay = coordinator.acquire(acquire(rejectedId, resource, "B", 1_000L));
        logResponse("idem-reject-replay", rejectedReplay);
        assertFalse(rejectedReplay.success());
        assertTrue(rejectedReplay.replay());
        assertEquals(rejectedFirst.errorCode(), rejectedReplay.errorCode());
        assertEquals(rejectedFirst.reason(), rejectedReplay.reason());
        assertEquals(rejectedFirst.token(), rejectedReplay.token());
    }

    @Test
    @DisplayName("存储故障归一化为 TEMPORARILY_UNAVAILABLE, 不泄漏底层异常")
    void storageFaultIsNormalizedToTemporarilyUnavailable() {
        store.faultOnPersist = true;
        LeaseResponse r;
        try {
            r = coordinator.acquire(acquire(rid(), "res-fault", "A", 1_000L));
        } catch (com.github.h3yow.fencingleasecoordinatorjava.error.LeaseException ex) {
            log.warn("fault-normalized code={} message='{}'", ex.getErrorCode(), ex.getMessage());
            assertEquals(LeaseErrorCode.TEMPORARILY_UNAVAILABLE, ex.getErrorCode());
            assertNotNull(ex.getCause(), "cause should be retained for diagnostics but not exposed");
            return;
        }
        assertFalse(r.success());
        assertEquals(LeaseErrorCode.TEMPORARILY_UNAVAILABLE, r.errorCode());
    }

    @Test
    @DisplayName("主动释放后可立即重新申请, 令牌递增")
    void releaseAllowsImmediateReacquire() {
        String resource = "res-release";
        LeaseResponse a = coordinator.acquire(acquire(rid(), resource, "A", 1_000L));
        assertEquals(1L, a.token());

        LeaseResponse released = coordinator.release(release(rid(), resource, "A", a.token()));
        logResponse("release-ok", released);
        assertTrue(released.success());

        LeaseResponse aSecond = coordinator.acquire(acquire(rid(), resource, "A", 1_000L));
        logResponse("release-reacquire", aSecond);
        assertTrue(aSecond.success());
        assertEquals(2L, aSecond.token());
    }
}
