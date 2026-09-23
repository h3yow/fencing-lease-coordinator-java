package com.github.h3yow.fencingleasecoordinatorjava.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.h3yow.fencingleasecoordinatorjava.service.Outcome;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 场景三：基于请求标识的幂等与重放。
 * 同一标识重复提交返回与首次一致的结果（包括首次被拒的结果）；
 * 同一标识用于不同请求时以 IDEMPOTENT_REPLAY_MISMATCH 拒绝。
 */
public class IdempotencyTest extends AbstractLeaseTest {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyTest.class);

    @Test
    void duplicateRequestReturnsFirstResultAndReplayFlag() {
        String resource = "res-idem";

        Outcome first = coordinator.acquire("idem-key-1", resource, "holder-A", 10_000L);
        assertThat(first.getHttpStatus()).isEqualTo(200);
        assertThat(first.isReplayed()).isFalse();

        Outcome replay = coordinator.acquire("idem-key-1", resource, "holder-A", 10_000L);
        assertThat(replay.getHttpStatus()).isEqualTo(200);
        assertThat(replay.isReplayed()).isTrue();
        assertThat(replay.getResult().getToken()).isEqualTo(first.getResult().getToken());
        assertThat(replay.getResult().getHolderId()).isEqualTo("holder-A");
        log.info("[幂等重放] requestId=idem-key-1 首次token={} 重放token={} replayed={}",
                first.getResult().getToken(), replay.getResult().getToken(), replay.isReplayed());
    }

    @Test
    void rejectedFirstResultIsReplayedWithSameReason() {
        String resource = "res-idem-reject";
        coordinator.acquire("idem-key-2", resource, "holder-A", 10_000L);

        Outcome first = coordinator.acquire("idem-key-3", resource, "holder-B", 10_000L);
        assertThat(first.getHttpStatus()).isEqualTo(409);
        assertThat(first.getError().getErrorCode()).isEqualTo("RESOURCE_BUSY");

        Outcome replay = coordinator.acquire("idem-key-3", resource, "holder-B", 10_000L);
        assertThat(replay.getHttpStatus()).isEqualTo(409);
        assertThat(replay.getError().getErrorCode()).isEqualTo("RESOURCE_BUSY");
        assertThat(replay.getError().getMessage()).isEqualTo(first.getError().getMessage());
        assertThat(replay.isReplayed()).isTrue();
        log.info("[幂等重放] requestId=idem-key-3 拒绝重放 reason=RESOURCE_BUSY replayed=true 依据='{}'",
                replay.getError().getMessage());
    }

    @Test
    void sameKeyWithDifferentRequestIsRejected() {
        String resource = "res-idem-mismatch";
        coordinator.acquire("idem-key-4", resource, "holder-A", 10_000L);

        // 同一 key 换一个 holder（请求体语义不同）
        Outcome mismatch = coordinator.acquire("idem-key-4", resource, "holder-B", 10_000L);
        assertThat(mismatch.getHttpStatus()).isEqualTo(422);
        assertThat(mismatch.getError().getErrorCode()).isEqualTo("IDEMPOTENT_REPLAY_MISMATCH");
        log.info("[幂等冲突] requestId=idem-key-4 不同请求重放 reason=IDEMPOTENT_REPLAY_MISMATCH");
    }

    @Test
    void concurrentSameKeyProducesSingleFirstResult() throws Exception {
        String resource = "res-idem-concurrent";
        int n = 6;
        var pool = java.util.concurrent.Executors.newFixedThreadPool(n);
        var latch = new java.util.concurrent.CountDownLatch(1);
        java.util.List<Outcome> outs = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
        for (int i = 0; i < n; i++) {
            pool.submit(() -> {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                outs.add(coordinator.acquire("same-idem-key", resource, "holder-A", 10_000L));
            });
        }
        latch.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();

        assertThat(outs).hasSize(n);
        long fresh = outs.stream().filter(o -> !o.isReplayed() && o.getHttpStatus() == 200).count();
        long replayed = outs.stream().filter(Outcome::isReplayed).count();
        assertThat(fresh).as("同一请求标识只能有一个首次结果").isEqualTo(1);
        assertThat(replayed).isEqualTo(n - 1);
        long distinctTokens = outs.stream().map(o -> o.getResult().getToken()).distinct().count();
        assertThat(distinctTokens).as("重放结果的令牌必须与首次一致").isEqualTo(1);
        log.info("[并发幂等] requestId=same-idem-key 首次={} 重放={} 令牌集合大小={}",
                fresh, replayed, distinctTokens);
    }
}
