package com.github.h3yow.fencingleasecoordinatorjava.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.h3yow.fencingleasecoordinatorjava.service.Outcome;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 场景一：多个消费者并发申请同一资源。
 * 断言：恰好一个成功；其余以 RESOURCE_BUSY 被拒；成功令牌唯一；原因可区分。
 */
public class ConcurrentAcquireTest extends AbstractLeaseTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentAcquireTest.class);

    @Test
    void onlyOneHolderWinsConcurrentAcquire() throws Exception {
        int n = 8;
        String resource = "res-concurrent";
        ExecutorService pool = Executors.newFixedThreadPool(n);
        CountDownLatch start = new CountDownLatch(1);
        List<Outcome> outcomes = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            outcomes.add(null);
        }

        for (int i = 0; i < n; i++) {
            int idx = i;
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                String holder = "holder-" + idx;
                String requestId = "req-acq-" + UUID.randomUUID();
                Outcome o = coordinator.acquire(requestId, resource, holder, 10_000L);
                outcomes.set(idx, o);
                String token = o.getResult() != null ? String.valueOf(o.getResult().getToken()) : "-";
                String code = o.getError() != null ? o.getError().getErrorCode() : "ACCEPTED";
                log.info("[并发申请] requestId={} resource={} holder={} token={} 判定={} 依据={}",
                        requestId, resource, holder, token, code,
                        o.getError() != null ? o.getError().getMessage() : "唯一赢家");
            });
        }
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        long accepted = outcomes.stream().filter(o -> o.getHttpStatus() == 200).count();
        long busy = outcomes.stream()
                .filter(o -> o.getError() != null && "RESOURCE_BUSY".equals(o.getError().getErrorCode()))
                .count();

        assertThat(accepted).as("恰好一个持有者申请成功").isEqualTo(1);
        assertThat(busy).as("其余申请必须以 RESOURCE_BUSY 被拒").isEqualTo(n - 1);

        long winnerToken = outcomes.stream().filter(o -> o.getHttpStatus() == 200)
                .mapToLong(o -> o.getResult().getToken()).findFirst().orElseThrow();
        assertThat(winnerToken).as("首次令牌为 1").isEqualTo(1L);

        log.info("[并发申请结果] resource={} 成功数={} 冲突拒绝数={} 唯一令牌={}",
                resource, accepted, busy, winnerToken);
    }
}
