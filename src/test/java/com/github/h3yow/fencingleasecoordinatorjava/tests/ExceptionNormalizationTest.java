package com.github.h3yow.fencingleasecoordinatorjava.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.h3yow.fencingleasecoordinatorjava.service.Outcome;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 场景四：底层异常链路归一化。
 * 存储冲突 -> STORAGE_CONFLICT(409/CONFLICT)；
 * 记录缺失 -> RECORD_MISSING(410/INVALIDATION)；
 * 续约超时 -> TEMPORARILY_UNAVAILABLE(503/UNAVAILABLE)。
 * 所有错误体均为结构化字段，不直接透出底层异常类名/堆栈。
 */
public class ExceptionNormalizationTest extends AbstractLeaseTest {

    private static final Logger log = LoggerFactory.getLogger(ExceptionNormalizationTest.class);

    @Test
    void storageConflictIsNormalized() {
        String resource = "res-conflict";
        coordinator.acquire("c-1", resource, "holder-A", 10_000L);
        // 先让租约过期，制造接管 CAS；注入一次 CAS 失败
        clock.advance(20_000L);
        leaseStore.setForceNextCasFailure(true);
        Outcome o = coordinator.acquire("c-2", resource, "holder-B", 10_000L);
        assertThat(o.getHttpStatus()).isEqualTo(409);
        assertThat(o.getError().getErrorCode()).isEqualTo("STORAGE_CONFLICT");
        assertThat(o.getError().getCategory()).isEqualTo("CONFLICT");
        assertThat(o.getError().getMessage()).doesNotContain("Exception", "at com.github");
        log.info("[异常归一化] 存储冲突 requestId=c-2 httpStatus=409 reason=STORAGE_CONFLICT 依据='{}'",
                o.getError().getMessage());

        // 存储冲突不占用幂等记录：同一个请求标识也应允许重试并成功
        Outcome sameKeyRetry = coordinator.acquire("c-2", resource, "holder-B", 10_000L);
        assertThat(sameKeyRetry.getHttpStatus()).isEqualTo(200);
        assertThat(sameKeyRetry.isReplayed()).isFalse();
        assertThat(sameKeyRetry.getResult().getToken()).isEqualTo(2L);
        log.info("[异常归一化] 存储冲突不占用幂等，同 requestId=c-2 重试 token=2 成功");
    }

    @Test
    void missingRecordIsNormalized() {
        // 注入一次"记录缺失"：申请时 find 抛错
        leaseStore.setForceNextMissing(true);
        Outcome o = coordinator.acquire("m-1", "res-missing", "holder-A", 10_000L);
        assertThat(o.getHttpStatus()).isEqualTo(410);
        assertThat(o.getError().getErrorCode()).isEqualTo("RECORD_MISSING");
        assertThat(o.getError().getCategory()).isEqualTo("INVALIDATION");
        log.info("[异常归一化] 记录缺失 requestId=m-1 httpStatus=410 reason=RECORD_MISSING 依据='{}'",
                o.getError().getMessage());
    }

    @Test
    void renewalTimeoutIsNormalized() {
        String resource = "res-timeout";
        coordinator.acquire("t-1", resource, "holder-A", 10_000L);
        // 模拟续约链路超过截止时间
        leaseStore.setForceNextRenewTimeout(true);
        Outcome o = coordinator.renew("t-2", resource, "holder-A", 1L, 10_000L);
        assertThat(o.getHttpStatus()).isEqualTo(503);
        assertThat(o.getError().getErrorCode()).isEqualTo("TEMPORARILY_UNAVAILABLE");
        assertThat(o.getError().getCategory()).isEqualTo("UNAVAILABLE");
        log.info("[异常归一化] 续约超时 requestId=t-2 httpStatus=503 reason=TEMPORARILY_UNAVAILABLE 依据='{}'",
                o.getError().getMessage());

        // 超时后换请求标识重试，租约仍有效时应续约成功
        Outcome retry = coordinator.renew("t-3", resource, "holder-A", 1L, 10_000L);
        assertThat(retry.getHttpStatus()).isEqualTo(200);
        log.info("[异常归一化] 超时后重试 requestId=t-3 token=1 续约成功");
    }
}
