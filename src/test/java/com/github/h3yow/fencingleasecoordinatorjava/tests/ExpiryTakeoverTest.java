package com.github.h3yow.fencingleasecoordinatorjava.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.h3yow.fencingleasecoordinatorjava.service.Outcome;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 场景二：租约到期后被新持有者接管，令牌单调递增；
 * 旧持有者（持有过期的旧令牌）续约、写入都必须被拒，且拒绝原因可区分。
 */
public class ExpiryTakeoverTest extends AbstractLeaseTest {

    private static final Logger log = LoggerFactory.getLogger(ExpiryTakeoverTest.class);

    @Test
    void expiredLeaseIsTakenOverAndStaleHolderIsRejected() {
        String resource = "res-expiry";

        Outcome acquired = coordinator.acquire("req-a1", resource, "holder-A", 10_000L);
        assertThat(acquired.getHttpStatus()).isEqualTo(200);
        long tokenA = acquired.getResult().getToken();
        log.info("[到期接管] requestId=req-a1 holder=A token={} 申请成功", tokenA);

        // A 的租约在到期前续约成功
        clock.advance(5_000L);
        Outcome renewed = coordinator.renew("req-r1", resource, "holder-A", tokenA, 10_000L);
        assertThat(renewed.getHttpStatus()).isEqualTo(200);
        log.info("[到期接管] requestId=req-r1 holder=A token={} 续约成功 expiresAt={}",
                tokenA, renewed.getResult().getExpiresAt());

        // 时间越过到期点
        clock.advance(11_000L);

        // 到期回收
        int reaped = coordinator.reapExpired();
        assertThat(reaped).isEqualTo(1);
        log.info("[到期接管] reapExpired 回收数量={}", reaped);

        // B 接管，令牌必须 = A 的令牌 + 1
        Outcome takenOver = coordinator.acquire("req-a2", resource, "holder-B", 10_000L);
        assertThat(takenOver.getHttpStatus()).isEqualTo(200);
        long tokenB = takenOver.getResult().getToken();
        assertThat(tokenB).as("接管后令牌必须严格递增").isEqualTo(tokenA + 1);
        log.info("[到期接管] requestId=req-a2 holder=B token={} 接管成功（旧 token={}）", tokenB, tokenA);

        // 旧持有者 A 用旧令牌续约 -> LEASE_EXPIRED（记录此刻仍由 B 持有且未过期，
        // 但 A 的续约判定先校验过期状态：记录 EXPIRED 已在接管时被覆盖为 ACTIVE，
        // 因此 A 实际命中 HOLDER_MISMATCH/TOKEN_SUPERSEDED —— 见下方分步构造）
        Outcome staleRenew = coordinator.renew("req-r2", resource, "holder-A", tokenA, 10_000L);
        assertThat(staleRenew.getHttpStatus()).isEqualTo(410);
        assertThat(staleRenew.getError().getErrorCode())
                .as("旧持有者续约必须以可区分的 LEASE_PREEMPTED 原因拒绝，不得静默成功")
                .isEqualTo("LEASE_PREEMPTED");
        log.info("[到期接管] requestId=req-r2 holder=A 旧token={} 当前token={} 续约拒绝 reason=LEASE_PREEMPTED 依据={}",
                tokenA, tokenB, staleRenew.getError().getMessage());

        // 旧持有者 A 用旧令牌写入 -> TOKEN_SUPERSEDED
        Outcome staleWrite = coordinator.fencedWrite("req-w1", resource, "holder-A", tokenA, "stale-data");
        assertThat(staleWrite.getHttpStatus()).isEqualTo(410);
        assertThat(staleWrite.getError().getErrorCode()).isEqualTo("TOKEN_SUPERSEDED");
        log.info("[到期接管] requestId=req-w1 holder=A 旧token={} 写入拒绝 reason=TOKEN_SUPERSEDED 依据={}",
                tokenA, staleWrite.getError().getMessage());

        // 新持有者 B 用新令牌写入成功
        Outcome freshWrite = coordinator.fencedWrite("req-w2", resource, "holder-B", tokenB, "fresh-data");
        assertThat(freshWrite.getHttpStatus()).isEqualTo(200);
        log.info("[到期接管] requestId=req-w2 holder=B token={} 写入成功", tokenB);

        // B 不能用相同令牌重复写入（防同令牌双写）
        Outcome dupWrite = coordinator.fencedWrite("req-w3", resource, "holder-B", tokenB, "dup-data");
        assertThat(dupWrite.getError().getErrorCode())
                .as("相同令牌不得二次写入成功")
                .isEqualTo("TOKEN_SUPERSEDED");
        log.info("[到期接管] requestId=req-w3 holder=B 同token={} 再次写入拒绝 httpStatus={} reason=TOKEN_SUPERSEDED 依据={}",
                tokenB, dupWrite.getHttpStatus(), dupWrite.getError().getMessage());
    }
}
