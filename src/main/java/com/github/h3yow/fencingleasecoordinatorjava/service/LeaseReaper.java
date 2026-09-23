package com.github.h3yow.fencingleasecoordinatorjava.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 周期性到期回收：把已过期但尚未被动发现的 ACTIVE 租约标记为 EXPIRED，
 * 使其令牌可以被后续接管者递增后重新分配。
 */
@Component
public class LeaseReaper {

    private static final Logger log = LoggerFactory.getLogger(LeaseReaper.class);

    private final LeaseCoordinator coordinator;

    public LeaseReaper(LeaseCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    @Scheduled(fixedDelayString = "${lease.reap-interval-millis:2000}")
    public void scheduledReap() {
        int reaped = coordinator.reapExpired();
        if (reaped > 0) {
            log.info("scheduled reaper marked {} lease(s) EXPIRED", reaped);
        }
    }
}
