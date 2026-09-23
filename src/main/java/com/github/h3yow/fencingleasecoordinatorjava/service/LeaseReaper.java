package com.github.h3yow.fencingleasecoordinatorjava.service;

import com.github.h3yow.fencingleasecoordinatorjava.config.LeaseProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class LeaseReaper {

    private static final Logger log = LoggerFactory.getLogger(LeaseReaper.class);

    private final LeaseCoordinator coordinator;
    private final LeaseProperties properties;

    public LeaseReaper(LeaseCoordinator coordinator, LeaseProperties properties) {
        this.coordinator = coordinator;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${lease.reap-interval-millis:2000}")
    public void tick() {
        if (!properties.isReapEnabled()) {
            return;
        }
        try {
            int n = coordinator.reapExpired();
            if (n > 0) {
                log.info("reaper tick marked {} lease(s) EXPIRED", n);
            }
        } catch (Exception e) {
            log.warn("reaper tick failed: {}", e.getMessage());
        }
    }
}
