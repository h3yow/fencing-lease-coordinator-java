package com.github.h3yow.fencingleasecoordinatorjava.tests;

import com.github.h3yow.fencingleasecoordinatorjava.config.LeaseProperties;
import com.github.h3yow.fencingleasecoordinatorjava.service.SystemClock;
import java.io.IOException;
import java.nio.file.Files;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/** 测试专用：可控时钟 + 临时数据目录。 */
@TestConfiguration
public class TestConfig {

    @Bean
    @Primary
    public SystemClock controllableClock() {
        return new ControllableClock(1_000_000L);
    }

    @Bean
    @Primary
    public LeaseProperties testLeaseProperties() throws IOException {
        LeaseProperties p = new LeaseProperties();
        p.setDataDir(Files.createTempDirectory("lease-it-data-").toString());
        p.setDefaultTtlMillis(10_000L);
        p.setRenewTimeoutMillis(1_000L);
        return p;
    }
}
