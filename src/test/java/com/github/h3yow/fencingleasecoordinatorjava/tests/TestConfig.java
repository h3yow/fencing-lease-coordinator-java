package com.github.h3yow.fencingleasecoordinatorjava.tests;

import com.github.h3yow.fencingleasecoordinatorjava.service.SystemMillisClock;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@TestConfiguration
public class TestConfig {

    @Bean
    @Primary
    public MutableMillisClock mutableClock() {
        return new MutableMillisClock();
    }

    @Bean
    @Primary
    public InMemoryLeaseStore inMemoryLeaseStore() {
        return new InMemoryLeaseStore();
    }
}
