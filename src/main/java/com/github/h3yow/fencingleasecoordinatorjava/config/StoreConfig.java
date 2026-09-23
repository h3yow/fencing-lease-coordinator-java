package com.github.h3yow.fencingleasecoordinatorjava.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.fencingleasecoordinatorjava.idem.FileIdempotencyStore;
import com.github.h3yow.fencingleasecoordinatorjava.service.SystemClock;
import com.github.h3yow.fencingleasecoordinatorjava.storage.FileLeaseStore;
import com.github.h3yow.fencingleasecoordinatorjava.storage.WriteLedger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 本地持久化组件装配：租约记录、幂等记录、写入台账共享同一个数据目录。
 */
@Configuration
public class StoreConfig {

    @Bean
    public FileLeaseStore fileLeaseStore(LeaseProperties properties, ObjectMapper mapper,
                                         SystemClock clock) {
        return new FileLeaseStore(properties, mapper, clock);
    }

    @Bean
    public FileIdempotencyStore fileIdempotencyStore(LeaseProperties properties, ObjectMapper mapper) {
        return new FileIdempotencyStore(properties, mapper);
    }

    @Bean
    public WriteLedger writeLedger(LeaseProperties properties) {
        return new WriteLedger(properties);
    }
}
