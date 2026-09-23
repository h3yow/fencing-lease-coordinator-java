package com.github.h3yow.fencingleasecoordinatorjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "lease")
public class LeaseProperties {
    private long defaultTtlMillis = 10_000L;
    private String dataDir = "data/leases";
    private long renewTimeoutMillis = 2_000L;

    public long getDefaultTtlMillis() {
        return defaultTtlMillis;
    }

    public void setDefaultTtlMillis(long defaultTtlMillis) {
        this.defaultTtlMillis = defaultTtlMillis;
    }

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public long getRenewTimeoutMillis() {
        return renewTimeoutMillis;
    }

    public void setRenewTimeoutMillis(long renewTimeoutMillis) {
        this.renewTimeoutMillis = renewTimeoutMillis;
    }
}
