package com.github.h3yow.fencingleasecoordinatorjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "lease")
public class LeaseProperties {

    private long defaultTtlMillis = 10_000L;
    private long minTtlMillis = 1_000L;
    private long maxTtlMillis = 3_600_000L;
    private String storeFile = "data/lease-store.json";
    private long reapIntervalMillis = 2_000L;
    private boolean reapEnabled = true;
    private long maxWaitMillis = 30_000L;
    private int maxWaitQueueSize = 64;

    public long getDefaultTtlMillis() { return defaultTtlMillis; }
    public void setDefaultTtlMillis(long v) { this.defaultTtlMillis = v; }
    public long getMinTtlMillis() { return minTtlMillis; }
    public void setMinTtlMillis(long v) { this.minTtlMillis = v; }
    public long getMaxTtlMillis() { return maxTtlMillis; }
    public void setMaxTtlMillis(long v) { this.maxTtlMillis = v; }
    public String getStoreFile() { return storeFile; }
    public void setStoreFile(String v) { this.storeFile = v; }
    public long getReapIntervalMillis() { return reapIntervalMillis; }
    public void setReapIntervalMillis(long v) { this.reapIntervalMillis = v; }
    public boolean isReapEnabled() { return reapEnabled; }
    public void setReapEnabled(boolean v) { this.reapEnabled = v; }
    public long getMaxWaitMillis() { return maxWaitMillis; }
    public void setMaxWaitMillis(long v) { this.maxWaitMillis = v; }
    public int getMaxWaitQueueSize() { return maxWaitQueueSize; }
    public void setMaxWaitQueueSize(int v) { this.maxWaitQueueSize = v; }
}
