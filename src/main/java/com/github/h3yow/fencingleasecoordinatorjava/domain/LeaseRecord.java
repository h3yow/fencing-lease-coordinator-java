package com.github.h3yow.fencingleasecoordinatorjava.domain;

/**
 * 单个资源的租约记录（本地持久化）。
 */
public class LeaseRecord {
    private String resourceId;
    private String holderId;
    private long token;
    private long acquiredAt;
    private long expiresAt;
    private LeaseState state;

    public LeaseRecord() {
    }

    public String getResourceId() {
        return resourceId;
    }

    public void setResourceId(String resourceId) {
        this.resourceId = resourceId;
    }

    public String getHolderId() {
        return holderId;
    }

    public void setHolderId(String holderId) {
        this.holderId = holderId;
    }

    public long getToken() {
        return token;
    }

    public void setToken(long token) {
        this.token = token;
    }

    public long getAcquiredAt() {
        return acquiredAt;
    }

    public void setAcquiredAt(long acquiredAt) {
        this.acquiredAt = acquiredAt;
    }

    public long getExpiresAt() {
        return expiresAt;
    }

    public void setExpiresAt(long expiresAt) {
        this.expiresAt = expiresAt;
    }

    public LeaseState getState() {
        return state;
    }

    public void setState(LeaseState state) {
        this.state = state;
    }
}
