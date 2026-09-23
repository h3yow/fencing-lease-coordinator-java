package com.github.h3yow.fencingleasecoordinatorjava.domain;

public class LeaseRecord {

    private String resourceId;
    private String holderId;
    private long token;
    private LeaseStatus status;
    private long acquiredAt;
    private long expiresAt;
    private long releasedAt;

    public LeaseRecord() {
    }

    public String getResourceId() { return resourceId; }
    public void setResourceId(String resourceId) { this.resourceId = resourceId; }
    public String getHolderId() { return holderId; }
    public void setHolderId(String holderId) { this.holderId = holderId; }
    public long getToken() { return token; }
    public void setToken(long token) { this.token = token; }
    public LeaseStatus getStatus() { return status; }
    public void setStatus(LeaseStatus status) { this.status = status; }
    public long getAcquiredAt() { return acquiredAt; }
    public void setAcquiredAt(long acquiredAt) { this.acquiredAt = acquiredAt; }
    public long getExpiresAt() { return expiresAt; }
    public void setExpiresAt(long expiresAt) { this.expiresAt = expiresAt; }
    public long getReleasedAt() { return releasedAt; }
    public void setReleasedAt(long releasedAt) { this.releasedAt = releasedAt; }
}
