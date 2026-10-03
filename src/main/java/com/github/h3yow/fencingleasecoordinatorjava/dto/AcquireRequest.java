package com.github.h3yow.fencingleasecoordinatorjava.dto;

public class AcquireRequest {
    private String requestId;
    private String resourceId;
    private String holderId;
    private Long ttlMillis;
    private Long waitMillis;

    public String getRequestId() { return requestId; }
    public void setRequestId(String requestId) { this.requestId = requestId; }
    public String getResourceId() { return resourceId; }
    public void setResourceId(String resourceId) { this.resourceId = resourceId; }
    public String getHolderId() { return holderId; }
    public void setHolderId(String holderId) { this.holderId = holderId; }
    public Long getTtlMillis() { return ttlMillis; }
    public void setTtlMillis(Long ttlMillis) { this.ttlMillis = ttlMillis; }
    public Long getWaitMillis() { return waitMillis; }
    public void setWaitMillis(Long waitMillis) { this.waitMillis = waitMillis; }
}
