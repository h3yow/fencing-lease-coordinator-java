package com.github.h3yow.fencingleasecoordinatorjava.dto;

public class RenewRequest {
    private String requestId;
    private String resourceId;
    private String holderId;
    private Long token;
    private Long ttlMillis;

    public String getRequestId() { return requestId; }
    public void setRequestId(String requestId) { this.requestId = requestId; }
    public String getResourceId() { return resourceId; }
    public void setResourceId(String resourceId) { this.resourceId = resourceId; }
    public String getHolderId() { return holderId; }
    public void setHolderId(String holderId) { this.holderId = holderId; }
    public Long getToken() { return token; }
    public void setToken(Long token) { this.token = token; }
    public Long getTtlMillis() { return ttlMillis; }
    public void setTtlMillis(Long ttlMillis) { this.ttlMillis = ttlMillis; }
}
