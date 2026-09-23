package com.github.h3yow.fencingleasecoordinatorjava.dto;

public class WriteRequest {
    private String requestId;
    private String resourceId;
    private String holderId;
    private Long token;
    private String value;

    public String getRequestId() { return requestId; }
    public void setRequestId(String requestId) { this.requestId = requestId; }
    public String getResourceId() { return resourceId; }
    public void setResourceId(String resourceId) { this.resourceId = resourceId; }
    public String getHolderId() { return holderId; }
    public void setHolderId(String holderId) { this.holderId = holderId; }
    public Long getToken() { return token; }
    public void setToken(Long token) { this.token = token; }
    public String getValue() { return value; }
    public void setValue(String value) { this.value = value; }
}
