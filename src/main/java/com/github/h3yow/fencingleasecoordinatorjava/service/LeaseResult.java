package com.github.h3yow.fencingleasecoordinatorjava.service;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * 租约操作结果（对外 DTO）。拒绝时 {@code accepted=false}，
 * {@code reason} 给出可区分的归一化原因，{@code decisionBasis} 给出判定依据。
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class LeaseResult {
    private boolean accepted;
    private String reason;
    private String decisionBasis;
    private String resourceId;
    private String holderId;
    private Long token;
    private Long currentToken;
    private Long expiresAt;
    private String requestId;
    private Boolean replayed;

    public boolean isAccepted() {
        return accepted;
    }

    public void setAccepted(boolean accepted) {
        this.accepted = accepted;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public String getDecisionBasis() {
        return decisionBasis;
    }

    public void setDecisionBasis(String decisionBasis) {
        this.decisionBasis = decisionBasis;
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

    public Long getToken() {
        return token;
    }

    public void setToken(Long token) {
        this.token = token;
    }

    public Long getCurrentToken() {
        return currentToken;
    }

    public void setCurrentToken(Long currentToken) {
        this.currentToken = currentToken;
    }

    public Long getExpiresAt() {
        return expiresAt;
    }

    public void setExpiresAt(Long expiresAt) {
        this.expiresAt = expiresAt;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public Boolean getReplayed() {
        return replayed;
    }

    public void setReplayed(Boolean replayed) {
        this.replayed = replayed;
    }
}
