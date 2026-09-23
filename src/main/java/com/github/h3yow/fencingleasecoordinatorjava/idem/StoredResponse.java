package com.github.h3yow.fencingleasecoordinatorjava.idem;

/**
 * 幂等记录中保存的首次响应。
 */
public class StoredResponse {
    private String action;
    private String fingerprint;
    private int httpStatus;
    private String bodyJson;

    public StoredResponse() {
    }

    public StoredResponse(String action, String fingerprint, int httpStatus, String bodyJson) {
        this.action = action;
        this.fingerprint = fingerprint;
        this.httpStatus = httpStatus;
        this.bodyJson = bodyJson;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getFingerprint() {
        return fingerprint;
    }

    public void setFingerprint(String fingerprint) {
        this.fingerprint = fingerprint;
    }

    public int getHttpStatus() {
        return httpStatus;
    }

    public void setHttpStatus(int httpStatus) {
        this.httpStatus = httpStatus;
    }

    public String getBodyJson() {
        return bodyJson;
    }

    public void setBodyJson(String bodyJson) {
        this.bodyJson = bodyJson;
    }
}
