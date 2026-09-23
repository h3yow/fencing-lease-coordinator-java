package com.github.h3yow.fencingleasecoordinatorjava.web;

/**
 * 归一化错误响应：绝不直接透出底层异常信息。
 */
public class ErrorResponse {
    private String errorCode;
    private String category;
    private String message;
    private String requestId;

    public ErrorResponse() {
    }

    public ErrorResponse(String errorCode, String category, String message, String requestId) {
        this.errorCode = errorCode;
        this.category = category;
        this.message = message;
        this.requestId = requestId;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }
}
