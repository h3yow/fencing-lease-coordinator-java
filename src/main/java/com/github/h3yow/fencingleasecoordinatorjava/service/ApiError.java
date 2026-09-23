package com.github.h3yow.fencingleasecoordinatorjava.service;

/**
 * 服务层归一化错误体：错误码 + 大类（CONFLICT / INVALIDATION / UNAVAILABLE）+ 可读信息。
 */
public class ApiError {
    private String errorCode;
    private String category;
    private String message;
    private String requestId;

    public ApiError() {
    }

    public ApiError(String errorCode, String category, String message, String requestId) {
        this.errorCode = errorCode;
        this.category = category;
        this.message = message;
        this.requestId = requestId;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public String getCategory() {
        return category;
    }

    public String getMessage() {
        return message;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }
}
