package com.github.h3yow.fencingleasecoordinatorjava.error;

/**
 * 业务层归一化异常，不直接透出底层异常。
 */
public class LeaseException extends RuntimeException {
    private final LeaseErrorCode errorCode;

    public LeaseException(LeaseErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public LeaseException(LeaseErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public LeaseErrorCode getErrorCode() {
        return errorCode;
    }
}
