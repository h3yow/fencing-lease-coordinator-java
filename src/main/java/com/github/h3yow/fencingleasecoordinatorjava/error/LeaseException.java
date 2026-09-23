package com.github.h3yow.fencingleasecoordinatorjava.error;

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
