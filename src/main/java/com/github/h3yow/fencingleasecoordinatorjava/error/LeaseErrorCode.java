package com.github.h3yow.fencingleasecoordinatorjava.error;

public enum LeaseErrorCode {
    CONFLICT,
    LEASE_EXPIRED,
    LEASE_UNKNOWN,
    FENCED,
    WAIT_TIMEOUT,
    QUEUE_FULL,
    REQUEST_MISMATCH,
    TEMPORARILY_UNAVAILABLE,
    INVALID_REQUEST;

    public int httpStatus() {
        return switch (this) {
            case CONFLICT, REQUEST_MISMATCH -> 409;
            case LEASE_EXPIRED, LEASE_UNKNOWN, FENCED -> 410;
            case WAIT_TIMEOUT -> 408;
            case QUEUE_FULL -> 429;
            case TEMPORARILY_UNAVAILABLE -> 503;
            case INVALID_REQUEST -> 400;
        };
    }
}
