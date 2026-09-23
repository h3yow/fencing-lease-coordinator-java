package com.github.h3yow.fencingleasecoordinatorjava.error;

public enum LeaseErrorCode {
    CONFLICT,
    LEASE_EXPIRED,
    LEASE_UNKNOWN,
    FENCED,
    TEMPORARILY_UNAVAILABLE,
    INVALID_REQUEST;

    public int httpStatus() {
        return switch (this) {
            case CONFLICT -> 409;
            case LEASE_EXPIRED, LEASE_UNKNOWN, FENCED -> 410;
            case TEMPORARILY_UNAVAILABLE -> 503;
            case INVALID_REQUEST -> 400;
        };
    }
}
