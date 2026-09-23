package com.github.h3yow.fencingleasecoordinatorjava.error;

/** 续约链路超时（临时不可用）。 */
public class LeaseRenewalTimeoutException extends RuntimeException {
    public LeaseRenewalTimeoutException(String message) {
        super(message);
    }

    public LeaseRenewalTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
