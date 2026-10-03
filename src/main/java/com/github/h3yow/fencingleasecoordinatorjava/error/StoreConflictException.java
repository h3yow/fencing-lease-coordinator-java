package com.github.h3yow.fencingleasecoordinatorjava.error;

/**
 * A StoreException subtype raised when an optimistic compare-and-swap fails
 * because the stored record no longer matches the expected identity.
 * Distinct from genuine storage I/O failures so callers may retry instead of
 * surfacing TEMPORARILY_UNAVAILABLE.
 */
public class StoreConflictException extends StoreException {

    public StoreConflictException(String message) {
        super(message);
    }

    public StoreConflictException(String message, Throwable cause) {
        super(message, cause);
    }
}
