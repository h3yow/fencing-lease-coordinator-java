package com.github.h3yow.fencingleasecoordinatorjava.error;

/** 底层存储并发冲突（CAS 失败）。 */
public class StorageConflictException extends RuntimeException {
    public StorageConflictException(String message) {
        super(message);
    }
}
