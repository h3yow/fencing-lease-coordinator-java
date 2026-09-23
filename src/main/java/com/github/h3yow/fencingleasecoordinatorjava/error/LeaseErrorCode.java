package com.github.h3yow.fencingleasecoordinatorjava.error;

/**
 * 对外可区分的归一化错误类型。
 */
public enum LeaseErrorCode {
    UNKNOWN_TOKEN,
    LEASE_EXPIRED,
    LEASE_PREEMPTED,
    RESOURCE_BUSY,
    ALREADY_HELD,
    TOKEN_SUPERSEDED,
    HOLDER_MISMATCH,
    STORAGE_CONFLICT,
    RECORD_MISSING,
    TEMPORARILY_UNAVAILABLE,
    IDEMPOTENT_REPLAY_MISMATCH
}
