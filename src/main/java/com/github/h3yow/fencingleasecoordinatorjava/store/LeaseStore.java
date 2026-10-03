package com.github.h3yow.fencingleasecoordinatorjava.store;

import com.github.h3yow.fencingleasecoordinatorjava.domain.LeaseRecord;

import java.util.Optional;

public interface LeaseStore {

    Optional<LeaseRecord> findLease(String resourceId);

    void putLeaseIfAbsent(String resourceId, LeaseRecord expected, LeaseRecord next);

    void replaceLease(String resourceId, LeaseRecord expected, LeaseRecord next);

    /**
     * Atomically grant a new lease if the resource is currently free (no record,
     * or the latest record is RELEASED/EXPIRED, or its HELD lease has expired by
     * {@code nowMillis}). The new token is derived inside the store lock as
     * {@code current == null ? 1 : current.token + 1}, so concurrent claimants can
     * never be granted equal or stale tokens. Returns the granted record, or
     * {@code Optional.empty()} when the resource is still validly held.
     */
    Optional<LeaseRecord> claimLeaseIfFree(String resourceId, String holderId, long acquiredAt, long expiresAt);

    Optional<String> getResourceData(String resourceId);

    long getResourceVersion(String resourceId);

    void putResourceDataIfVersion(String resourceId, String expected, String value, long version);

    java.util.Collection<String> allResourceIds();

    /** Non-blocking lookup of a previously stored idempotent outcome. */
    Optional<IdempotentRecord> findIdempotent(String requestId);

    /**
     * Atomically (under the store lock) replay an existing outcome or compute and persist a new one.
     * Suitable for operations whose computation is fast and must not run twice concurrently.
     */
    IdempotentResult replayOrCompute(String requestId, String fingerprint,
                                     java.util.function.Function<String, com.github.h3yow.fencingleasecoordinatorjava.dto.LeaseResponse> computer);

    /**
     * Persist the outcome of a long-running (e.g. waiting) operation if no record exists yet.
     * If a record already exists, it is returned as a replay instead.
     */
    IdempotentResult putIdempotentIfAbsent(String requestId, String fingerprint,
                                           com.github.h3yow.fencingleasecoordinatorjava.dto.LeaseResponse response);

    void reset();
}
