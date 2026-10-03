package com.github.h3yow.fencingleasecoordinatorjava.store;

import com.github.h3yow.fencingleasecoordinatorjava.domain.LeaseRecord;
import com.github.h3yow.fencingleasecoordinatorjava.dto.LeaseResponse;

import java.util.Optional;

public interface LeaseStore {

    Optional<LeaseRecord> findLease(String resourceId);

    void putLeaseIfAbsent(String resourceId, LeaseRecord expected, LeaseRecord next);

    void replaceLease(String resourceId, LeaseRecord expected, LeaseRecord next);

    Optional<String> getResourceData(String resourceId);

    long getResourceVersion(String resourceId);

    void putResourceDataIfVersion(String resourceId, String expected, String value, long version);

    java.util.Collection<String> allResourceIds();

    Optional<LeaseResponse> getIdempotentResponse(String requestId);

    /**
     * Returns the recorded response for {@code requestId} if present, otherwise
     * computes, records (when {@code recordable} accepts the outcome) and returns it.
     *
     * <p>The {@code fingerprint} captures the semantic content of the request
     * (action, resource, holder, token, payload...). A replayed requestId whose
     * fingerprint does not match the recorded one is rejected with a
     * {@link com.github.h3yow.fencingleasecoordinatorjava.error.LeaseException}
     * carrying {@code REQUEST_MISMATCH} instead of silently returning the old result.
     * Outcomes rejected by {@code recordable} (transient failures) are not persisted,
     * so later retries of the same requestId may still execute.
     */
    IdempotentResult replayOrCompute(String requestId, String fingerprint,
                                     java.util.function.Function<String, LeaseResponse> computer,
                                     java.util.function.Predicate<LeaseResponse> recordable);

    void reset();
}
