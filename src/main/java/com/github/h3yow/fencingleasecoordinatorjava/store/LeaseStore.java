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

    IdempotentResult replayOrCompute(String requestId, java.util.function.Function<String, LeaseResponse> computer);

    void reset();
}
