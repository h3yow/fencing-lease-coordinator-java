package com.github.h3yow.fencingleasecoordinatorjava.store;

import com.github.h3yow.fencingleasecoordinatorjava.dto.LeaseResponse;

/**
 * @param response            the (first or replayed) response
 * @param replay              true when an existing record was returned instead of computing
 * @param fingerprintMismatch true when a record exists but was produced by different request content
 */
public record IdempotentResult(LeaseResponse response, boolean replay, boolean fingerprintMismatch) {
}
