package com.github.h3yow.fencingleasecoordinatorjava.store;

import com.github.h3yow.fencingleasecoordinatorjava.dto.LeaseResponse;

/**
 * A persisted idempotent outcome together with the fingerprint of the request
 * content that produced it. Replays are only served when the fingerprint matches.
 */
public record IdempotentRecord(String fingerprint, LeaseResponse response) {
}
