package com.github.h3yow.fencingleasecoordinatorjava.store;

import com.github.h3yow.fencingleasecoordinatorjava.dto.LeaseResponse;

public record IdempotentResult(LeaseResponse response, boolean replay) {
}
