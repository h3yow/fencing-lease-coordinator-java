package com.github.h3yow.fencingleasecoordinatorjava.dto;

import com.github.h3yow.fencingleasecoordinatorjava.error.LeaseErrorCode;

public record LeaseResponse(
        boolean success,
        boolean replay,
        String requestId,
        String action,
        String resourceId,
        String holderId,
        Long token,
        Long currentToken,
        String status,
        Long expiresAt,
        String value,
        Long version,
        LeaseErrorCode errorCode,
        String reason,
        String decision
) {
}
