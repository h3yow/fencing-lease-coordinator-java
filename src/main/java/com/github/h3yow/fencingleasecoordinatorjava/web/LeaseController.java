package com.github.h3yow.fencingleasecoordinatorjava.web;

import com.github.h3yow.fencingleasecoordinatorjava.service.LeaseCoordinator;
import com.github.h3yow.fencingleasecoordinatorjava.service.Outcome;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 租约协调 REST 接口。所有写接口必须携带 {@code Idempotency-Key} 请求头。
 * 业务拒绝同样返回结构化错误体，错误码与大类可区分冲突 / 失效 / 临时不可用。
 */
@RestController
@RequestMapping("/api/leases")
public class LeaseController {

    private final LeaseCoordinator coordinator;

    public LeaseController(LeaseCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    @PostMapping("/acquire")
    public ResponseEntity<Object> acquire(@RequestHeader("Idempotency-Key") String requestId,
                                          @RequestBody AcquireRequest request) {
        return toEntity(coordinator.acquire(
                requestId, request.getResourceId(), request.getHolderId(), request.getTtlMillis()));
    }

    @PostMapping("/renew")
    public ResponseEntity<Object> renew(@RequestHeader("Idempotency-Key") String requestId,
                                        @RequestBody RenewRequest request) {
        return toEntity(coordinator.renew(
                requestId, request.getResourceId(), request.getHolderId(), request.getToken(),
                request.getTtlMillis()));
    }

    @PostMapping("/release")
    public ResponseEntity<Object> release(@RequestHeader("Idempotency-Key") String requestId,
                                          @RequestBody ReleaseRequest request) {
        return toEntity(coordinator.release(
                requestId, request.getResourceId(), request.getHolderId(), request.getToken()));
    }

    @PostMapping("/write")
    public ResponseEntity<Object> write(@RequestHeader("Idempotency-Key") String requestId,
                                        @RequestBody WriteRequest request) {
        return toEntity(coordinator.fencedWrite(
                requestId, request.getResourceId(), request.getHolderId(), request.getToken(),
                request.getPayload()));
    }

    private ResponseEntity<Object> toEntity(Outcome outcome) {
        Object body = outcome.getResult() != null ? outcome.getResult() : outcome.getError();
        return ResponseEntity.status(HttpStatus.valueOf(outcome.getHttpStatus())).body(body);
    }
}
