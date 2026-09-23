package com.github.h3yow.fencingleasecoordinatorjava.web;

import com.github.h3yow.fencingleasecoordinatorjava.dto.AcquireRequest;
import com.github.h3yow.fencingleasecoordinatorjava.dto.LeaseResponse;
import com.github.h3yow.fencingleasecoordinatorjava.dto.ReleaseRequest;
import com.github.h3yow.fencingleasecoordinatorjava.dto.RenewRequest;
import com.github.h3yow.fencingleasecoordinatorjava.dto.WriteRequest;
import com.github.h3yow.fencingleasecoordinatorjava.service.LeaseCoordinator;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/leases")
public class LeaseController {

    private final LeaseCoordinator coordinator;

    public LeaseController(LeaseCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    @PostMapping("/acquire")
    public ResponseEntity<LeaseResponse> acquire(@RequestBody AcquireRequest request) {
        return toEntity(coordinator.acquire(request));
    }

    @PostMapping("/renew")
    public ResponseEntity<LeaseResponse> renew(@RequestBody RenewRequest request) {
        return toEntity(coordinator.renew(request));
    }

    @PostMapping("/release")
    public ResponseEntity<LeaseResponse> release(@RequestBody ReleaseRequest request) {
        return toEntity(coordinator.release(request));
    }

    @PostMapping("/write")
    public ResponseEntity<LeaseResponse> write(@RequestBody WriteRequest request) {
        return toEntity(coordinator.write(request));
    }

    @GetMapping("/resources/{resourceId}")
    public ResponseEntity<LeaseResponse> inspect(@PathVariable String resourceId) {
        return toEntity(coordinator.inspect(resourceId));
    }

    @PostMapping("/reap")
    public ResponseEntity<LeaseResponse> reap() {
        int n = coordinator.reapExpired();
        LeaseResponse body = new LeaseResponse(true, false, null, "reap", null, null, null,
                null, null, null, null, (long) n, null, null,
                "reaped " + n + " expired lease(s)");
        return ResponseEntity.ok(body);
    }

    private static ResponseEntity<LeaseResponse> toEntity(LeaseResponse response) {
        if (response.success()) {
            return ResponseEntity.ok(response);
        }
        HttpStatus status = response.errorCode() == null
                ? HttpStatus.INTERNAL_SERVER_ERROR
                : HttpStatus.valueOf(response.errorCode().httpStatus());
        return ResponseEntity.status(status).body(response);
    }
}
