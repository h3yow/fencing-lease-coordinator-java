package com.github.h3yow.fencingleasecoordinatorjava.service;

import com.github.h3yow.fencingleasecoordinatorjava.config.LeaseProperties;
import com.github.h3yow.fencingleasecoordinatorjava.domain.LeaseRecord;
import com.github.h3yow.fencingleasecoordinatorjava.domain.LeaseStatus;
import com.github.h3yow.fencingleasecoordinatorjava.dto.AcquireRequest;
import com.github.h3yow.fencingleasecoordinatorjava.dto.LeaseResponse;
import com.github.h3yow.fencingleasecoordinatorjava.dto.ReleaseRequest;
import com.github.h3yow.fencingleasecoordinatorjava.dto.RenewRequest;
import com.github.h3yow.fencingleasecoordinatorjava.dto.WriteRequest;
import com.github.h3yow.fencingleasecoordinatorjava.error.LeaseErrorCode;
import com.github.h3yow.fencingleasecoordinatorjava.error.LeaseException;
import com.github.h3yow.fencingleasecoordinatorjava.error.StoreException;
import com.github.h3yow.fencingleasecoordinatorjava.store.IdempotentResult;
import com.github.h3yow.fencingleasecoordinatorjava.store.LeaseStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class DefaultLeaseCoordinator implements LeaseCoordinator {

    private static final Logger log = LoggerFactory.getLogger(DefaultLeaseCoordinator.class);

    private final LeaseStore store;
    private final LeaseProperties properties;
    private final SystemMillisClock clock;
    private final java.util.Map<String, LeaseWaitQueue> waitQueues = new java.util.concurrent.ConcurrentHashMap<>();

    public DefaultLeaseCoordinator(LeaseStore store, LeaseProperties properties, SystemMillisClock clock) {
        this.store = store;
        this.properties = properties;
        this.clock = clock;
    }

    @Override
    public LeaseResponse acquire(AcquireRequest request) {
        validate(request.getResourceId(), request.getHolderId(), request.getRequestId());
        long ttl = normalizeTtl(request.getTtlMillis());
        Long waitMillis = normalizeWait(request.getWaitMillis());
        String fingerprint = fingerprint("acquire", request.getResourceId(), request.getHolderId(),
                ttl, request.getWaitMillis());
        if (waitMillis == null) {
            return runIdempotent(request.getRequestId(), fingerprint, "acquire",
                    rid -> doAcquireGuarded(rid, request.getResourceId(), request.getHolderId(), ttl));
        }
        return acquireWithWait(request.getRequestId(), fingerprint, request.getResourceId(),
                request.getHolderId(), ttl, waitMillis);
    }

    /**
     * Immediate (non-waiting) acquire. When a wait queue is active for the resource,
     * queued waiters have priority and the immediate acquire is rejected with CONFLICT.
     */
    private LeaseResponse doAcquireGuarded(String requestId, String resourceId, String holderId, long ttl) {
        LeaseWaitQueue queue = waitQueues.get(resourceId);
        if (queue != null && queue.hasActiveWaiters()) {
            return reject(requestId, "acquire", resourceId, holderId, null,
                    store.findLease(resourceId).orElse(null),
                    LeaseErrorCode.CONFLICT,
                    "resource has queued waiters; immediate acquire yields to them",
                    "wait queue active -> queued waiters have priority over immediate acquire");
        }
        return doAcquire(requestId, resourceId, holderId, ttl);
    }

    private LeaseResponse doAcquire(String requestId, String resourceId, String holderId, long ttl) {
        long now = clock.now();
        LeaseRecord before = store.findLease(resourceId).orElse(null);
        LeaseRecord granted;
        try {
            granted = store.claimLeaseIfFree(resourceId, holderId, now, now + ttl).orElse(null);
        } catch (StoreException e) {
            throw unavailable("acquire", resourceId, holderId,
                    before == null ? null : before.getToken() + 1L, e);
        }
        if (granted == null) {
            LeaseRecord current = store.findLease(resourceId).orElse(null);
            return reject(requestId, "acquire", resourceId, holderId, null, current,
                    LeaseErrorCode.CONFLICT,
                    current == null
                            ? "resource is currently held"
                            : "resource is currently held by '" + current.getHolderId()
                                    + "' with token " + current.getToken() + " until " + current.getExpiresAt(),
                    "lease still valid -> request conflicts with active holder");
        }

        String decision = (before != null && before.getStatus() == LeaseStatus.HELD)
                ? "previous lease expired at " + before.getExpiresAt() + " -> token advanced from "
                        + before.getToken() + " to " + granted.getToken()
                : "granted new lease with monotonic token " + granted.getToken();
        log.info("acquire requestId={} resource={} holder={} token={} decision='{}'",
                requestId, resourceId, holderId, granted.getToken(), decision);
        return ok(requestId, "acquire", resourceId, holderId, granted, false, decision);
    }

    private LeaseResponse acquireWithWait(String requestId, String fingerprint, String resourceId,
                                          String holderId, long ttl, long waitMillis) {
        // 1. non-blocking idempotent lookup; never wait twice for the same requestId
        try {
            java.util.Optional<com.github.h3yow.fencingleasecoordinatorjava.store.IdempotentRecord> prior =
                    store.findIdempotent(requestId);
            if (prior.isPresent()) {
                return settleIdempotent(
                        new IdempotentResult(prior.get().response(), true,
                                !java.util.Objects.equals(prior.get().fingerprint(), fingerprint)),
                        requestId, fingerprint, "acquire");
            }
        } catch (StoreException e) {
            throw new LeaseException(LeaseErrorCode.TEMPORARILY_UNAVAILABLE,
                    "storage unavailable while checking idempotency: " + e.getMessage(), e);
        }

        // 2. wait in the FIFO queue (outside the store lock) for a grant or the deadline
        LeaseWaitQueue queue = queueFor(resourceId);
        LeaseResponse outcome = queue.awaitTurn(
                new LeaseWaitQueue.Waiter(requestId, holderId), waitMillis, clock::now,
                w -> attemptQueuedAcquire(requestId, resourceId, holderId, ttl),
                () -> queueFull(requestId, resourceId, holderId, queue),
                () -> waitTimeout(requestId, resourceId, holderId, waitMillis));

        // 3. persist the outcome; a concurrent duplicate may have stored it first
        try {
            IdempotentResult stored = store.putIdempotentIfAbsent(requestId, fingerprint, outcome);
            return settleIdempotent(stored, requestId, fingerprint, "acquire");
        } catch (StoreException e) {
            throw new LeaseException(LeaseErrorCode.TEMPORARILY_UNAVAILABLE,
                    "storage unavailable while recording wait outcome: " + e.getMessage(), e);
        }
    }

    private LeaseWaitQueue.Attempt attemptQueuedAcquire(String requestId, String resourceId,
                                                        String holderId, long ttl) {
        long now = clock.now();
        LeaseRecord granted;
        try {
            granted = store.claimLeaseIfFree(resourceId, holderId, now, now + ttl).orElse(null);
        } catch (StoreException e) {
            throw unavailable("acquire(wait)", resourceId, holderId, null, e);
        }
        if (granted == null) {
            LeaseRecord current = store.findLease(resourceId).orElse(null);
            long recheckAt = current == null ? now + 50L : current.getExpiresAt();
            return LeaseWaitQueue.Attempt.stillHeld(recheckAt);
        }
        String decision = "granted after waiting; token advanced to " + granted.getToken();
        log.info("acquire(wait) requestId={} resource={} holder={} token={} decision='{}'",
                requestId, resourceId, holderId, granted.getToken(), decision);
        return LeaseWaitQueue.Attempt.granted(
                ok(requestId, "acquire", resourceId, holderId, granted, false, decision));
    }

    private LeaseResponse waitTimeout(String requestId, String resourceId, String holderId, long waitMillis) {
        LeaseRecord current = store.findLease(resourceId).orElse(null);
        return reject(requestId, "acquire", resourceId, holderId, null, current,
                LeaseErrorCode.WAIT_TIMEOUT,
                "waited " + waitMillis + "ms but the lease is still held"
                        + (current == null ? "" : " by '" + current.getHolderId()
                                + "' with token " + current.getToken() + " until " + current.getExpiresAt()),
                "wait budget " + waitMillis + "ms exhausted -> WAIT_TIMEOUT");
    }

    private LeaseResponse queueFull(String requestId, String resourceId, String holderId,
                                    LeaseWaitQueue queue) {
        return reject(requestId, "acquire", resourceId, holderId, null,
                store.findLease(resourceId).orElse(null),
                LeaseErrorCode.QUEUE_FULL,
                "wait queue for resource '" + resourceId + "' is full (" + queue.size()
                        + "/" + properties.getMaxWaitQueueSize() + ")",
                "queue size " + queue.size() + " reached capacity "
                        + properties.getMaxWaitQueueSize() + " -> QUEUE_FULL");
    }

    private LeaseWaitQueue queueFor(String resourceId) {
        return waitQueues.computeIfAbsent(resourceId,
                k -> new LeaseWaitQueue(properties.getMaxWaitQueueSize()));
    }

    private void signalWaiters(String resourceId) {
        LeaseWaitQueue queue = waitQueues.get(resourceId);
        if (queue != null) {
            queue.signalAll();
        }
    }

    @Override
    public LeaseResponse renew(RenewRequest request) {
        validate(request.getResourceId(), request.getHolderId(), request.getRequestId());
        requireToken(request.getToken());
        long ttl = normalizeTtl(request.getTtlMillis());
        String fingerprint = fingerprint("renew", request.getResourceId(), request.getHolderId(),
                request.getToken(), ttl);
        return runIdempotent(request.getRequestId(), fingerprint, "renew",
                rid -> doRenew(rid, request.getResourceId(), request.getHolderId(), request.getToken(), ttl));
    }

    private LeaseResponse doRenew(String requestId, String resourceId, String holderId, long token, long ttl) {
        long now = clock.now();
        LeaseRecord existing = store.findLease(resourceId).orElse(null);

        if (existing == null) {
            return reject(requestId, "renew", resourceId, holderId, token, null,
                    LeaseErrorCode.LEASE_UNKNOWN,
                    "no lease exists for resource '" + resourceId + "'",
                    "record missing -> unknown lease");
        }
        if (existing.getToken() > token) {
            return reject(requestId, "renew", resourceId, holderId, token, existing,
                    LeaseErrorCode.FENCED,
                    "token " + token + " is stale; current holder '" + existing.getHolderId()
                            + "' owns token " + existing.getToken(),
                    "current token " + existing.getToken() + " > presented " + token + " -> fenced");
        }
        if (existing.getToken() < token || !holderId.equals(existing.getHolderId())) {
            return reject(requestId, "renew", resourceId, holderId, token, existing,
                    LeaseErrorCode.LEASE_UNKNOWN,
                    "holder/token pair not recognised (record token " + existing.getToken()
                            + ", record holder '" + existing.getHolderId() + "')",
                    "presented holder/token does not match stored record -> unknown lease");
        }
        if (existing.getStatus() != LeaseStatus.HELD || existing.getExpiresAt() <= now) {
            return reject(requestId, "renew", resourceId, holderId, token, existing,
                    LeaseErrorCode.LEASE_EXPIRED,
                    "lease with token " + token + " expired at " + existing.getExpiresAt()
                            + " (status=" + existing.getStatus() + "); it must be re-acquired",
                    "now=" + now + " exceeds expiry=" + existing.getExpiresAt() + " -> expired, no renewal");
        }

        LeaseRecord next = copy(existing);
        next.setExpiresAt(now + ttl);
        try {
            store.replaceLease(resourceId, existing, next);
        } catch (StoreException e) {
            throw unavailable("renew", resourceId, holderId, token, e);
        }

        String decision = "token " + token + " still current and held -> expiry extended to " + next.getExpiresAt();
        log.info("renew requestId={} resource={} holder={} token={} decision='{}'",
                requestId, resourceId, holderId, token, decision);
        return ok(requestId, "renew", resourceId, holderId, next, false, decision);
    }

    @Override
    public LeaseResponse release(ReleaseRequest request) {
        validate(request.getResourceId(), request.getHolderId(), request.getRequestId());
        requireToken(request.getToken());
        String fingerprint = fingerprint("release", request.getResourceId(), request.getHolderId(),
                request.getToken());
        LeaseResponse response = runIdempotent(request.getRequestId(), fingerprint, "release",
                rid -> doRelease(rid, request.getResourceId(), request.getHolderId(), request.getToken()));
        if (response.success() && !response.replay()) {
            signalWaiters(request.getResourceId());
        }
        return response;
    }

    private LeaseResponse doRelease(String requestId, String resourceId, String holderId, long token) {
        LeaseRecord existing = store.findLease(resourceId).orElse(null);

        if (existing == null) {
            return reject(requestId, "release", resourceId, holderId, token, null,
                    LeaseErrorCode.LEASE_UNKNOWN,
                    "no lease exists for resource '" + resourceId + "'",
                    "record missing -> unknown lease");
        }
        if (existing.getToken() > token) {
            return reject(requestId, "release", resourceId, holderId, token, existing,
                    LeaseErrorCode.FENCED,
                    "token " + token + " is stale; current token is " + existing.getToken(),
                    "current token " + existing.getToken() + " > presented " + token + " -> fenced release rejected");
        }
        if (existing.getToken() < token || !holderId.equals(existing.getHolderId())) {
            return reject(requestId, "release", resourceId, holderId, token, existing,
                    LeaseErrorCode.LEASE_UNKNOWN,
                    "holder/token pair not recognised (record token " + existing.getToken() + ")",
                    "presented holder/token does not match stored record -> unknown lease");
        }
        if (existing.getStatus() != LeaseStatus.HELD) {
            return reject(requestId, "release", resourceId, holderId, token, existing,
                    LeaseErrorCode.LEASE_EXPIRED,
                    "lease with token " + token + " is already " + existing.getStatus(),
                    "stored status=" + existing.getStatus() + " -> expired/released before");
        }

        LeaseRecord next = copy(existing);
        next.setStatus(LeaseStatus.RELEASED);
        next.setReleasedAt(clock.now());
        try {
            store.replaceLease(resourceId, existing, next);
        } catch (StoreException e) {
            throw unavailable("release", resourceId, holderId, token, e);
        }

        String decision = "token " + token + " matches current holder -> released";
        log.info("release requestId={} resource={} holder={} token={} decision='{}'",
                requestId, resourceId, holderId, token, decision);
        return ok(requestId, "release", resourceId, holderId, next, false, decision);
    }

    @Override
    public LeaseResponse write(WriteRequest request) {
        validate(request.getResourceId(), request.getHolderId(), request.getRequestId());
        requireToken(request.getToken());
        if (request.getValue() == null) {
            throw new LeaseException(LeaseErrorCode.INVALID_REQUEST, "value must not be null");
        }
        String fingerprint = fingerprint("write", request.getResourceId(), request.getHolderId(),
                request.getToken(), request.getValue());
        return runIdempotent(request.getRequestId(), fingerprint, "write",
                rid -> doWrite(rid, request.getResourceId(), request.getHolderId(),
                        request.getToken(), request.getValue()));
    }

    private LeaseResponse doWrite(String requestId, String resourceId, String holderId, long token, String value) {
        long now = clock.now();
        LeaseRecord existing = store.findLease(resourceId).orElse(null);

        if (existing == null) {
            return reject(requestId, "write", resourceId, holderId, token, null,
                    LeaseErrorCode.LEASE_UNKNOWN,
                    "no lease exists for resource '" + resourceId + "'",
                    "record missing -> unknown lease");
        }
        if (existing.getToken() > token) {
            return reject(requestId, "write", resourceId, holderId, token, existing,
                    LeaseErrorCode.FENCED,
                    "write with stale token " + token + " rejected; current token is " + existing.getToken()
                            + " held by '" + existing.getHolderId() + "'",
                    "fencing check: current " + existing.getToken() + " > presented " + token
                            + " -> stale holder cannot write");
        }
        if (existing.getToken() < token || !holderId.equals(existing.getHolderId())) {
            return reject(requestId, "write", resourceId, holderId, token, existing,
                    LeaseErrorCode.LEASE_UNKNOWN,
                    "holder/token pair not recognised (record token " + existing.getToken() + ")",
                    "presented holder/token does not match stored record -> unknown lease");
        }
        if (existing.getStatus() != LeaseStatus.HELD || existing.getExpiresAt() <= now) {
            return reject(requestId, "write", resourceId, holderId, token, existing,
                    LeaseErrorCode.LEASE_EXPIRED,
                    "lease with token " + token + " expired at " + existing.getExpiresAt(),
                    "fencing check: lease expired at " + existing.getExpiresAt() + " -> write rejected");
        }

        long version = store.getResourceVersion(resourceId);
        try {
            store.putResourceDataIfVersion(resourceId, null, value, version);
        } catch (StoreException e) {
            throw unavailable("write", resourceId, holderId, token, e);
        }

        String decision = "fencing token " + token + " is current and lease held -> write accepted at version "
                + (version + 1);
        log.info("write requestId={} resource={} holder={} token={} version={} decision='{}'",
                requestId, resourceId, holderId, token, version + 1, decision);
        return new LeaseResponse(true, false, requestId, "write", resourceId, holderId, token,
                existing.getToken(), LeaseStatus.HELD.name(), existing.getExpiresAt(),
                value, version + 1, null, null, decision);
    }

    @Override
    public LeaseResponse inspect(String resourceId) {
        LeaseRecord rec = store.findLease(resourceId).orElse(null);
        String dataValue = store.getResourceData(resourceId).orElse(null);
        if (rec == null) {
            return new LeaseResponse(false, false, null, "inspect", resourceId, null, null, null,
                    null, null, dataValue, null, LeaseErrorCode.LEASE_UNKNOWN,
                    "no lease exists for resource '" + resourceId + "'", "record missing");
        }
        long now = clock.now();
        String effectiveStatus = rec.getStatus() == LeaseStatus.HELD && rec.getExpiresAt() <= now
                ? LeaseStatus.HELD.name() + "(expired-pending-reap)" : rec.getStatus().name();
        return new LeaseResponse(true, false, null, "inspect", resourceId, rec.getHolderId(),
                rec.getToken(), rec.getToken(), effectiveStatus, rec.getExpiresAt(),
                dataValue, null, null, null, "latest persisted record");
    }

    @Override
    public int waitQueueSize(String resourceId) {
        LeaseWaitQueue queue = waitQueues.get(resourceId);
        return queue == null ? 0 : queue.size();
    }

    @Override
    public int reapExpired() {
        long now = clock.now();
        int[] reaped = {0};
        for (String resourceId : allResourceIds()) {
            LeaseRecord rec = store.findLease(resourceId).orElse(null);
            if (rec != null && rec.getStatus() == LeaseStatus.HELD && rec.getExpiresAt() <= now) {
                LeaseRecord next = copy(rec);
                next.setStatus(LeaseStatus.EXPIRED);
                try {
                    store.replaceLease(resourceId, rec, next);
                    reaped[0]++;
                    log.info("reaper resource={} token={} expiredAt={} marked EXPIRED",
                            resourceId, rec.getToken(), rec.getExpiresAt());
                    signalWaiters(resourceId);
                } catch (StoreException e) {
                    log.warn("reaper skipped resource={} due to storage conflict: {}", resourceId, e.getMessage());
                }
            }
        }
        return reaped[0];
    }

    private java.util.List<String> allResourceIds() {
        return new java.util.ArrayList<>(store.allResourceIds());
    }

    private LeaseResponse runIdempotent(String requestId, String fingerprint, String action,
                                        java.util.function.Function<String, LeaseResponse> computer) {
        try {
            IdempotentResult result = store.replayOrCompute(requestId, fingerprint, computer);
            return settleIdempotent(result, requestId, fingerprint, action);
        } catch (StoreException e) {
            throw new LeaseException(LeaseErrorCode.TEMPORARILY_UNAVAILABLE,
                    "storage unavailable while processing " + action + ": " + e.getMessage(), e);
        }
    }

    private LeaseResponse settleIdempotent(IdempotentResult result, String requestId,
                                           String fingerprint, String action) {
        if (result.fingerprintMismatch()) {
            log.warn("idempotency conflict requestId={} action={} fingerprint={} -> rejected as REQUEST_CONFLICT",
                    requestId, action, fingerprint);
            throw new LeaseException(LeaseErrorCode.REQUEST_CONFLICT,
                    "requestId '" + requestId + "' was already used with different request content; "
                            + "the previous result is not returned");
        }
        LeaseResponse response = result.response();
        if (result.replay()) {
            response = withReplay(response);
            log.info("replay requestId={} action={} token={} success={} errorCode={} decision='{}'",
                    requestId, action, response.token(), response.success(),
                    response.errorCode(), "idempotent replay of first result");
        }
        return response;
    }

    private static String fingerprint(String action, Object... parts) {
        StringBuilder sb = new StringBuilder(action);
        for (Object p : parts) {
            sb.append('|').append(p == null ? "-" : p);
        }
        return sb.toString();
    }

    private static LeaseResponse withReplay(LeaseResponse r) {
        return new LeaseResponse(r.success(), true, r.requestId(), r.action(), r.resourceId(),
                r.holderId(), r.token(), r.currentToken(), r.status(), r.expiresAt(),
                r.value(), r.version(), r.errorCode(), r.reason(), r.decision());
    }

    private LeaseResponse reject(String requestId, String action, String resourceId, String holderId,
                                 Long presentedToken, LeaseRecord current, LeaseErrorCode code,
                                 String reason, String decision) {
        log.warn("reject requestId={} action={} resource={} holder={} presentedToken={} currentToken={} code={} reason='{}' decision='{}'",
                requestId, action, resourceId, holderId, presentedToken,
                current == null ? null : current.getToken(), code, reason, decision);
        return new LeaseResponse(false, false, requestId, action, resourceId, holderId,
                presentedToken, current == null ? null : current.getToken(),
                current == null ? null : current.getStatus().name(),
                current == null ? null : current.getExpiresAt(),
                null, null, code, reason, decision);
    }

    private LeaseResponse ok(String requestId, String action, String resourceId, String holderId,
                             LeaseRecord rec, boolean replay, String decision) {
        return new LeaseResponse(true, replay, requestId, action, resourceId, holderId,
                rec.getToken(), rec.getToken(), rec.getStatus().name(), rec.getExpiresAt(),
                null, null, null, null, decision);
    }

    private LeaseException unavailable(String action, String resourceId, String holderId,
                                       Long token, StoreException cause) {
        log.warn("storage failure action={} resource={} holder={} token={}: {}",
                action, resourceId, holderId, token, cause.getMessage());
        return new LeaseException(LeaseErrorCode.TEMPORARILY_UNAVAILABLE,
                "temporarily unavailable: storage " + action + " failed, please retry", cause);
    }

    private static LeaseRecord copy(LeaseRecord r) {
        LeaseRecord c = new LeaseRecord();
        c.setResourceId(r.getResourceId());
        c.setHolderId(r.getHolderId());
        c.setToken(r.getToken());
        c.setStatus(r.getStatus());
        c.setAcquiredAt(r.getAcquiredAt());
        c.setExpiresAt(r.getExpiresAt());
        c.setReleasedAt(r.getReleasedAt());
        return c;
    }

    private long normalizeTtl(Long requested) {
        long ttl = requested == null ? properties.getDefaultTtlMillis() : requested;
        if (ttl <= 0) {
            throw new LeaseException(LeaseErrorCode.INVALID_REQUEST, "ttlMillis must be positive");
        }
        ttl = Math.max(properties.getMinTtlMillis(), Math.min(properties.getMaxTtlMillis(), ttl));
        return ttl;
    }

    private Long normalizeWait(Long requested) {
        if (requested == null) {
            return null;
        }
        if (requested <= 0) {
            throw new LeaseException(LeaseErrorCode.INVALID_REQUEST, "waitMillis must be positive");
        }
        return Math.min(requested, properties.getMaxWaitMillis());
    }

    private static void validate(String resourceId, String holderId, String requestId) {
        if (resourceId == null || resourceId.isBlank()) {
            throw new LeaseException(LeaseErrorCode.INVALID_REQUEST, "resourceId is required");
        }
        if (holderId == null || holderId.isBlank()) {
            throw new LeaseException(LeaseErrorCode.INVALID_REQUEST, "holderId is required");
        }
        if (requestId == null || requestId.isBlank()) {
            throw new LeaseException(LeaseErrorCode.INVALID_REQUEST, "requestId is required");
        }
    }

    private static void requireToken(Long token) {
        if (token == null) {
            throw new LeaseException(LeaseErrorCode.INVALID_REQUEST, "token is required");
        }
    }
}
