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
import com.github.h3yow.fencingleasecoordinatorjava.error.StoreConflictException;
import com.github.h3yow.fencingleasecoordinatorjava.error.StoreException;
import com.github.h3yow.fencingleasecoordinatorjava.store.IdempotentResult;
import com.github.h3yow.fencingleasecoordinatorjava.store.LeaseStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class DefaultLeaseCoordinator implements LeaseCoordinator {

    private static final Logger log = LoggerFactory.getLogger(DefaultLeaseCoordinator.class);

    /** How long a waiter parks per iteration; short slices notice clock-driven expiry. */
    private static final long WAIT_SLICE_MILLIS = 50L;

    /** Sentinel used to probe the idempotency log without computing. */
    private static final RuntimeException NOT_RECORDED = new RuntimeException("not recorded yet");

    private final LeaseStore store;
    private final LeaseProperties properties;
    private final SystemMillisClock clock;
    private final LeaseWaitQueue waitQueue;

    public DefaultLeaseCoordinator(LeaseStore store, LeaseProperties properties, SystemMillisClock clock) {
        this.store = store;
        this.properties = properties;
        this.clock = clock;
        this.waitQueue = new LeaseWaitQueue(properties.getMaxWaitersPerResource(),
                properties.getMaxTotalWaiters());
    }

    @Override
    public LeaseResponse acquire(AcquireRequest request) {
        validate(request.getResourceId(), request.getHolderId(), request.getRequestId());
        long ttl = normalizeTtl(request.getTtlMillis());
        Long waitMillis = normalizeWait(request.getWaitMillis());
        String fingerprint = fingerprint("acquire", request.getResourceId(), request.getHolderId(),
                "ttl=" + ttl, "wait=" + request.getWaitMillis());
        if (waitMillis == null) {
            return runIdempotent(request.getRequestId(), "acquire", fingerprint,
                    rid -> doAcquire(rid, request.getResourceId(), request.getHolderId(), ttl));
        }
        return acquireWithWait(request, ttl, waitMillis, fingerprint);
    }

    private LeaseResponse doAcquire(String requestId, String resourceId, String holderId, long ttl) {
        long now = clock.now();
        LeaseRecord existing = store.findLease(resourceId).orElse(null);

        if (existing != null && existing.getStatus() == LeaseStatus.HELD && existing.getExpiresAt() > now) {
            return reject(requestId, "acquire", resourceId, holderId, null, existing,
                    LeaseErrorCode.CONFLICT,
                    "resource is currently held by '" + existing.getHolderId()
                            + "' with token " + existing.getToken() + " until " + existing.getExpiresAt(),
                    "lease still valid -> request conflicts with active holder");
        }

        long newToken = existing == null ? 1L : existing.getToken() + 1L;
        LeaseRecord next = new LeaseRecord();
        next.setResourceId(resourceId);
        next.setHolderId(holderId);
        next.setToken(newToken);
        next.setStatus(LeaseStatus.HELD);
        next.setAcquiredAt(now);
        next.setExpiresAt(now + ttl);

        try {
            if (existing == null) {
                store.putLeaseIfAbsent(resourceId, null, next);
            } else {
                store.replaceLease(resourceId, existing, next);
            }
        } catch (StoreException e) {
            throw unavailable("acquire", resourceId, holderId, newToken, e);
        }

        String decision = (existing != null && existing.getStatus() == LeaseStatus.HELD)
                ? "previous lease expired at " + existing.getExpiresAt() + " -> token advanced from "
                        + existing.getToken() + " to " + newToken
                : "granted new lease with monotonic token " + newToken;
        log.info("acquire requestId={} resource={} holder={} token={} decision='{}'",
                requestId, resourceId, holderId, newToken, decision);
        return ok(requestId, "acquire", resourceId, holderId, next, false, decision);
    }

    /**
     * Blocking acquire: queues behind other waiters of the same resource in FIFO
     * order and waits up to {@code waitMillis} for the lease to be released or to
     * expire. Only the head waiter may attempt a grant, so exactly one waiter wins
     * and tokens stay strictly monotonic. A wait that elapses without a grant is
     * recorded as WAIT_TIMEOUT (itself idempotently replayable); QUEUE_FULL and
     * storage failures are transient and never recorded.
     */
    private LeaseResponse acquireWithWait(AcquireRequest request, long ttl, long waitMillis, String fingerprint) {
        String requestId = request.getRequestId();
        String resourceId = request.getResourceId();
        String holderId = request.getHolderId();

        // A duplicate of an already finished request must not queue again.
        LeaseResponse alreadyRecorded = recordedResponse(requestId, fingerprint);
        if (alreadyRecorded != null) {
            return alreadyRecorded;
        }

        long deadline = clock.now() + waitMillis;

        // Fast path: nobody waiting and the lease is free -> grant without queueing.
        if (waitQueue.size(resourceId) == 0 && leaseAvailable(resourceId, clock.now())) {
            LeaseResponse granted = tryGrantOnce(requestId, fingerprint, resourceId, holderId, ttl);
            if (granted != null) {
                return granted;
            }
        }

        LeaseWaitQueue.Waiter waiter = waitQueue.enqueueOrJoin(resourceId, requestId, holderId);
        log.info("wait-acquire requestId={} resource={} holder={} waitMillis={} deadline={} queueSize={}",
                requestId, resourceId, holderId, waitMillis, deadline, waitQueue.size(resourceId));
        try {
            while (true) {
                long now = clock.now();
                if (now >= deadline) {
                    LeaseResponse timeout = waitTimeoutResponse(requestId, resourceId, holderId,
                            waitMillis, deadline);
                    return runIdempotent(requestId, "acquire", fingerprint, rid -> timeout);
                }
                if (waitQueue.isHead(waiter)) {
                    if (leaseAvailable(resourceId, now)) {
                        LeaseResponse granted = tryGrantOnce(requestId, fingerprint, resourceId,
                                holderId, ttl);
                        if (granted != null) {
                            log.info("wait-acquire granted requestId={} resource={} holder={} token={} "
                                            + "decision='reached head of FIFO queue and lease became available'",
                                    requestId, resourceId, holderId, granted.token());
                            return granted;
                        }
                    }
                } else {
                    // A duplicate thread sharing this requestId may have finished the request
                    // (grant or recorded timeout) and dequeued the shared waiter.
                    LeaseResponse recorded = recordedResponse(requestId, fingerprint);
                    if (recorded != null) {
                        log.info("wait-acquire requestId={} resource={} decision='duplicate observed "
                                + "recorded outcome of shared request'", requestId, resourceId);
                        return recorded;
                    }
                }
                waitQueue.awaitTurn(waiter, Math.min(deadline - now, WAIT_SLICE_MILLIS));
            }
        } finally {
            waitQueue.remove(waiter);
        }
    }

    /** Returns the recorded idempotent outcome for the requestId, or null if none exists yet. */
    private LeaseResponse recordedResponse(String requestId, String fingerprint) {
        try {
            return runIdempotent(requestId, "acquire", fingerprint, rid -> {
                throw NOT_RECORDED;
            });
        } catch (RuntimeException notRecorded) {
            if (notRecorded != NOT_RECORDED) {
                throw notRecorded;
            }
            return null;
        }
    }

    /**
     * Single grant attempt for a waiting request. Returns the final response when
     * the attempt produced one (grant succeeded, or a previously recorded outcome
     * was replayed), or {@code null} when the lease is still contested and the
     * caller should keep waiting. Only successful grants are recorded here, so a
     * CONFLICT never poisons the idempotency log of a still-waiting request.
     */
    private LeaseResponse tryGrantOnce(String requestId, String fingerprint,
                                       String resourceId, String holderId, long ttl) {
        IdempotentResult result;
        try {
            result = store.replayOrCompute(requestId, fingerprint,
                    rid -> doAcquire(rid, resourceId, holderId, ttl), LeaseResponse::success);
        } catch (LeaseException e) {
            if (e.getCause() instanceof StoreConflictException) {
                log.info("wait-acquire contested requestId={} resource={} decision='lost CAS race, keep waiting'",
                        requestId, resourceId);
                return null;
            }
            throw e;
        } catch (StoreException e) {
            throw new LeaseException(LeaseErrorCode.TEMPORARILY_UNAVAILABLE,
                    "storage unavailable while waiting to acquire: " + e.getMessage(), e);
        }
        LeaseResponse response = result.response();
        if (result.replay()) {
            log.info("wait-acquire replay requestId={} resource={} token={} success={} errorCode={}",
                    requestId, resourceId, response.token(), response.success(), response.errorCode());
            return withReplay(response);
        }
        return response.success() ? response : null;
    }

    private LeaseResponse waitTimeoutResponse(String requestId, String resourceId, String holderId,
                                              long waitMillis, long deadline) {
        LeaseRecord current = store.findLease(resourceId).orElse(null);
        String holderInfo = current == null ? "no record"
                : "holder='" + current.getHolderId() + "' token=" + current.getToken()
                        + " status=" + current.getStatus() + " expiresAt=" + current.getExpiresAt();
        return reject(requestId, "acquire", resourceId, holderId, null, current,
                LeaseErrorCode.WAIT_TIMEOUT,
                "waited " + waitMillis + "ms for resource '" + resourceId
                        + "' but it was still unavailable (" + holderInfo + ")",
                "wait budget " + waitMillis + "ms elapsed at " + deadline
                        + " without release/expiry -> WAIT_TIMEOUT (distinct from CONFLICT and"
                        + " TEMPORARILY_UNAVAILABLE); waiter left the queue and cannot win later");
    }

    private boolean leaseAvailable(String resourceId, long now) {
        LeaseRecord rec = store.findLease(resourceId).orElse(null);
        return rec == null || rec.getStatus() != LeaseStatus.HELD || rec.getExpiresAt() <= now;
    }

    /** Number of currently queued waiting acquire requests for a resource (diagnostics/tests). */
    public int pendingWaiters(String resourceId) {
        return waitQueue.size(resourceId);
    }

    private Long normalizeWait(Long waitMillis) {
        if (waitMillis == null) {
            return null;
        }
        if (waitMillis <= 0) {
            throw new LeaseException(LeaseErrorCode.INVALID_REQUEST, "waitMillis must be positive");
        }
        return Math.min(waitMillis, properties.getMaxWaitMillis());
    }

    @Override
    public LeaseResponse renew(RenewRequest request) {
        validate(request.getResourceId(), request.getHolderId(), request.getRequestId());
        requireToken(request.getToken());
        long ttl = normalizeTtl(request.getTtlMillis());
        String fingerprint = fingerprint("renew", request.getResourceId(), request.getHolderId(),
                "token=" + request.getToken(), "ttl=" + ttl);
        return runIdempotent(request.getRequestId(), "renew", fingerprint,
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
                "token=" + request.getToken());
        LeaseResponse response = runIdempotent(request.getRequestId(), "release", fingerprint,
                rid -> doRelease(rid, request.getResourceId(), request.getHolderId(), request.getToken()));
        if (response.success()) {
            waitQueue.signal(request.getResourceId());
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
                "token=" + request.getToken(), "value=" + request.getValue());
        return runIdempotent(request.getRequestId(), "write", fingerprint,
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
                    waitQueue.signal(resourceId);
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

    private LeaseResponse runIdempotent(String requestId, String action, String fingerprint,
                                        java.util.function.Function<String, LeaseResponse> computer) {
        return runIdempotent(requestId, action, fingerprint, computer, r -> true);
    }

    private LeaseResponse runIdempotent(String requestId, String action, String fingerprint,
                                        java.util.function.Function<String, LeaseResponse> computer,
                                        java.util.function.Predicate<LeaseResponse> recordable) {
        try {
            IdempotentResult result = store.replayOrCompute(requestId, fingerprint, computer, recordable);
            LeaseResponse response = result.response();
            if (result.replay()) {
                response = withReplay(response);
                log.info("replay requestId={} action={} token={} success={} errorCode={} decision='{}'",
                        requestId, action, response.token(), response.success(),
                        response.errorCode(), "idempotent replay of first result");
            }
            return response;
        } catch (StoreException e) {
            throw new LeaseException(LeaseErrorCode.TEMPORARILY_UNAVAILABLE,
                    "storage unavailable while processing " + action + ": " + e.getMessage(), e);
        }
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

    private static String fingerprint(String action, String resourceId, String holderId, String... parts) {
        StringBuilder sb = new StringBuilder(action).append('|').append(resourceId).append('|').append(holderId);
        for (String part : parts) {
            sb.append('|').append(part);
        }
        return sb.toString();
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
