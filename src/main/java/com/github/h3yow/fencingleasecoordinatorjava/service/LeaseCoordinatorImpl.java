package com.github.h3yow.fencingleasecoordinatorjava.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.fencingleasecoordinatorjava.domain.ActionType;
import com.github.h3yow.fencingleasecoordinatorjava.domain.LeaseRecord;
import com.github.h3yow.fencingleasecoordinatorjava.domain.LeaseState;
import com.github.h3yow.fencingleasecoordinatorjava.error.LeaseErrorCode;
import com.github.h3yow.fencingleasecoordinatorjava.error.LeaseRenewalTimeoutException;
import com.github.h3yow.fencingleasecoordinatorjava.error.RecordMissingException;
import com.github.h3yow.fencingleasecoordinatorjava.error.StorageConflictException;
import com.github.h3yow.fencingleasecoordinatorjava.idem.FileIdempotencyStore;
import com.github.h3yow.fencingleasecoordinatorjava.idem.StoredResponse;
import com.github.h3yow.fencingleasecoordinatorjava.config.LeaseProperties;
import com.github.h3yow.fencingleasecoordinatorjava.storage.FileLeaseStore;
import com.github.h3yow.fencingleasecoordinatorjava.storage.WriteLedger;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 租约协调核心实现。
 *
 * <p>关键不变量：
 * <ul>
 *   <li>同一资源的令牌单调递增，每次接管/重新申请都在持久化旧令牌上加 1；</li>
 *   <li>所有状态判定与写入都在"资源锁 + 存储 CAS"双重保护下完成；</li>
 *   <li>旧持有者即使持有历史令牌，续约/写入/释放都会以可区分原因被拒绝；</li>
 *   <li>同一请求标识的结果原样重放，底层异常被归一化为冲突 / 失效 / 临时不可用。</li>
 * </ul>
 */
@Service
public class LeaseCoordinatorImpl implements LeaseCoordinator {

    private static final Logger log = LoggerFactory.getLogger(LeaseCoordinatorImpl.class);

    private final FileLeaseStore leaseStore;
    private final FileIdempotencyStore idempotencyStore;
    private final WriteLedger writeLedger;
    private final LeaseProperties properties;
    private final SystemClock clock;
    private final ObjectMapper mapper;
    private final ConcurrentHashMap<String, ReentrantLock> resourceLocks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ReentrantLock> keyLocks = new ConcurrentHashMap<>();

    public LeaseCoordinatorImpl(FileLeaseStore leaseStore,
                                FileIdempotencyStore idempotencyStore,
                                WriteLedger writeLedger,
                                LeaseProperties properties,
                                SystemClock clock,
                                ObjectMapper mapper) {
        this.leaseStore = leaseStore;
        this.idempotencyStore = idempotencyStore;
        this.writeLedger = writeLedger;
        this.properties = properties;
        this.clock = clock;
        this.mapper = mapper;
    }

    // ============================ 对外操作 ============================

    @Override
    public Outcome acquire(String requestId, String resourceId, String holderId, Long ttlMillis) {
        String fp = fingerprint(ActionType.ACQUIRE, resourceId, holderId,
                "ttl=" + (ttlMillis == null ? "default" : ttlMillis));
        return executeIdempotent(ActionType.ACQUIRE, requestId, fp, resourceId, () -> {
            long now = clock.nowMillis();
            long ttl = ttlMillis == null ? properties.getDefaultTtlMillis() : ttlMillis;
            LeaseRecord existing = leaseStore.find(resourceId);
            if (existing == null) {
                LeaseRecord rec = newRecord(resourceId, holderId, 1L, now, now + ttl);
                leaseStore.insert(rec);
                log.info("requestId={} action=ACQUIRE resource={} holder={} token={} decision=ACCEPTED basis='fresh resource token allocated'",
                        requestId, resourceId, holderId, rec.getToken());
                return Outcome.ok(success(requestId, resourceId, holderId, rec.getToken(), rec.getExpiresAt()));
            }
            boolean active = existing.getState() == LeaseState.ACTIVE && existing.getExpiresAt() > now;
            if (active) {
                if (holderId.equals(existing.getHolderId())) {
                    log.info("requestId={} action=ACQUIRE resource={} holder={} token={} decision=REJECTED reason=ALREADY_HELD basis='same holder still active'",
                            requestId, resourceId, holderId, existing.getToken());
                    return reject(LeaseErrorCode.ALREADY_HELD, 409, requestId,
                            "resource " + resourceId + " is already held by this holder with token "
                                    + existing.getToken());
                }
                log.info("requestId={} action=ACQUIRE resource={} holder={} currentToken={} decision=REJECTED reason=RESOURCE_BUSY basis='held by other active holder'",
                        requestId, resourceId, holderId, existing.getToken());
                return reject(LeaseErrorCode.RESOURCE_BUSY, 409, requestId,
                        "resource " + resourceId + " is actively held by another holder");
            }
            // 已过期或已释放：允许接管，令牌严格递增
            long nextToken = leaseStore.nextToken(resourceId);
            LeaseRecord next = newRecord(resourceId, holderId, nextToken, now, now + ttl);
            boolean cas = leaseStore.compareAndUpdate(resourceId, existing.getToken(), next);
            if (!cas) {
                log.info("requestId={} action=ACQUIRE resource={} holder={} decision=REJECTED reason=STORAGE_CONFLICT basis='cas lost during takeover'",
                        requestId, resourceId, holderId);
                return reject(LeaseErrorCode.STORAGE_CONFLICT, 409, requestId,
                        "storage conflict while taking over resource " + resourceId);
            }
            log.info("requestId={} action=ACQUIRE resource={} holder={} oldToken={} token={} decision=ACCEPTED basis='expired/released lease taken over, token advanced'",
                    requestId, resourceId, holderId, existing.getToken(), nextToken);
            return Outcome.ok(success(requestId, resourceId, holderId, nextToken, next.getExpiresAt()));
        });
    }

    @Override
    public Outcome renew(String requestId, String resourceId, String holderId, long token, Long ttlMillis) {
        String fp = fingerprint(ActionType.RENEW, resourceId, holderId,
                "token=" + token, "ttl=" + (ttlMillis == null ? "default" : ttlMillis));
        return executeIdempotent(ActionType.RENEW, requestId, fp, resourceId, () -> {
            long now = clock.nowMillis();
            long ttl = ttlMillis == null ? properties.getDefaultTtlMillis() : ttlMillis;
            LeaseRecord rec = leaseStore.find(resourceId);
            if (rec == null) {
                return reject(LeaseErrorCode.UNKNOWN_TOKEN, 422, requestId,
                        "no lease exists for resource " + resourceId);
            }
            if (rec.getState() == LeaseState.RELEASED) {
                return reject(LeaseErrorCode.UNKNOWN_TOKEN, 422, requestId,
                        "lease for resource " + resourceId + " was released; current token is "
                                + rec.getToken());
            }
            if (rec.getExpiresAt() <= now) {
                log.info("requestId={} action=RENEW resource={} holder={} token={} currentToken={} decision=REJECTED reason=LEASE_EXPIRED basis='now>{} expiresAt={}'",
                        requestId, resourceId, holderId, token, rec.getToken(), now, rec.getExpiresAt());
                return reject(LeaseErrorCode.LEASE_EXPIRED, 410, requestId,
                        "lease expired at " + rec.getExpiresAt() + " and may be taken over");
            }
            // 令牌新旧判定优先于持有者判定：旧持有者续约必须得到可区分的 PREEMPTED 原因
            if (token < rec.getToken()) {
                log.info("requestId={} action=RENEW resource={} holder={} token={} currentToken={} decision=REJECTED reason=LEASE_PREEMPTED basis='stale token, resource preempted by higher token'",
                        requestId, resourceId, holderId, token, rec.getToken());
                return reject(LeaseErrorCode.LEASE_PREEMPTED, 410, requestId,
                        "token " + token + " was superseded by token " + rec.getToken());
            }
            if (token > rec.getToken()) {
                return reject(LeaseErrorCode.UNKNOWN_TOKEN, 422, requestId,
                        "token " + token + " is ahead of known token " + rec.getToken());
            }
            if (!holderId.equals(rec.getHolderId())) {
                log.info("requestId={} action=RENEW resource={} holder={} token={} currentToken={} decision=REJECTED reason=HOLDER_MISMATCH basis='record holder differs'",
                        requestId, resourceId, holderId, token, rec.getToken());
                return reject(LeaseErrorCode.HOLDER_MISMATCH, 422, requestId,
                        "token " + token + " is not held by " + holderId);
            }
            LeaseRecord next = newRecord(resourceId, holderId, token, rec.getAcquiredAt(), now + ttl);
            next.setState(LeaseState.ACTIVE);
            boolean cas = renewWithTimeout(resourceId, rec.getToken(), next, requestId);
            if (!cas) {
                return reject(LeaseErrorCode.STORAGE_CONFLICT, 409, requestId,
                        "storage conflict while renewing resource " + resourceId);
            }
            log.info("requestId={} action=RENEW resource={} holder={} token={} expiresAt={} decision=ACCEPTED basis='holder and current token match'",
                    requestId, resourceId, holderId, token, next.getExpiresAt());
            return Outcome.ok(success(requestId, resourceId, holderId, token, next.getExpiresAt()));
        });
    }

    @Override
    public Outcome release(String requestId, String resourceId, String holderId, long token) {
        String fp = fingerprint(ActionType.RELEASE, resourceId, holderId, "token=" + token);
        return executeIdempotent(ActionType.RELEASE, requestId, fp, resourceId, () -> {
            LeaseRecord rec = leaseStore.find(resourceId);
            if (rec == null) {
                return reject(LeaseErrorCode.UNKNOWN_TOKEN, 422, requestId,
                        "no lease exists for resource " + resourceId);
            }
            if (rec.getState() == LeaseState.RELEASED) {
                if (token == rec.getToken() && holderId.equals(rec.getHolderId())) {
                    log.info("requestId={} action=RELEASE resource={} holder={} token={} decision=ACCEPTED basis='already released idempotently'",
                            requestId, resourceId, holderId, token);
                    return Outcome.ok(released(requestId, resourceId, holderId, token));
                }
                return reject(LeaseErrorCode.UNKNOWN_TOKEN, 422, requestId,
                        "resource " + resourceId + " was released at token " + rec.getToken());
            }
            if (!holderId.equals(rec.getHolderId())) {
                return reject(LeaseErrorCode.HOLDER_MISMATCH, 422, requestId,
                        "token " + token + " is not held by " + holderId);
            }
            if (token < rec.getToken()) {
                log.info("requestId={} action=RELEASE resource={} holder={} token={} currentToken={} decision=REJECTED reason=LEASE_PREEMPTED basis='stale token after takeover'",
                        requestId, resourceId, holderId, token, rec.getToken());
                return reject(LeaseErrorCode.LEASE_PREEMPTED, 410, requestId,
                        "token " + token + " was superseded by token " + rec.getToken());
            }
            if (token > rec.getToken()) {
                return reject(LeaseErrorCode.UNKNOWN_TOKEN, 422, requestId,
                        "token " + token + " is ahead of known token " + rec.getToken());
            }
            LeaseRecord next = newRecord(resourceId, holderId, token, rec.getAcquiredAt(), rec.getExpiresAt());
            next.setState(LeaseState.RELEASED);
            boolean cas = leaseStore.compareAndUpdate(resourceId, token, next);
            if (!cas) {
                return reject(LeaseErrorCode.STORAGE_CONFLICT, 409, requestId,
                        "storage conflict while releasing resource " + resourceId);
            }
            log.info("requestId={} action=RELEASE resource={} holder={} token={} decision=ACCEPTED basis='current holder with current token'",
                    requestId, resourceId, holderId, token);
            return Outcome.ok(released(requestId, resourceId, holderId, token));
        });
    }

    @Override
    public Outcome fencedWrite(String requestId, String resourceId, String holderId, long token,
                               String payload) {
        String fp = fingerprint(ActionType.WRITE, resourceId, holderId, "token=" + token,
                "payload=" + (payload == null ? "" : payload));
        return executeIdempotent(ActionType.WRITE, requestId, fp, resourceId, () -> {
            long now = clock.nowMillis();
            LeaseRecord rec = leaseStore.find(resourceId);
            if (rec == null) {
                return reject(LeaseErrorCode.UNKNOWN_TOKEN, 422, requestId,
                        "no lease exists for resource " + resourceId);
            }
            if (rec.getState() == LeaseState.RELEASED) {
                return reject(LeaseErrorCode.UNKNOWN_TOKEN, 422, requestId,
                        "lease for resource " + resourceId + " was released");
            }
            if (rec.getExpiresAt() <= now) {
                log.info("requestId={} action=WRITE resource={} holder={} token={} currentToken={} decision=REJECTED reason=LEASE_EXPIRED basis='lease expired before write'",
                        requestId, resourceId, holderId, token, rec.getToken());
                return reject(LeaseErrorCode.LEASE_EXPIRED, 410, requestId,
                        "cannot write with an expired lease");
            }
            // 旧令牌（无论持有者是谁）一律以 TOKEN_SUPERSEDED 拒绝，保证 fencing 语义
            if (token < rec.getToken()) {
                log.info("requestId={} action=WRITE resource={} holder={} token={} currentToken={} decision=REJECTED reason=TOKEN_SUPERSEDED basis='write fencing: stale token rejected'",
                        requestId, resourceId, holderId, token, rec.getToken());
                return reject(LeaseErrorCode.TOKEN_SUPERSEDED, 410, requestId,
                        "write token " + token + " was superseded by token " + rec.getToken());
            }
            if (token > rec.getToken()) {
                return reject(LeaseErrorCode.UNKNOWN_TOKEN, 422, requestId,
                        "write token " + token + " is ahead of known token " + rec.getToken());
            }
            if (!holderId.equals(rec.getHolderId())) {
                return reject(LeaseErrorCode.HOLDER_MISMATCH, 422, requestId,
                        "token " + token + " is not held by " + holderId);
            }
            // 写入台账的单调校验：相同令牌（重复写）同样按 SUPERSEDED 拒绝
            if (writeLedger.lastToken(resourceId) >= token) {
                log.info("requestId={} action=WRITE resource={} holder={} token={} currentToken={} decision=REJECTED reason=TOKEN_SUPERSEDED basis='write ledger already saw token >= {}'",
                        requestId, resourceId, holderId, token, rec.getToken(), token);
                return reject(LeaseErrorCode.TOKEN_SUPERSEDED, 410, requestId,
                        "write token " + token + " was already consumed by an earlier write");
            }
            // 令牌有效，写入台账做最后一道单调校验
            writeLedger.acceptWrite(resourceId, holderId, token, payload);
            log.info("requestId={} action=WRITE resource={} holder={} token={} decision=ACCEPTED basis='current token, ledger monotonic check passed'",
                    requestId, resourceId, holderId, token);
            LeaseResult r = success(requestId, resourceId, holderId, token, rec.getExpiresAt());
            r.setDecisionBasis("write accepted at current token " + token);
            return Outcome.ok(r);
        });
    }

    @Override
    public int reapExpired() {
        long now = clock.nowMillis();
        int reaped = 0;
        for (String resourceId : leaseStore.listResourceIds()) {
            ReentrantLock lock = lockFor(resourceId);
            lock.lock();
            try {
                LeaseRecord rec = leaseStore.find(resourceId);
                if (rec != null && rec.getState() == LeaseState.ACTIVE && rec.getExpiresAt() <= now) {
                    LeaseRecord next = newRecord(resourceId, rec.getHolderId(), rec.getToken(),
                            rec.getAcquiredAt(), rec.getExpiresAt());
                    next.setState(LeaseState.EXPIRED);
                    if (leaseStore.compareAndUpdate(resourceId, rec.getToken(), next)) {
                        reaped++;
                        log.info("reaper resource={} holder={} token={} expiredAt={} marked EXPIRED",
                                resourceId, rec.getHolderId(), rec.getToken(), rec.getExpiresAt());
                    }
                }
            } finally {
                lock.unlock();
            }
        }
        return reaped;
    }

    // ============================ 幂等与归一化 ============================

    @FunctionalInterface
    private interface Action {
        Outcome run() throws Exception;
    }

    private Outcome executeIdempotent(ActionType action, String requestId, String fingerprint,
                                      String resourceId, Action body) {
        Optional<StoredResponse> existing = idempotencyStore.find(requestId);
        if (existing.isPresent()) {
            StoredResponse stored = existing.get();
            if (!action.name().equals(stored.getAction()) || !fingerprint.equals(stored.getFingerprint())) {
                log.warn("requestId={} action={} decision=REJECTED reason=IDEMPOTENT_REPLAY_MISMATCH basis='same key used for a different request'",
                        requestId, action);
                return reject(LeaseErrorCode.IDEMPOTENT_REPLAY_MISMATCH, 422, requestId,
                        "Idempotency-Key " + requestId + " was already used for a different request");
            }
            Outcome replayed = deserialize(stored, requestId);
            replayed.setReplayed(true);
            if (replayed.getResult() != null) {
                replayed.getResult().setReplayed(true);
            }
            log.info("requestId={} action={} resource={} decision=REPLAY basis='returning stored first response httpStatus={}'",
                    requestId, action, resourceId, stored.getHttpStatus());
            return replayed;
        }

        // 同一请求标识全局串行化：保证并发同 key 时只有一个线程执行业务，其余重放
        ReentrantLock keyLock = keyLocks.computeIfAbsent(requestId, k -> new ReentrantLock());
        keyLock.lock();
        try {
            Optional<StoredResponse> raced = idempotencyStore.find(requestId);
            if (raced.isPresent()) {
                return replayOutcome(action, requestId, fingerprint, resourceId, raced.get());
            }
            ReentrantLock lock = lockFor(resourceId);
            lock.lock();
            Outcome outcome;
            try {
                outcome = body.run();
        } catch (LeaseRenewalTimeoutException e) {
            log.warn("requestId={} action={} resource={} decision=REJECTED reason=TEMPORARILY_UNAVAILABLE basis='renewal exceeded deadline: {}'",
                    requestId, action, resourceId, e.getMessage());
            outcome = reject(LeaseErrorCode.TEMPORARILY_UNAVAILABLE, 503, requestId,
                    "renewal timed out, please retry");
        } catch (StorageConflictException e) {
            log.warn("requestId={} action={} resource={} decision=REJECTED reason=STORAGE_CONFLICT basis='{}'",
                    requestId, action, resourceId, e.getMessage());
            outcome = reject(LeaseErrorCode.STORAGE_CONFLICT, 409, requestId,
                    "storage conflict, please retry");
        } catch (RecordMissingException e) {
            log.warn("requestId={} action={} resource={} decision=REJECTED reason=RECORD_MISSING basis='{}'",
                    requestId, action, resourceId, e.getMessage());
            outcome = reject(LeaseErrorCode.RECORD_MISSING, 410, requestId,
                    "lease record is missing, please re-acquire");
        } catch (UncheckedIOException e) {
            log.error("requestId={} action={} resource={} decision=REJECTED reason=TEMPORARILY_UNAVAILABLE basis='local storage io failure'",
                    requestId, action, resourceId, e);
            outcome = reject(LeaseErrorCode.TEMPORARILY_UNAVAILABLE, 503, requestId,
                    "local storage temporarily unavailable, please retry");
        } catch (Exception e) {
            log.error("requestId={} action={} resource={} decision=REJECTED reason=TEMPORARILY_UNAVAILABLE basis='unexpected {}'",
                    requestId, action, resourceId, e.getClass().getSimpleName(), e);
            outcome = reject(LeaseErrorCode.TEMPORARILY_UNAVAILABLE, 503, requestId,
                    "temporarily unavailable, please retry");
            } finally {
                lock.unlock();
            }

            // 确定性结果（成功与业务拒绝，如 RESOURCE_BUSY/LEASE_PREEMPTED 等）持久化，重放一致；
            // 存储层 STORAGE_CONFLICT 与 TEMPORARILY_UNAVAILABLE 属可重试错误，不占用幂等记录。
            boolean retriable = outcome.getError() != null
                    && (LeaseErrorCode.STORAGE_CONFLICT.name().equals(outcome.getError().getErrorCode())
                    || LeaseErrorCode.TEMPORARILY_UNAVAILABLE.name().equals(outcome.getError().getErrorCode()));
            if (!retriable) {
                idempotencyStore.saveIfAbsent(requestId,
                        new StoredResponse(action.name(), fingerprint, outcome.getHttpStatus(),
                                serializePayload(outcome)));
            } else {
                log.info("requestId={} action={} resource={} 可重试错误 reason={} 不记录幂等，允许同标识重试",
                        requestId, action, resourceId, outcome.getError().getErrorCode());
            }
            return outcome;
        } finally {
            keyLock.unlock();
        }
    }

    private Outcome replayOutcome(ActionType action, String requestId, String fingerprint,
                                  String resourceId, StoredResponse stored) {
        if (!action.name().equals(stored.getAction()) || !fingerprint.equals(stored.getFingerprint())) {
            log.warn("requestId={} action={} decision=REJECTED reason=IDEMPOTENT_REPLAY_MISMATCH basis='same key used for a different request'",
                    requestId, action);
            return reject(LeaseErrorCode.IDEMPOTENT_REPLAY_MISMATCH, 422, requestId,
                    "Idempotency-Key " + requestId + " was already used for a different request");
        }
        Outcome replayed = deserialize(stored, requestId);
        replayed.setReplayed(true);
        if (replayed.getResult() != null) {
            replayed.getResult().setReplayed(true);
        }
        log.info("requestId={} action={} resource={} decision=REPLAY basis='returning stored first response httpStatus={}'",
                requestId, action, resourceId, stored.getHttpStatus());
        return replayed;
    }

    private boolean renewWithTimeout(String resourceId, long expectedToken, LeaseRecord next,
                                     String requestId) {
        long deadline = clock.nowMillis() + properties.getRenewTimeoutMillis();
        return leaseStore.compareAndUpdateWithDeadline(resourceId, expectedToken, next, deadline);
    }

    // ============================ 工具方法 ============================

    private ReentrantLock lockFor(String resourceId) {
        return resourceLocks.computeIfAbsent(resourceId, k -> new ReentrantLock());
    }

    private LeaseRecord newRecord(String resourceId, String holderId, long token, long acquiredAt,
                                  long expiresAt) {
        LeaseRecord rec = new LeaseRecord();
        rec.setResourceId(resourceId);
        rec.setHolderId(holderId);
        rec.setToken(token);
        rec.setAcquiredAt(acquiredAt);
        rec.setExpiresAt(expiresAt);
        rec.setState(LeaseState.ACTIVE);
        return rec;
    }

    private LeaseResult success(String requestId, String resourceId, String holderId, long token,
                                Long expiresAt) {
        LeaseResult r = new LeaseResult();
        r.setAccepted(true);
        r.setReason("ACCEPTED");
        r.setRequestId(requestId);
        r.setResourceId(resourceId);
        r.setHolderId(holderId);
        r.setToken(token);
        r.setExpiresAt(expiresAt);
        return r;
    }

    private LeaseResult released(String requestId, String resourceId, String holderId, long token) {
        LeaseResult r = success(requestId, resourceId, holderId, token, null);
        r.setReason("RELEASED");
        return r;
    }

    private Outcome reject(LeaseErrorCode code, int httpStatus, String requestId, String message) {
        return Outcome.rejected(httpStatus, new ApiError(code.name(), categoryOf(code), message, requestId));
    }

    private String categoryOf(LeaseErrorCode code) {
        return switch (code) {
            case RESOURCE_BUSY, ALREADY_HELD, STORAGE_CONFLICT, IDEMPOTENT_REPLAY_MISMATCH -> "CONFLICT";
            case UNKNOWN_TOKEN, LEASE_EXPIRED, LEASE_PREEMPTED, TOKEN_SUPERSEDED, HOLDER_MISMATCH,
                 RECORD_MISSING -> "INVALIDATION";
            case TEMPORARILY_UNAVAILABLE -> "UNAVAILABLE";
        };
    }

    private String fingerprint(Object... parts) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            for (Object part : parts) {
                md.update(String.valueOf(part).getBytes(StandardCharsets.UTF_8));
                md.update((byte) '|');
            }
            return HexFormat.of().formatHex(md.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private String serializePayload(Outcome outcome) {
        try {
            return mapper.writeValueAsString(outcome.getResult() != null ? outcome.getResult()
                    : outcome.getError());
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("cannot serialize outcome", e);
        }
    }

    private Outcome deserialize(StoredResponse stored, String requestId) {
        try {
            Outcome o = new Outcome();
            o.setHttpStatus(stored.getHttpStatus());
            if (stored.getHttpStatus() == 200) {
                o.setResult(mapper.readValue(stored.getBodyJson(), LeaseResult.class));
            } else {
                o.setError(mapper.readValue(stored.getBodyJson(), ApiError.class));
            }
            return o;
        } catch (IOException e) {
            throw new UncheckedIOException("cannot deserialize idem record " + requestId, e);
        }
    }
}
