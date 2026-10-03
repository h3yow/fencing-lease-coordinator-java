package com.github.h3yow.fencingleasecoordinatorjava.tests;

import com.github.h3yow.fencingleasecoordinatorjava.domain.LeaseRecord;
import com.github.h3yow.fencingleasecoordinatorjava.dto.LeaseResponse;
import com.github.h3yow.fencingleasecoordinatorjava.error.StoreException;
import com.github.h3yow.fencingleasecoordinatorjava.store.IdempotentResult;
import com.github.h3yow.fencingleasecoordinatorjava.store.LeaseStore;
import com.github.h3yow.fencingleasecoordinatorjava.store.PersistentData;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

public class InMemoryLeaseStore implements LeaseStore {

    private final Map<String, LeaseRecord> leases = new ConcurrentHashMap<>();
    private final Map<String, LeaseResponse> responses = new ConcurrentHashMap<>();
    private final Map<String, String> responseFingerprints = new ConcurrentHashMap<>();
    private final Map<String, PersistentData.ResourceValue> resources = new ConcurrentHashMap<>();
    private final ReentrantLock lock = new ReentrantLock();

    public volatile boolean faultOnPersist = false;

    @Override
    public Optional<LeaseRecord> findLease(String resourceId) {
        lock.lock();
        try {
            return Optional.ofNullable(leases.get(resourceId));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void putLeaseIfAbsent(String resourceId, LeaseRecord expected, LeaseRecord next) {
        lock.lock();
        try {
            LeaseRecord current = leases.get(resourceId);
            if (!sameIdentity(current, expected)) {
                throw new StoreException("in-memory create conflict on " + resourceId);
            }
            if (faultOnPersist) {
                throw new StoreException("injected persistence I/O failure");
            }
            leases.put(resourceId, next);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void replaceLease(String resourceId, LeaseRecord expected, LeaseRecord next) {
        lock.lock();
        try {
            LeaseRecord current = leases.get(resourceId);
            if (!sameIdentity(current, expected)) {
                throw new StoreException("in-memory replace conflict on " + resourceId
                        + " expectedToken=" + (expected == null ? null : expected.getToken())
                        + " actualToken=" + (current == null ? null : current.getToken()));
            }
            if (faultOnPersist) {
                throw new StoreException("injected persistence I/O failure");
            }
            leases.put(resourceId, next);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<LeaseRecord> claimLeaseIfFree(String resourceId, String holderId,
                                                  long acquiredAt, long expiresAt) {
        lock.lock();
        try {
            LeaseRecord current = leases.get(resourceId);
            if (current != null && current.getStatus() == com.github.h3yow.fencingleasecoordinatorjava.domain.LeaseStatus.HELD
                    && current.getExpiresAt() > acquiredAt) {
                return Optional.empty();
            }
            if (faultOnPersist) {
                throw new StoreException("injected persistence I/O failure");
            }
            LeaseRecord next = new LeaseRecord();
            next.setResourceId(resourceId);
            next.setHolderId(holderId);
            next.setToken(current == null ? 1L : current.getToken() + 1L);
            next.setStatus(com.github.h3yow.fencingleasecoordinatorjava.domain.LeaseStatus.HELD);
            next.setAcquiredAt(acquiredAt);
            next.setExpiresAt(expiresAt);
            leases.put(resourceId, next);
            return Optional.of(next);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<String> getResourceData(String resourceId) {
        lock.lock();
        try {
            return Optional.ofNullable(resources.get(resourceId)).map(PersistentData.ResourceValue::getValue);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long getResourceVersion(String resourceId) {
        lock.lock();
        try {
            PersistentData.ResourceValue v = resources.get(resourceId);
            return v == null ? 0L : v.getVersion();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void putResourceDataIfVersion(String resourceId, String expected, String value, long version) {
        lock.lock();
        try {
            PersistentData.ResourceValue current = resources.get(resourceId);
            long currentVersion = current == null ? 0L : current.getVersion();
            if (currentVersion != version) {
                throw new StoreException("in-memory version conflict on " + resourceId);
            }
            if (faultOnPersist) {
                throw new StoreException("injected persistence I/O failure");
            }
            PersistentData.ResourceValue v = new PersistentData.ResourceValue();
            v.setValue(value);
            v.setVersion(version + 1);
            resources.put(resourceId, v);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<com.github.h3yow.fencingleasecoordinatorjava.store.IdempotentRecord> findIdempotent(String requestId) {
        lock.lock();
        try {
            LeaseResponse response = responses.get(requestId);
            if (response == null) {
                return Optional.empty();
            }
            return Optional.of(new com.github.h3yow.fencingleasecoordinatorjava.store.IdempotentRecord(
                    responseFingerprints.get(requestId), response));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public IdempotentResult replayOrCompute(String requestId, String fingerprint,
                                            Function<String, LeaseResponse> computer) {
        lock.lock();
        try {
            LeaseResponse existing = responses.get(requestId);
            if (existing != null) {
                boolean mismatch = !java.util.Objects.equals(responseFingerprints.get(requestId), fingerprint);
                return new IdempotentResult(existing, true, mismatch);
            }
            LeaseResponse computed = computer.apply(requestId);
            responses.put(requestId, computed);
            responseFingerprints.put(requestId, fingerprint);
            return new IdempotentResult(computed, false, false);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public IdempotentResult putIdempotentIfAbsent(String requestId, String fingerprint, LeaseResponse response) {
        lock.lock();
        try {
            LeaseResponse existing = responses.get(requestId);
            if (existing != null) {
                boolean mismatch = !java.util.Objects.equals(responseFingerprints.get(requestId), fingerprint);
                return new IdempotentResult(existing, true, mismatch);
            }
            responses.put(requestId, response);
            responseFingerprints.put(requestId, fingerprint);
            return new IdempotentResult(response, false, false);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void reset() {
        lock.lock();
        try {
            leases.clear();
            responses.clear();
            responseFingerprints.clear();
            resources.clear();
            faultOnPersist = false;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Collection<String> allResourceIds() {
        lock.lock();
        try {
            return new ArrayList<>(leases.keySet());
        } finally {
            lock.unlock();
        }
    }

    private static boolean sameIdentity(LeaseRecord current, LeaseRecord expected) {
        if (current == null && expected == null) {
            return true;
        }
        if (current == null || expected == null) {
            return false;
        }
        return current.getToken() == expected.getToken()
                && java.util.Objects.equals(current.getStatus(), expected.getStatus())
                && java.util.Objects.equals(current.getHolderId(), expected.getHolderId());
    }
}
