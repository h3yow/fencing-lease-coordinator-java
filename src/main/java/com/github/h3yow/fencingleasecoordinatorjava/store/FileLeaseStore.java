package com.github.h3yow.fencingleasecoordinatorjava.store;

import com.github.h3yow.fencingleasecoordinatorjava.config.LeaseProperties;
import com.github.h3yow.fencingleasecoordinatorjava.domain.LeaseRecord;
import com.github.h3yow.fencingleasecoordinatorjava.dto.LeaseResponse;
import com.github.h3yow.fencingleasecoordinatorjava.error.LeaseErrorCode;
import com.github.h3yow.fencingleasecoordinatorjava.error.LeaseException;
import com.github.h3yow.fencingleasecoordinatorjava.error.StoreConflictException;
import com.github.h3yow.fencingleasecoordinatorjava.error.StoreException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

@Repository
public class FileLeaseStore implements LeaseStore {

    private static final Logger log = LoggerFactory.getLogger(FileLeaseStore.class);

    private final LeaseProperties properties;
    private final ObjectMapper objectMapper;
    private final ReentrantLock lock = new ReentrantLock();
    private PersistentData data;
    private Path file;

    public FileLeaseStore(LeaseProperties properties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    private void ensureLoaded() {
        if (data != null) {
            return;
        }
        file = Path.of(properties.getStoreFile());
        try {
            if (Files.exists(file)) {
                data = objectMapper.readValue(file.toFile(), PersistentData.class);
                log.info("Loaded lease store from {}: leases={}, responses={}",
                        file, data.getLeases().size(), data.getResponses().size());
            } else {
                data = new PersistentData();
                Files.createDirectories(file.toAbsolutePath().getParent());
                persist();
                log.info("Initialized new lease store at {}", file.toAbsolutePath());
            }
        } catch (IOException e) {
            throw new StoreException("failed to load lease store: " + file, e);
        }
    }

    private void persist() {
        try {
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), data);
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new StoreException("failed to persist lease store: " + file, e);
        }
    }

    @Override
    public Optional<LeaseRecord> findLease(String resourceId) {
        lock.lock();
        try {
            ensureLoaded();
            return Optional.ofNullable(data.getLeases().get(resourceId));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void putLeaseIfAbsent(String resourceId, LeaseRecord expected, LeaseRecord next) {
        lock.lock();
        try {
            ensureLoaded();
            LeaseRecord current = data.getLeases().get(resourceId);
            if (!sameIdentity(current, expected)) {
                throw new StoreConflictException("optimistic-create conflict on resource " + resourceId);
            }
            data.getLeases().put(resourceId, next);
            persist();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void replaceLease(String resourceId, LeaseRecord expected, LeaseRecord next) {
        lock.lock();
        try {
            ensureLoaded();
            LeaseRecord current = data.getLeases().get(resourceId);
            if (!sameIdentity(current, expected)) {
                throw new StoreConflictException("optimistic-replace conflict on resource " + resourceId
                        + " (expected token " + (expected == null ? null : expected.getToken())
                        + ", actual token " + (current == null ? null : current.getToken()) + ")");
            }
            data.getLeases().put(resourceId, next);
            persist();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<String> getResourceData(String resourceId) {
        lock.lock();
        try {
            ensureLoaded();
            return Optional.ofNullable(data.getResources().get(resourceId))
                    .map(PersistentData.ResourceValue::getValue);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long getResourceVersion(String resourceId) {
        lock.lock();
        try {
            ensureLoaded();
            PersistentData.ResourceValue v = data.getResources().get(resourceId);
            return v == null ? 0L : v.getVersion();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void putResourceDataIfVersion(String resourceId, String expected, String value, long version) {
        lock.lock();
        try {
            ensureLoaded();
            PersistentData.ResourceValue current = data.getResources().get(resourceId);
            long currentVersion = current == null ? 0L : current.getVersion();
            if (currentVersion != version) {
                throw new StoreConflictException("resource version conflict on " + resourceId
                        + " (expected " + version + ", actual " + currentVersion + ")");
            }
            PersistentData.ResourceValue next = new PersistentData.ResourceValue();
            next.setValue(value);
            next.setVersion(version + 1);
            data.getResources().put(resourceId, next);
            persist();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<LeaseResponse> getIdempotentResponse(String requestId) {
        lock.lock();
        try {
            ensureLoaded();
            return Optional.ofNullable(data.getResponses().get(requestId));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public IdempotentResult replayOrCompute(String requestId, String fingerprint,
                                            Function<String, LeaseResponse> computer,
                                            java.util.function.Predicate<LeaseResponse> recordable) {
        lock.lock();
        try {
            ensureLoaded();
            LeaseResponse existing = data.getResponses().get(requestId);
            if (existing != null) {
                checkFingerprint(requestId, fingerprint);
                return new IdempotentResult(existing, true);
            }
            LeaseResponse computed;
            try {
                computed = computer.apply(requestId);
            } catch (RuntimeException ex) {
                throw ex;
            }
            if (recordable.test(computed)) {
                data.getResponses().put(requestId, computed);
                data.getRequestFingerprints().put(requestId, fingerprint);
                persist();
            }
            return new IdempotentResult(computed, false);
        } finally {
            lock.unlock();
        }
    }

    private void checkFingerprint(String requestId, String fingerprint) {
        String recorded = data.getRequestFingerprints().get(requestId);
        if (recorded != null && !recorded.equals(fingerprint)) {
            log.warn("idempotency mismatch requestId={} recorded='{}' presented='{}'",
                    requestId, recorded, fingerprint);
            throw new LeaseException(LeaseErrorCode.REQUEST_MISMATCH,
                    "requestId '" + requestId + "' was already used with different request content");
        }
    }

    @Override
    public void reset() {
        lock.lock();
        try {
            data = new PersistentData();
            persist();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public java.util.Collection<String> allResourceIds() {
        lock.lock();
        try {
            ensureLoaded();
            return new java.util.ArrayList<>(data.getLeases().keySet());
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
