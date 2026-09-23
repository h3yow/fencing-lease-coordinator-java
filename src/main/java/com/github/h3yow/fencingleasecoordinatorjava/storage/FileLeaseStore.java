package com.github.h3yow.fencingleasecoordinatorjava.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.fencingleasecoordinatorjava.config.LeaseProperties;
import com.github.h3yow.fencingleasecoordinatorjava.service.SystemClock;
import com.github.h3yow.fencingleasecoordinatorjava.domain.LeaseRecord;
import com.github.h3yow.fencingleasecoordinatorjava.error.RecordMissingException;
import com.github.h3yow.fencingleasecoordinatorjava.error.StorageConflictException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

/**
 * 基于本地文件的租约存储，模拟带 CAS 语义的共享状态存储。
 *
 * <p>持久化布局：{@code <dataDir>/leases/<urlencode(resourceId)>.json}。
 * 每次写入采用"临时文件 + 原子 move"，保证单条记录可持久化、不出现半写内容。
 * 同一资源的读-改-写在进程内按资源加锁，配合令牌比较实现 CAS。
 */
public class FileLeaseStore implements LeaseStore {

    private final Path leaseDir;
    private final ObjectMapper mapper;
    private final SystemClock clock;
    private final ConcurrentHashMap<String, ReentrantLock> locks = new ConcurrentHashMap<>();

    /** 测试钩子：置 true 后下一次 compareAndUpdate 强制失败，模拟并发存储冲突。 */
    private volatile boolean forceNextCasFailure;
    /** 测试钩子：置 true 后下一次 find 抛出记录缺失。 */
    private volatile boolean forceNextMissing;
    /** 测试钩子：每次 compareAndUpdate 前睡眠该毫秒数，模拟续约链路延迟。 */
    private volatile long casDelayMillis;
    /** 测试钩子：置 true 后下一次带截止时间的 CAS 直接抛续约超时。 */
    private volatile boolean forceNextRenewTimeout;

    public void setForceNextRenewTimeout(boolean forceNextRenewTimeout) {
        this.forceNextRenewTimeout = forceNextRenewTimeout;
    }

    public void setForceNextCasFailure(boolean v) {
        this.forceNextCasFailure = v;
    }

    public void setForceNextMissing(boolean v) {
        this.forceNextMissing = v;
    }

    public void setCasDelayMillis(long casDelayMillis) {
        this.casDelayMillis = casDelayMillis;
    }

    public FileLeaseStore(LeaseProperties properties, ObjectMapper mapper, SystemClock clock) {
        this.leaseDir = Paths.get(properties.getDataDir(), "leases");
        this.mapper = mapper;
        this.clock = clock;
        try {
            Files.createDirectories(leaseDir);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot create lease dir: " + leaseDir, e);
        }
    }

    private ReentrantLock lockFor(String resourceId) {
        return locks.computeIfAbsent(resourceId, k -> new ReentrantLock());
    }

    private Path fileFor(String resourceId) {
        String name = URLEncoder.encode(resourceId, StandardCharsets.UTF_8).replace("+", "%20");
        return leaseDir.resolve(name + ".json");
    }

    @Override
    public LeaseRecord find(String resourceId) {
        if (forceNextMissing) {
            forceNextMissing = false;
            throw new RecordMissingException("injected missing record: " + resourceId);
        }
        Path file = fileFor(resourceId);
        if (!Files.exists(file)) {
            return null;
        }
        try {
            byte[] bytes = Files.readAllBytes(file);
            if (bytes.length == 0) {
                throw new RecordMissingException("lease record is empty: " + resourceId);
            }
            return mapper.readValue(bytes, LeaseRecord.class);
        } catch (RecordMissingException e) {
            throw e;
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read lease record: " + resourceId, e);
        }
    }

    @Override
    public void insert(LeaseRecord record) {
        ReentrantLock lock = lockFor(record.getResourceId());
        lock.lock();
        try {
            Path file = fileFor(record.getResourceId());
            if (Files.exists(file)) {
                throw new StorageConflictException("lease record already exists: " + record.getResourceId());
            }
            writeAtomically(file, record);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean compareAndUpdate(String resourceId, long expectedToken, LeaseRecord next) {
        ReentrantLock lock = lockFor(resourceId);
        lock.lock();
        try {
            if (casDelayMillis > 0) {
                try {
                    Thread.sleep(casDelayMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new com.github.h3yow.fencingleasecoordinatorjava.error
                            .LeaseRenewalTimeoutException("interrupted during renew", e);
                }
            }
            if (forceNextCasFailure) {
                forceNextCasFailure = false;
                return false;
            }
            LeaseRecord current = find(resourceId);
            if (current == null) {
                throw new RecordMissingException("lease record missing for cas: " + resourceId);
            }
            if (current.getToken() != expectedToken) {
                return false;
            }
            writeAtomically(fileFor(resourceId), next);
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean compareAndUpdateWithDeadline(String resourceId, long expectedToken, LeaseRecord next,
                                                long deadlineMillis) {
        ReentrantLock lock = lockFor(resourceId);
        lock.lock();
        try {
            if (forceNextRenewTimeout) {
                forceNextRenewTimeout = false;
                throw new com.github.h3yow.fencingleasecoordinatorjava.error
                        .LeaseRenewalTimeoutException("injected renewal timeout");
            }
            // 等待资源锁期间可能已经超过续约截止时间
            if (clock.nowMillis() > deadlineMillis) {
                throw new com.github.h3yow.fencingleasecoordinatorjava.error
                        .LeaseRenewalTimeoutException("renewal deadline exceeded before storage cas");
            }
            LeaseRecord current = find(resourceId);
            if (current == null) {
                throw new RecordMissingException("lease record missing for cas: " + resourceId);
            }
            if (current.getToken() != expectedToken) {
                return false;
            }
            writeAtomically(fileFor(resourceId), next);
            if (clock.nowMillis() > deadlineMillis) {
                throw new com.github.h3yow.fencingleasecoordinatorjava.error
                        .LeaseRenewalTimeoutException("renewal deadline exceeded after storage cas");
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 在资源锁内分配下一个令牌；令牌取自当前持久化记录，保证随时间单调递增。
     */
    @Override
    public long nextToken(String resourceId) {
        LeaseRecord current = find(resourceId);
        return current == null ? 1L : current.getToken() + 1L;
    }

    @Override
    public List<String> listResourceIds() {
        List<String> ids = new ArrayList<>();
        try (Stream<Path> files = Files.list(leaseDir)) {
            files.filter(f -> f.getFileName().toString().endsWith(".json")).forEach(f -> {
                String fileName = f.getFileName().toString();
                ids.add(java.net.URLDecoder.decode(
                        fileName.substring(0, fileName.length() - ".json".length()),
                        StandardCharsets.UTF_8));
            });
        } catch (IOException e) {
            throw new UncheckedIOException("cannot list lease records", e);
        }
        return ids;
    }

    private void writeAtomically(Path target, LeaseRecord record) {
        try {
            Path tmp = Files.createTempFile(leaseDir, "lease-", ".tmp");
            Files.writeString(tmp, mapper.writeValueAsString(record), StandardCharsets.UTF_8);
            try {
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException atomicFail) {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("cannot persist lease record: " + target, e);
        }
    }
}
