package com.github.h3yow.fencingleasecoordinatorjava.idem;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.fencingleasecoordinatorjava.config.LeaseProperties;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Optional;

/**
 * 基于本地文件的幂等记录存储：{@code <dataDir>/idem/<urlencode(requestId)>.json}。
 *
 * <p>同一请求标识首次提交时原子创建记录文件；重复提交直接读取并原样返回首次响应。
 * 若文件已被其他线程创建，则本线程退化为读取者，保证"同一标识只有一个首次结果"。
 */
public class FileIdempotencyStore implements IdempotencyStore {

    private final Path idemDir;
    private final ObjectMapper mapper;

    public FileIdempotencyStore(LeaseProperties properties, ObjectMapper mapper) {
        this.idemDir = Paths.get(properties.getDataDir(), "idem");
        this.mapper = mapper;
        try {
            Files.createDirectories(idemDir);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot create idem dir: " + idemDir, e);
        }
    }

    private Path fileFor(String requestId) {
        String name = URLEncoder.encode(requestId, StandardCharsets.UTF_8).replace("+", "%20");
        return idemDir.resolve(name + ".json");
    }

    @Override
    public Optional<StoredResponse> find(String requestId) {
        Path file = fileFor(requestId);
        if (!Files.exists(file)) {
            return Optional.empty();
        }
        try {
            return Optional.of(mapper.readValue(Files.readAllBytes(file), StoredResponse.class));
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read idem record: " + requestId, e);
        }
    }

    /**
     * 原子保存首次结果。
     *
     * @return true 表示本次调用赢得首次写入；false 表示记录已存在（调用方应转为重放）
     */
    @Override
    public boolean saveIfAbsent(String requestId, StoredResponse response) {
        Path file = fileFor(requestId);
        try {
            Path tmp = Files.createTempFile(idemDir, "idem-", ".tmp");
            Files.writeString(tmp, mapper.writeValueAsString(response), StandardCharsets.UTF_8);
            try {
                // 原子地"仅当不存在时创建"，天然防止两个线程同时成为首次提交者
                Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE);
                return true;
            } catch (IOException atomicFail) {
                Files.deleteIfExists(tmp);
                return false;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("cannot persist idem record: " + requestId, e);
        }
    }
}
