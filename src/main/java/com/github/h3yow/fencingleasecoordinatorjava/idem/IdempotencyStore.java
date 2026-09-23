package com.github.h3yow.fencingleasecoordinatorjava.idem;

import java.util.Optional;

/**
 * 基于请求标识（Idempotency-Key）的幂等记录存储。
 */
public interface IdempotencyStore {

    Optional<StoredResponse> find(String requestId);

    /**
     * 仅当记录不存在时原子保存首次结果。
     *
     * @return true 表示本次赢得首次写入；false 表示记录已存在
     */
    boolean saveIfAbsent(String requestId, StoredResponse response);
}
