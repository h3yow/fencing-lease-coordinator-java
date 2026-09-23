package com.github.h3yow.fencingleasecoordinatorjava.storage;

import com.github.h3yow.fencingleasecoordinatorjava.domain.LeaseRecord;
import java.util.List;

/**
 * 租约记录存储抽象。
 */
public interface LeaseStore {

    LeaseRecord find(String resourceId);

    void insert(LeaseRecord record);

    /**
     * 条件更新：仅当当前持久化令牌等于 expectedToken 时写入。
     *
     * @return 是否更新成功
     */
    boolean compareAndUpdate(String resourceId, long expectedToken, LeaseRecord next);

    /**
     * 带截止时间（epoch millis）的 CAS：等待/落盘过程超过 deadline 时抛出
     * {@code LeaseRenewalTimeoutException}，归一化为临时不可用。
     */
    boolean compareAndUpdateWithDeadline(String resourceId, long expectedToken, LeaseRecord next,
                                         long deadlineMillis);

    /**
     * 下一个全局单调递增令牌（同一资源严格递增）。
     */
    long nextToken(String resourceId);

    /** 列出所有已持久化的资源标识。 */
    List<String> listResourceIds();
}
