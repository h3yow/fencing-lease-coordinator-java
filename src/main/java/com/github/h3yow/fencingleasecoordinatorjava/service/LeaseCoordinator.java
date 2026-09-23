package com.github.h3yow.fencingleasecoordinatorjava.service;

/**
 * 租约协调核心服务：申请 / 续约 / 释放 / 到期回收 / 带令牌写入。
 * 所有方法返回 {@link Outcome}（成功结果或归一化错误），天然支持幂等重放一致响应。
 */
public interface LeaseCoordinator {

    Outcome acquire(String requestId, String resourceId, String holderId, Long ttlMillis);

    Outcome renew(String requestId, String resourceId, String holderId, long token, Long ttlMillis);

    Outcome release(String requestId, String resourceId, String holderId, long token);

    /** 带 fencing token 的资源写入；相同或更旧令牌一律拒绝。 */
    Outcome fencedWrite(String requestId, String resourceId, String holderId, long token, String payload);

    /** 到期回收：使所有过期租约失效，返回回收数量。 */
    int reapExpired();
}
