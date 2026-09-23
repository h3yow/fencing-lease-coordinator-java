package com.github.h3yow.fencingleasecoordinatorjava.domain;

/**
 * 受幂等保护的操作类型。
 */
public enum ActionType {
    ACQUIRE,
    RENEW,
    RELEASE,
    WRITE
}
