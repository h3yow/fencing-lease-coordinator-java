package com.github.h3yow.fencingleasecoordinatorjava.service;

import org.springframework.stereotype.Component;

/** 可替换的时间源，便于测试。 */
@Component
public class SystemClock {
    public long nowMillis() {
        return System.currentTimeMillis();
    }
}
