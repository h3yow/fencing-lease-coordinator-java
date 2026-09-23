package com.github.h3yow.fencingleasecoordinatorjava.tests;

import com.github.h3yow.fencingleasecoordinatorjava.service.SystemClock;

/** 可控时钟：测试可手动推进时间，模拟租约到期。 */
public class ControllableClock extends SystemClock {
    private volatile long current;

    public ControllableClock(long start) {
        this.current = start;
    }

    @Override
    public long nowMillis() {
        return current;
    }

    public void advance(long deltaMillis) {
        this.current += deltaMillis;
    }
}
