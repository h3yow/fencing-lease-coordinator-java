package com.github.h3yow.fencingleasecoordinatorjava.tests;

import com.github.h3yow.fencingleasecoordinatorjava.service.SystemMillisClock;

public class MutableMillisClock extends SystemMillisClock {

    private volatile long time;

    public MutableMillisClock() {
        this.time = 1_700_000_000_000L;
    }

    @Override
    public long now() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public void advance(long deltaMillis) {
        this.time += deltaMillis;
    }
}
