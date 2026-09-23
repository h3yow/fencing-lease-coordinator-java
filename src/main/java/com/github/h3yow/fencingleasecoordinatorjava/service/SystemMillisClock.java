package com.github.h3yow.fencingleasecoordinatorjava.service;

import org.springframework.stereotype.Component;

@Component
public class SystemMillisClock {
    public long now() {
        return System.currentTimeMillis();
    }
}
