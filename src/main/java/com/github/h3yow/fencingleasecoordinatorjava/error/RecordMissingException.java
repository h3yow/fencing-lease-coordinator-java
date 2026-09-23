package com.github.h3yow.fencingleasecoordinatorjava.error;

/** 底层存储记录缺失。 */
public class RecordMissingException extends RuntimeException {
    public RecordMissingException(String message) {
        super(message);
    }
}
