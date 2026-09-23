package com.github.h3yow.fencingleasecoordinatorjava.service;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * 协调器统一返回信封：要么携带 {@link LeaseResult}，要么携带 {@link ApiError}。
 * 幂等重放时 {@code replayed=true}，httpStatus 与首次完全一致。
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Outcome {
    private int httpStatus;
    private LeaseResult result;
    private ApiError error;
    private boolean replayed;

    public Outcome() {
    }

    public static Outcome ok(LeaseResult result) {
        Outcome o = new Outcome();
        o.httpStatus = 200;
        o.result = result;
        return o;
    }

    public static Outcome rejected(int httpStatus, ApiError error) {
        Outcome o = new Outcome();
        o.httpStatus = httpStatus;
        o.error = error;
        return o;
    }

    public int getHttpStatus() {
        return httpStatus;
    }

    public void setHttpStatus(int httpStatus) {
        this.httpStatus = httpStatus;
    }

    public LeaseResult getResult() {
        return result;
    }

    public void setResult(LeaseResult result) {
        this.result = result;
    }

    public ApiError getError() {
        return error;
    }

    public void setError(ApiError error) {
        this.error = error;
    }

    public boolean isReplayed() {
        return replayed;
    }

    public void setReplayed(boolean replayed) {
        this.replayed = replayed;
    }
}
