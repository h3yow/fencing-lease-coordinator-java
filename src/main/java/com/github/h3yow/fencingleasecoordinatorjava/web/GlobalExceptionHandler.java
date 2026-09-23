package com.github.h3yow.fencingleasecoordinatorjava.web;

import com.github.h3yow.fencingleasecoordinatorjava.error.LeaseErrorCode;
import com.github.h3yow.fencingleasecoordinatorjava.error.LeaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(LeaseException.class)
    public ResponseEntity<Map<String, Object>> handleLease(LeaseException ex) {
        LeaseErrorCode code = ex.getErrorCode();
        log.warn("normalized error code={} message='{}'", code, ex.getMessage());
        return ResponseEntity.status(code.httpStatus()).body(body(code, ex.getMessage()));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<Map<String, Object>> handleUnreadable(HttpMessageNotReadableException ex) {
        return ResponseEntity.badRequest()
                .body(body(LeaseErrorCode.INVALID_REQUEST, "malformed JSON request body"));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleUnexpected(Exception ex) {
        log.error("unexpected error, normalized to TEMPORARILY_UNAVAILABLE", ex);
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(body(LeaseErrorCode.TEMPORARILY_UNAVAILABLE,
                        "temporarily unavailable, please retry"));
    }

    private static Map<String, Object> body(LeaseErrorCode code, String message) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("success", false);
        map.put("errorCode", code.name());
        map.put("reason", message);
        return map;
    }
}
