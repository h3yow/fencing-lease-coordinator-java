package com.github.h3yow.fencingleasecoordinatorjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@Import(TestConfig.class)
class LeaseApiTests {

    private static final Logger log = LoggerFactory.getLogger(LeaseApiTests.class);

    @Autowired
    private MockMvc mockMvc;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private InMemoryLeaseStore store;
    @Autowired
    private MutableMillisClock clock;

    private Map<String, Object> body(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }

    @Test
    void httpLifecycleFencingAndIdempotency() throws Exception {
        store.reset();

        MvcResult a1 = mockMvc.perform(post("/api/leases/acquire")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(body(
                                "requestId", "http-req-1",
                                "resourceId", "widget-42",
                                "holderId", "consumer-A",
                                "ttlMillis", 500))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.token").value(1))
                .andReturn();
        log.info("[http] A acquire -> {}", a1.getResponse().getContentAsString());

        mockMvc.perform(post("/api/leases/acquire")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(body(
                                "requestId", "http-req-2",
                                "resourceId", "widget-42",
                                "holderId", "consumer-B",
                                "ttlMillis", 500))))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.errorCode").value("CONFLICT"));

        MvcResult replay = mockMvc.perform(post("/api/leases/acquire")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(body(
                                "requestId", "http-req-1",
                                "resourceId", "widget-42",
                                "holderId", "consumer-A",
                                "ttlMillis", 500))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.replay").value(true))
                .andExpect(jsonPath("$.token").value(1))
                .andReturn();
        log.info("[http] A replay -> {}", replay.getResponse().getContentAsString());

        clock.advance(600);

        mockMvc.perform(post("/api/leases/write")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(body(
                                "requestId", "http-req-3",
                                "resourceId", "widget-42",
                                "holderId", "consumer-A",
                                "token", 1,
                                "value", "from-stale-A"))))
                .andExpect(status().isGone())
                .andExpect(jsonPath("$.errorCode").value("LEASE_EXPIRED"));

        mockMvc.perform(post("/api/leases/acquire")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(body(
                                "requestId", "http-req-4",
                                "resourceId", "widget-42",
                                "holderId", "consumer-B",
                                "ttlMillis", 5000))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.token").value(2))
                .andReturn();

        MvcResult fencedWrite = mockMvc.perform(post("/api/leases/write")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(body(
                                "requestId", "http-req-5",
                                "resourceId", "widget-42",
                                "holderId", "consumer-A",
                                "token", 1,
                                "value", "from-stale-A"))))
                .andExpect(status().isGone())
                .andExpect(jsonPath("$.errorCode").value("FENCED"))
                .andReturn();
        JsonNode fencedJson = objectMapper.readTree(fencedWrite.getResponse().getContentAsString());
        log.info("[http] stale A write -> code={}, reason='{}', decision='{}'",
                fencedJson.path("errorCode").asText(),
                fencedJson.path("reason").asText(),
                fencedJson.path("decision").asText());

        mockMvc.perform(get("/api/leases/resources/widget-42"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.token").value(2))
                .andExpect(jsonPath("$.holderId").value("consumer-B"));
    }

    @Test
    void httpInvalidRequestIsNormalized() throws Exception {
        mockMvc.perform(post("/api/leases/acquire")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errorCode").value("INVALID_REQUEST"));
    }

    @Test
    void httpWaitAcquireTimeoutAndRequestMismatch() throws Exception {
        store.reset();

        // Owner holds the lease.
        mockMvc.perform(post("/api/leases/acquire")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(body(
                                "requestId", "http-wait-owner",
                                "resourceId", "widget-wait",
                                "holderId", "owner",
                                "ttlMillis", 60000))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.token").value(1));

        // Waiting acquire blocks in the controller thread; run it async and let the
        // mock clock push it past its wait deadline.
        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newSingleThreadExecutor();
        java.util.concurrent.Future<MvcResult> waiting = pool.submit(() -> mockMvc.perform(
                        post("/api/leases/acquire")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(body(
                                        "requestId", "http-wait-1",
                                        "resourceId", "widget-wait",
                                        "holderId", "waiter",
                                        "ttlMillis", 1000,
                                        "waitMillis", 300))))
                .andReturn());
        Thread.sleep(300); // allow the request to enqueue
        clock.advance(400);

        MvcResult timedOut = waiting.get(5, java.util.concurrent.TimeUnit.SECONDS);
        pool.shutdown();
        log.info("[http] wait timeout -> {}", timedOut.getResponse().getContentAsString());
        JsonNode timeoutJson = objectMapper.readTree(timedOut.getResponse().getContentAsString());
        org.junit.jupiter.api.Assertions.assertEquals(408, timedOut.getResponse().getStatus());
        org.junit.jupiter.api.Assertions.assertEquals("WAIT_TIMEOUT", timeoutJson.path("errorCode").asText());

        // Same requestId + same content -> recorded WAIT_TIMEOUT replayed.
        mockMvc.perform(post("/api/leases/acquire")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(body(
                                "requestId", "http-wait-1",
                                "resourceId", "widget-wait",
                                "holderId", "waiter",
                                "ttlMillis", 1000,
                                "waitMillis", 300))))
                .andExpect(status().is(408))
                .andExpect(jsonPath("$.errorCode").value("WAIT_TIMEOUT"))
                .andExpect(jsonPath("$.replay").value(true));

        // Same requestId + different content -> REQUEST_MISMATCH (409), old result not returned.
        mockMvc.perform(post("/api/leases/acquire")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(body(
                                "requestId", "http-wait-1",
                                "resourceId", "widget-wait",
                                "holderId", "someone-else",
                                "ttlMillis", 1000,
                                "waitMillis", 300))))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.errorCode").value("REQUEST_MISMATCH"));
    }
}
