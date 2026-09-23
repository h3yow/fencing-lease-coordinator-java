package com.github.h3yow.fencingleasecoordinatorjava.tests;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;

/**
 * 场景五：HTTP 端到端 —— 申请 -> 写入 -> 旧令牌写入被拒，验证状态码、
 * 错误码大类与 Idempotency-Key 头的完整链路。
 */
public class HttpApiTest extends AbstractLeaseTest {

    @Test
    void acquireWriteAndStaleWriteOverHttp() throws Exception {
        mockMvc.perform(post("/api/leases/acquire")
                        .header("Idempotency-Key", "http-1")
                        .contentType("application/json")
                        .content("{\"resourceId\":\"res-http\",\"holderId\":\"A\",\"ttlMillis\":10000}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.accepted").value(true))
                .andExpect(jsonPath("$.token").value(1));

        mockMvc.perform(post("/api/leases/write")
                        .header("Idempotency-Key", "http-2")
                        .contentType("application/json")
                        .content("{\"resourceId\":\"res-http\",\"holderId\":\"A\",\"token\":1,\"payload\":\"x\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.accepted").value(true));

        // 相同令牌重复写入 -> 410 + 可区分错误码
        mockMvc.perform(post("/api/leases/write")
                        .header("Idempotency-Key", "http-3")
                        .contentType("application/json")
                        .content("{\"resourceId\":\"res-http\",\"holderId\":\"A\",\"token\":1,\"payload\":\"y\"}"))
                .andExpect(status().isGone())
                .andExpect(jsonPath("$.errorCode").value("TOKEN_SUPERSEDED"))
                .andExpect(jsonPath("$.category").value("INVALIDATION"));

        // 缺少幂等头 -> 400
        mockMvc.perform(post("/api/leases/acquire")
                        .contentType("application/json")
                        .content("{\"resourceId\":\"r\",\"holderId\":\"A\"}"))
                .andExpect(status().isBadRequest());
    }
}
