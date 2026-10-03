# Fencing Lease Coordinator（租约协调服务）

一个可本地运行的 Spring Boot 服务，用**本地 JSON 文件持久化**模拟分布式共享存储，为多个并发消费者提供
**带 fencing token（隔离令牌）的租约协调**能力：申请（acquire）、续约（renew）、主动释放（release）、
到期回收（reap）以及受令牌保护的资源写入（write）。

## 1. 租约与令牌语义

### 租约生命周期

```
        acquire(无记录/已过期)         到期(reaper 或惰性判定)
   ┌──────────────────────────► HELD ───────────────────────────► EXPIRED
   │                              │                                 │
   │                              │ release(token 匹配)             │ 任意时刻
   │                              ▼                                 │ 可被新的
   └────────────────────────── RELEASED ◄───────────────────────────┘ acquire 接管
```

- 每个资源（`resourceId`）在存储中只保留**最新一条租约记录**。
- 租约带有 `expiresAt`；到期后不会自动删除，而是由后台 reaper（默认每 2 秒）标记为 `EXPIRED`，
  也可在续约/写入时被惰性判定为过期。
- `POST /api/leases/reap` 可手动触发一次回收。

### Fencing Token 单调性（核心不变量）

- 令牌是一个 `long`，**按资源维度**单调递增：资源的第一次申请得到令牌 `1`；
  此后每次在「无有效持有者」（租约已释放或已过期）时成功申请，令牌在上一条记录基础上 `+1`。
- 续约与释放**不改变令牌**；只有接管（重新 acquire）才会产生更高令牌。
- 所有写操作都必须携带 `(holderId, token)`，判定顺序为：
  1. 资源无记录 → `LEASE_UNKNOWN`
  2. `当前令牌 > 提交令牌` → `FENCED`（旧持有者已被更高令牌接管）
  3. 令牌/持有者与记录不符（提交令牌更大或持有者不匹配）→ `LEASE_UNKNOWN`
  4. 状态非 HELD 或已过期 → `LEASE_EXPIRED`
  5. 全部通过 → 操作生效
- 因此**不可能**出现两个持有者以相同或更旧令牌写入成功：被接管的旧持有者后续续约、释放、写入一律被拒。

> 存储层通过单进程 `ReentrantLock` 串行化所有读改写，并以「期望记录 (token,status,holder) → 替换」的
> 乐观 CAS 提交；冲突会抛底层 `StoreException`，由服务层归一化为 `TEMPORARILY_UNAVAILABLE`，
> 绝不会出现静默的双重授权。

### 受保护写入与版本

- `write` 在通过令牌校验后写入资源值，并维护一个从 1 开始递增的 `version`，同样使用 CAS 提交。

## 1.5 等待式申请（waitMillis，可选）

`acquire` 支持可选字段 `waitMillis`（毫秒）。**不传时保持原有即时行为**（占用即 `CONFLICT`），
老接入方完全不受影响；传入后语义变为「等一等再说」：

- 资源正被有效持有时，请求进入该资源的 **FIFO 等待队列**，在预算内等待对方 `release`
  或租约到期；等到后按原有规则在上一任令牌基础上 `+1` 授权。
- **公平性**：只有队首等待者被允许尝试获取，因此授权顺序严格等于入队顺序，
  不会插队、不会有人被一直饿着；任一时刻至多一个等待者胜出，令牌由存储层在
  同一临界区内推导（`current.token + 1`），不可能发出相同或更旧的令牌。
- **等待上限**：`waitMillis` 会被夹取到 `lease.max-wait-millis`（默认 30s）以内；
  每个资源的排队人数受 `lease.max-wait-queue-size`（默认 64）限制，满了以后新请求
  立即得到 `QUEUE_FULL`，不会无限堆积。
- **不空转**：等待者阻塞在条件变量上；`release`、reaper 标记到期、队首完成时都会
  唤醒队列，队首还会在当前租约的 `expiresAt` 时刻自行醒来重试，无需轮询刷 CPU。
- **超时不抢锁**：等待预算耗尽即返回 `WAIT_TIMEOUT` 并退出队列，之后绝不会再
  「阴差阳错」抢到租约。
- **等待者优先**：只要某资源的等待队列非空，即时申请（不带 `waitMillis`）会被
  `CONFLICT` 拒绝并让位于排队者，防止后来者插队。
- 同一 `requestId` 并发重复提交等待请求时，队列内会去重：重复方观察首个等待者的
  结果，最终以幂等记录为准，双方拿到一致结果。

## 2. 幂等与重放

- 每个写请求必须携带全局唯一的 `requestId`（建议 UUID）。
- 首次请求的**完整结果（包括失败结果，如 `CONFLICT` / `WAIT_TIMEOUT` / `QUEUE_FULL`）**
  会以 `requestId` 为键持久化，同时记录请求内容的**指纹**（action + 资源 + 持有者 +
  令牌 + TTL + 等待时长 + 写入值）。
- 相同 `requestId` 重复提交时：
  - 内容指纹一致 → 不重新执行业务，直接返回首次结果，`replay=true`，
    其余字段（成功与否、令牌、错误码、原因、判定依据）与首次完全一致；
  - 内容指纹不一致（换了资源、持有者、令牌、写入内容等）→ 拒绝并返回
    `REQUEST_CONFLICT`（409），**不会**把旧结果悄悄还回去。
- **临时性失败不污染幂等记录**：存储抖动等归一化为 `TEMPORARILY_UNAVAILABLE` 的
  故障不会写入幂等记录，恢复后用同一 `requestId` 可以正常重试。
- 注意：`requestId` 应与具体的业务意图一一对应（例如「第 3 次续约」要用新的 requestId）。

## 3. 错误类型划分

对外只暴露归一化后的错误码（响应体 `errorCode` + HTTP 状态码），底层异常（I/O、CAS 冲突等）不会透出：

| errorCode               | HTTP | 含义（可区分的拒绝原因）                                     |
|-------------------------|------|--------------------------------------------------------------|
| `CONFLICT`              | 409  | 申请时资源仍被有效持有者占用（或有排队等待者优先）            |
| `WAIT_TIMEOUT`          | 408  | 等待式申请在预算内未等到释放/到期                            |
| `QUEUE_FULL`            | 429  | 该资源的等待队列已满，请求没有排上队                         |
| `REQUEST_CONFLICT`      | 409  | 同一 `requestId` 携带了与首次不同的请求内容                  |
| `LEASE_EXPIRED`         | 410  | 令牌有效但租约已到期（或已释放），需重新申请                  |
| `FENCED`                | 410  | 提交令牌小于当前令牌：持有者已被更高令牌接管，旧持有者被隔离  |
| `LEASE_UNKNOWN`         | 410  | 资源无记录，或提交的 holder/token 与记录不匹配                |
| `TEMPORARILY_UNAVAILABLE` | 503 | 存储冲突、持久化失败、续约期间底层异常等临时性故障，可安全重试 |
| `INVALID_REQUEST`       | 400  | 缺少必填字段、TTL/waitMillis 非法、JSON 格式错误              |

`LEASE_EXPIRED` 与 `FENCED` 虽然 HTTP 状态同为 410，但 `errorCode` 明确区分：
前者是「你的时间窗过了」，后者是「你已经被更高令牌的主人取代」。

每个响应还包含 `reason`（人类可读原因）与 `decision`（判定依据，例如
`current token 2 > presented 1 -> fenced`），便于审计与排障。

## 4. HTTP API

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/leases/acquire`  | 申请租约（可选 `waitMillis` 等待式申请） |
| POST | `/api/leases/renew`    | 续约（需要 token） |
| POST | `/api/leases/release`  | 主动释放（需要 token），会唤醒等待队列 |
| POST | `/api/leases/write`    | 携带令牌的受保护写入 |
| GET  | `/api/leases/resources/{resourceId}` | 查询资源当前租约与值 |
| POST | `/api/leases/reap`     | 手动触发到期回收 |

请求体字段：`requestId`、`resourceId`、`holderId`、`token`（renew/release/write）、
`ttlMillis`（acquire/renew，可选，默认 10000）、`waitMillis`（acquire，可选，等待式申请）、
`value`（write）。

## 5. 本地验证方法

需要 JDK 21+、Maven 3.9+。

### 运行测试

```bash
mvn test
```

覆盖场景（测试日志会打印 `requestId`、`token`、`currentToken`、`errorCode` 与 `decision`）：

- `concurrentAcquireOnlyOneWinnerAndMonotonicToken`：16 个线程并发申请同一资源，恰好 1 个成功、
  其余全部 `CONFLICT`，最终令牌唯一且单调；
- `expiredLeaseCanBeTakenOverWithHigherToken`：租约到期后续约被拒（`LEASE_EXPIRED`），
  新持有者接管后令牌 +1；
- `staleHolderRenewAndWriteAreRejectedWithDistinctReasons`：旧持有者过期写入 → `LEASE_EXPIRED`；
  被接管后续约/写入/释放 → `FENCED`；未知资源 → `LEASE_UNKNOWN`；
- `idempotentReplayReturnsIdenticalResult`：成功与失败结果的重放一致性；
- `storageFaultIsNormalizedToTemporarilyUnavailable`：注入存储故障，验证归一化为 503；
- `LeaseWaitTest`（等待式申请，真实时钟、短等待）：
  - `waitersAreGrantedInFifoOrderWithMonotonicTokens`：3 个等待者并发排队，依次释放后
    按 FIFO 顺序拿到令牌 2/3/4，其余时刻仍阻塞在队列中；
  - `waitTimeoutIsDistinctReplayableAndDoesNotSteal`：`WAIT_TIMEOUT`（408）判定、同
    `requestId` 重放一致、同 `requestId` 换持有者 → `REQUEST_CONFLICT`（409）、
    超时方之后抢不到租约；
  - `queueFullIsDistinctAndHeadIsWokenOnRelease`：队列容量 2 满员后第 3 个等待者立即
    `QUEUE_FULL`（429）且可重放，释放后队首被唤醒；
  - `waiterIsWokenOnExpiryWithAdvancedToken`：持有者到期后等待者被唤醒且令牌 +1；
  - `waitingAcquireReplayReturnsSameGrant`：等待成功后的幂等重放不会重复获取或推高令牌；
  - `storageFaultDuringWaitIsTransientAndNotRecorded`：等待期间存储抖动归一化为 503、
    不写入幂等记录、恢复后同 `requestId` 可重试；
  - `immediateAcquireSemanticsUnchanged`：不带 `waitMillis` 的老调用仍是即时 `CONFLICT`；
- `LeaseApiTests`：通过 MockMvc 验证完整 HTTP 链路与状态码（含 `REQUEST_CONFLICT`）。

### 启动服务并手动验证

```bash
mvn spring-boot:run
# 若 8080 被占用:
# mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8088

# 1. A 申请（得到 token=1）
curl -s -X POST localhost:8080/api/leases/acquire -H 'Content-Type: application/json' \
  -d '{"requestId":"demo-1","resourceId":"order-1","holderId":"worker-A","ttlMillis":2000}'

# 2. B 同时申请 -> 409 CONFLICT
curl -i -X POST localhost:8080/api/leases/acquire -H 'Content-Type: application/json' \
  -d '{"requestId":"demo-2","resourceId":"order-1","holderId":"worker-B","ttlMillis":2000}'

# 3. 相同 requestId 重放 -> replay=true，结果与首次一致
curl -s -X POST localhost:8080/api/leases/acquire -H 'Content-Type: application/json' \
  -d '{"requestId":"demo-1","resourceId":"order-1","holderId":"worker-A","ttlMillis":2000}'

# 4. 等待 2 秒后 A 续约 -> 410 LEASE_EXPIRED
# 5. B 重新申请 -> token=2
# 6. A 仍用 token=1 写入 -> 410 FENCED
curl -s -X POST localhost:8080/api/leases/write -H 'Content-Type: application/json' \
  -d '{"requestId":"demo-6","resourceId":"order-1","holderId":"worker-A","token":1,"value":"stale"}'

# 7. 等待式申请（新资源 order-2）：B 持有时 C 带 waitMillis 申请，
#    B 释放后 C 被唤醒并拿到 token=2
curl -s -X POST localhost:8080/api/leases/acquire -H 'Content-Type: application/json' \
  -d '{"requestId":"demo-7","resourceId":"order-2","holderId":"worker-B","ttlMillis":30000}'
curl -s -X POST localhost:8080/api/leases/acquire -H 'Content-Type: application/json' \
  -d '{"requestId":"demo-8","resourceId":"order-2","holderId":"worker-C","ttlMillis":5000,"waitMillis":10000}' &
sleep 1
curl -s -X POST localhost:8080/api/leases/release -H 'Content-Type: application/json' \
  -d '{"requestId":"demo-9","resourceId":"order-2","holderId":"worker-B","token":1}'
wait   # C 的响应：success=true, token=2

# 8. 等待超时：C 持有时 D 只等 500ms -> 408 WAIT_TIMEOUT；同 requestId 重放结果一致
curl -s -X POST localhost:8080/api/leases/acquire -H 'Content-Type: application/json' \
  -d '{"requestId":"demo-10","resourceId":"order-2","holderId":"worker-D","ttlMillis":5000,"waitMillis":500}'

# 9. 同一 requestId 换内容 -> 409 REQUEST_CONFLICT
curl -s -X POST localhost:8080/api/leases/acquire -H 'Content-Type: application/json' \
  -d '{"requestId":"demo-10","resourceId":"order-2","holderId":"worker-E","ttlMillis":5000,"waitMillis":500}'

# 查看状态
curl -s localhost:8080/api/leases/resources/order-2
```

持久化文件默认为 `data/lease-store.json`（启动时自动创建，原子 tmp+move 落盘），
删除该文件即可重置全部状态。

## 6. 配置项（application.properties）

| 键 | 默认值 | 说明 |
|----|--------|------|
| `lease.default-ttl-millis` | 10000 | 默认租约时长 |
| `lease.min-ttl-millis` / `lease.max-ttl-millis` | 1000 / 3600000 | TTL 夹取范围 |
| `lease.max-wait-millis` | 30000 | `waitMillis` 的上限（超出会被夹取） |
| `lease.max-wait-queue-size` | 64 | 每个资源的等待队列容量，满员返回 `QUEUE_FULL` |
| `lease.store-file` | `data/lease-store.json` | 本地持久化文件路径 |
| `lease.reap-interval-millis` | 2000 | 后台回收轮询间隔 |
| `lease.reap-enabled` | true | 是否启用后台回收 |

## 7. 实现说明与边界

- 本实现的「共享存储」是**单 JVM 内加锁 + 本地文件**，用于演示 fencing token 与幂等语义；
  生产环境的多进程部署应将 `LeaseStore` 替换为带条件写入（CAS）的真实存储（如关系库乐观锁、
  etcd 事务等），服务层判定逻辑无需改动。
- 等待队列（`LeaseWaitQueue`）是**进程内**结构：授权判定与令牌推导由存储层
  `claimLeaseIfFree` 在同一临界区完成，因此等待语义的安全性与队列驻留位置无关；
  但多副本部署时每个副本只看到自己的等待者，FIFO 公平性退化为「每副本内公平」。
- 时钟来源为可替换的 `SystemMillisClock`（测试中使用可控时钟模拟到期；等待式申请的
  测试使用真实时钟配合短等待）。
