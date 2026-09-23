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

## 2. 幂等与重放

- 每个写请求必须携带全局唯一的 `requestId`（建议 UUID）。
- 首次请求的**完整结果（包括失败结果）**会以 `requestId` 为键持久化。
- 相同 `requestId` 重复提交时：
  - 不重新执行业务，直接返回首次结果；
  - 响应中 `replay=true`，其余字段（成功与否、令牌、错误码、原因、判定依据）与首次完全一致。
- 注意：`requestId` 应与具体的业务意图一一对应（例如「第 3 次续约」要用新的 requestId）。

## 3. 错误类型划分

对外只暴露归一化后的错误码（响应体 `errorCode` + HTTP 状态码），底层异常（I/O、CAS 冲突等）不会透出：

| errorCode               | HTTP | 含义（可区分的拒绝原因）                                     |
|-------------------------|------|--------------------------------------------------------------|
| `CONFLICT`              | 409  | 申请时资源仍被有效持有者占用                                  |
| `LEASE_EXPIRED`         | 410  | 令牌有效但租约已到期（或已释放），需重新申请                  |
| `FENCED`                | 410  | 提交令牌小于当前令牌：持有者已被更高令牌接管，旧持有者被隔离  |
| `LEASE_UNKNOWN`         | 410  | 资源无记录，或提交的 holder/token 与记录不匹配                |
| `TEMPORARILY_UNAVAILABLE` | 503 | 存储冲突、持久化失败、续约期间底层异常等临时性故障，可安全重试 |
| `INVALID_REQUEST`       | 400  | 缺少必填字段、TTL 非法、JSON 格式错误                         |

`LEASE_EXPIRED` 与 `FENCED` 虽然 HTTP 状态同为 410，但 `errorCode` 明确区分：
前者是「你的时间窗过了」，后者是「你已经被更高令牌的主人取代」。

每个响应还包含 `reason`（人类可读原因）与 `decision`（判定依据，例如
`current token 2 > presented 1 -> fenced`），便于审计与排障。

## 4. HTTP API

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/leases/acquire`  | 申请租约 |
| POST | `/api/leases/renew`    | 续约（需要 token） |
| POST | `/api/leases/release`  | 主动释放（需要 token） |
| POST | `/api/leases/write`    | 携带令牌的受保护写入 |
| GET  | `/api/leases/resources/{resourceId}` | 查询资源当前租约与值 |
| POST | `/api/leases/reap`     | 手动触发到期回收 |

请求体字段：`requestId`、`resourceId`、`holderId`、`token`（renew/release/write）、
`ttlMillis`（acquire/renew，可选，默认 10000）、`value`（write）。

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
- `LeaseApiTests`：通过 MockMvc 验证完整 HTTP 链路与状态码。

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

# 查看状态
curl -s localhost:8080/api/leases/resources/order-1
```

持久化文件默认为 `data/lease-store.json`（启动时自动创建，原子 tmp+move 落盘），
删除该文件即可重置全部状态。

## 6. 配置项（application.properties）

| 键 | 默认值 | 说明 |
|----|--------|------|
| `lease.default-ttl-millis` | 10000 | 默认租约时长 |
| `lease.min-ttl-millis` / `lease.max-ttl-millis` | 1000 / 3600000 | TTL 夹取范围 |
| `lease.store-file` | `data/lease-store.json` | 本地持久化文件路径 |
| `lease.reap-interval-millis` | 2000 | 后台回收轮询间隔 |
| `lease.reap-enabled` | true | 是否启用后台回收 |

## 7. 实现说明与边界

- 本实现的「共享存储」是**单 JVM 内加锁 + 本地文件**，用于演示 fencing token 与幂等语义；
  生产环境的多进程部署应将 `LeaseStore` 替换为带条件写入（CAS）的真实存储（如关系库乐观锁、
  etcd 事务等），服务层判定逻辑无需改动。
- 时钟来源为可替换的 `SystemMillisClock`（测试中使用可控时钟模拟到期）。
