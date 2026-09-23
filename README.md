# Fencing Lease Coordinator（租约协调器）

一个可本地运行的租约协调服务（Spring Boot 3 / Java 21）。用**本地文件持久化**模拟共享状态存储，
保证多个并发消费者对同一资源的操作结果一致且可解释。

## 1. 租约（Lease）语义

每个资源（`resourceId`）在任意时刻至多有一个**持有者**（`holderId`），租约有 TTL，
状态在 `ACTIVE / EXPIRED / RELEASED` 间流转。

支持四个操作（均为 `POST`，均需携带 `Idempotency-Key` 请求头）：

| 接口 | 说明 |
| --- | --- |
| `/api/leases/acquire` | 申请租约；资源被其他活跃持有者占用时拒绝；过期或已释放的租约可被接管 |
| `/api/leases/release` | 当前持有者主动释放 |
| `/api/leases/renew` | 当前持有者凭当前令牌续约，延长 `expiresAt` |
| `/api/leases/write` | 携带 fencing token 的资源写入，旧/同令牌一律拒绝 |

到期回收：后台任务（`lease.reap-interval-millis`，默认 2s）扫描并把过期租约置为 `EXPIRED`；
所有读路径也会做惰性过期判定，即使回收任务尚未运行，过期租约也不会被接受。

## 2. 令牌（Fencing Token）语义

- 令牌是每个资源上的 `long`，首次申请为 `1`，**每次接管 / 重新申请都在持久化的旧令牌上加 1**，
  随时间严格单调递增。
- 令牌来源于持久化记录本身（`当前 token + 1`），并通过 **资源锁 + 存储 CAS** 落盘，
  因此并发申请时不会出现两个持有者拿到相同或回退的令牌。
- 旧持有者即使网络恢复后继续使用历史令牌：
  - 续约 → `LEASE_PREEMPTED`（410）
  - 写入 → `TOKEN_SUPERSEDED`（410）
  - 释放 → `LEASE_PREEMPTED`（410）
- 写入另有第二道防线：`writes/<resource>.log` 台账记录每个资源最近一次被接受写入的令牌，
  **相同令牌只能写入成功一次**，相同或更旧令牌再次写入必被拒绝。

成功响应示例：

```json
{ "accepted": true, "reason": "ACCEPTED", "resourceId": "doc", "holderId": "A",
  "token": 1, "expiresAt": 1790150044650, "requestId": "s2" }
```

## 3. 幂等与重放

- 每个写请求必须带 `Idempotency-Key`（即请求标识）。首次结果（**包括各种业务拒绝**）
  会原子落盘到 `idem/<requestId>.json`，记录动作类型与请求指纹（SHA-256）。
- 同一标识重复提交：返回与首次**完全一致的 HTTP 状态与响应体**，并带 `"replayed": true`，
  不会重复执行（不会重复分配令牌、不会重复写台账）。
- 同一标识被用于**语义不同**的请求（动作或参数不同）：返回
  `IDEMPOTENT_REPLAY_MISMATCH`（422）。
- 并发提交同一标识：按请求标识串行化，只有一个请求成为"首次"，其余全部重放首次结果。
- 503 等临时不可用**不占用**幂等结果（实际不落首次记录失败路径下仍可安全重试），
  调用方可用新的或原标识重试。

## 4. 错误类型划分

底层异常（IO、CAS 失败、记录缺失、续约超时等）**不会直接透出**，统一归一化为三类：

| 大类 `category` | 错误码 `errorCode` | HTTP | 含义 |
| --- | --- | --- | --- |
| `CONFLICT` | `RESOURCE_BUSY` | 409 | 资源正被其他活跃持有者持有 |
| `CONFLICT` | `ALREADY_HELD` | 409 | 同一持有者重复申请自己仍持有的资源 |
| `CONFLICT` | `STORAGE_CONFLICT` | 409 | 存储 CAS 冲突（并发抢占落败），可重试 |
| `CONFLICT` | `IDEMPOTENT_REPLAY_MISMATCH` | 422 | 同请求标识对应了不同请求 |
| `INVALIDATION` | `LEASE_EXPIRED` | 410 | 租约已过期，可被接管 |
| `INVALIDATION` | `LEASE_PREEMPTED` | 410 | 旧令牌已被更高令牌接管（旧持有者续约/释放） |
| `INVALIDATION` | `TOKEN_SUPERSEDED` | 410 | 写入令牌已被超越 / 相同令牌重复写 |
| `INVALIDATION` | `HOLDER_MISMATCH` | 422 | 令牌与持有者不匹配 |
| `INVALIDATION` | `UNKNOWN_TOKEN` | 422 | 令牌未知、超前，或租约已释放/不存在 |
| `INVALIDATION` | `RECORD_MISSING` | 410 | 存储中记录缺失，需重新申请 |
| `UNAVAILABLE` | `TEMPORARILY_UNAVAILABLE` | 503 | 续约超时 / 本地存储 IO 故障等临时问题，可重试 |

> 设计要点：**过期**、**未知/已释放**、**被抢占**是三种不同错误码，拒绝原因可区分，
> 不会静默成功，也不会归为同一种失败。

## 5. 本地持久化布局

默认目录由 `lease.data-dir`（`./data/leases`）配置：

```
data/leases/
├── leases/<urlencode(resourceId)>.json   # 租约记录（临时文件 + 原子 move）
├── idem/<urlencode(requestId)>.json      # 幂等首次结果
└── writes/<resourceId>.log               # 写入台账（追加，令牌单调校验）
```

单进程内同一资源的读-改-写由 `ReentrantLock` 串行化，落盘再经令牌 CAS 校验，
模拟共享存储上的乐观并发控制。

## 6. 本地验证

### 构建与测试

```bash
mvn test          # 11 个用例：并发申请 / 到期接管 / 幂等重放 / 异常归一化 / HTTP 端到端
mvn package       # 产出可执行 jar
```

测试日志会打印**请求标识、令牌与判定依据**，例如：

```
[到期接管] requestId=req-r2 holder=A 旧token=1 当前token=2 续约拒绝 reason=LEASE_PREEMPTED 依据=token 1 was superseded by token 2
[并发幂等] requestId=same-idem-key 首次=1 重放=5 令牌集合大小=1
```

### 启动并手工验证

```bash
java -jar target/fencing-lease-coordinator-java-1.0-SNAPSHOT.jar --server.port=8090

# 1) 申请
curl -s -X POST localhost:8090/api/leases/acquire \
  -H 'Idempotency-Key: k1' -H 'Content-Type: application/json' \
  -d '{"resourceId":"doc","holderId":"A","ttlMillis":60000}'
# 2) 同标识重放（replayed=true，token 不变）
curl -s -X POST localhost:8090/api/leases/acquire \
  -H 'Idempotency-Key: k1' -H 'Content-Type: application/json' \
  -d '{"resourceId":"doc","holderId":"A","ttlMillis":60000}'
# 3) 凭令牌写入 / 同令牌再写被拒（TOKEN_SUPERSEDED）
curl -s -X POST localhost:8090/api/leases/write -H 'Idempotency-Key: k2' \
  -H 'Content-Type: application/json' \
  -d '{"resourceId":"doc","holderId":"A","token":1,"payload":"hello"}'
```

### 关键配置（`application.properties`）

| 配置 | 默认 | 说明 |
| --- | --- | --- |
| `lease.default-ttl-millis` | 10000 | 默认租约 TTL |
| `lease.data-dir` | `./data/leases` | 本地持久化目录 |
| `lease.renew-timeout-millis` | 2000 | 续约链路超时，超出归一化为 503 |
| `lease.reap-interval-millis` | 2000 | 到期回收扫描间隔 |
