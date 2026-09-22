# EdgeLink

EdgeLink 是一个运行在 Linux 上的工业设备数据采集与可靠上报网关，使用 C/C++ 实现。项目以 Modbus TCP 设备为数据源，通过 `epoll` 驱动多设备非阻塞轮询，将采集事件原子写入 SQLite，再经 MQTT 和业务 ACK 完成可恢复的远程上报。它是一个用于实践 Linux 系统编程、网络协议、并发解耦和可靠消息设计的个人项目。

**状态：** 核心功能完整。普通 Debug 与 ASan/UBSan 构建均为 4/4 测试通过。已覆盖 TCP 分段、异常报文、设备断链、Broker 中断、ACK 丢失、重复消息、进程崩溃恢复和存储容量限制。当前定位是可重复演示和验证的工程原型，不是可直接替代商业工业网关的生产产品。

---

## 架构

```text
 Modbus TCP 设备                         EdgeLink 网关
┌──────────────┐       ┌──────────────────────────────────────────────────┐
│ meter-01     │◄─────►│ 主线程                                           │
├──────────────┤       │                                                  │
│ meter-02 ... │◄─────►│ epoll + timerfd + signalfd                       │
└──────────────┘       │   │                                              │
                       │   ├─ 每设备独立状态机                             │
                       │   │  CONNECTING / ONLINE / BACKOFF               │
                       │   │                                              │
 edgectl               │   └─ Modbus 响应 ──► 有界采集队列 ───────┐       │
┌──────────────┐       │                                           │       │
│ status       │       │ Unix Domain Socket                        ▼       │
│ devices      │◄─────►│ 管理请求 ───────────► 优先控制队列 ──► 存储线程  │
│ latest       │       │                                      SQLite/WAL  │
│ history      │       │                                  events/readings │
│ export       │       │                                       + outbox   │
└──────────────┘       │                                           ▲       │
                       │                                           │ ACK   │
                       │ MQTT 线程 ◄──── 读取 outbox / 提交 ACK ───┘       │
                       └───────┬───────────────────────────▲───────────────┘
                               │ QoS 1 事件                │ 业务 ACK
                               ▼                           │
                        ┌─────────────┐              ┌──────┴──────┐
                        │ MQTT Broker │◄────────────►│ 幂等接收端  │
                        └─────────────┘              │ SQLite      │
                                                     └─────────────┘
```

主线程独占设备 Socket、管理 Socket 和设备状态机，不执行 SQLite 或 MQTT 阻塞操作。存储线程独占 SQLite 连接，通过两个有界队列接收任务；ACK 和查询使用优先控制队列，避免采集积压阻塞补传确认。MQTT 线程只通过存储接口读取 outbox 和提交业务 ACK，不跨线程共享数据库连接。

采集队列达到高水位后，主线程暂停发起新轮询；队列回落到低水位后再恢复。已在途响应、设备重连、MQTT 补传、ACK 和管理查询不会随采集暂停，因而存储变慢时不会把压力继续传导到设备通信。

### 事件与恢复生命周期

```text
Modbus 响应
    │
    ▼
内存 Event ──► 有界队列 ──► SQLite 单事务
                              ├─ events
                              ├─ readings
                              └─ outbox
                                  │
                    本地提交完成后才进入可靠范围
                                  │
                                  ▼
                         MQTT QoS 1 PUBLISH
                                  │
                                  ▼
                      接收端事务 + 唯一键幂等
                                  │
                                  ▼
                             业务 ACK
                                  │
                                  ▼
                         网关删除 outbox 行
```

MQTT PUBACK 只表示 Broker 收到报文，不能证明业务接收端已经落库，因此不会删除 outbox。接收端必须先提交自己的事务，再返回包含 `gateway_id + store_epoch + event_seq` 的业务 ACK。ACK 丢失会造成重复传输，但接收端唯一键保证事件只保存一份，并会对重复事件再次确认。

---

## 构建与运行

需要 Linux、CMake 3.16+、支持 C++17 的 GCC/Clang、pthread、SQLite3 开发文件和 Python 3。Ubuntu 可安装：

```bash
sudo apt install build-essential cmake ninja-build libsqlite3-dev python3
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON
cmake --build build
ctest --test-dir build --output-on-failure
```

也可以使用仓库提供的 Conda 环境隔离编译器、sysroot 和开发依赖：

```bash
conda env create -f environment.yml
conda activate edgelink-dev
cmake -S . -B build-conda -G Ninja -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON
cmake --build build-conda
ctest --test-dir build-conda --output-on-failure
```

### 五分钟演示

以下示例假设可执行文件位于 `build/`。在仓库根目录打开四个终端：

```bash
# 终端 1：仅用于本机测试的最小 Broker
python3 tools/broker/minibroker.py

# 终端 2：Modbus TCP 设备模拟器，响应会被拆成两个 TCP 写入
python3 tools/device_sim/modbus_sim.py --split

# 终端 3：幂等业务接收端
python3 tools/receiver/receiver.py \
  --gateway-id gw-001 \
  --database var/receiver.db

# 终端 4：网关
./build/edgelinkd --config config/edgelink.conf
```

查询运行状态和本地数据：

```bash
./build/edgectl status
./build/edgectl devices
./build/edgectl latest
./build/edgectl history meter-01.voltage
./build/edgectl export meter-01.voltage var/voltage.csv
```

停止接收端后，`status` 中的 `pending_events` 会持续增长；重新启动接收端后，积压应补传并回落到 0。数据库中已经提交的 outbox 事件在 `edgelinkd` 被强制结束并重启后仍会继续上报。

### 模块布局

| 文件或目录 | 职责 |
|---|---|
| `apps/edgelinkd/main.cpp` | 进程入口、epoll 循环、设备状态机、管理 Socket 和采集反压 |
| `apps/edgectl/main.cpp` | 状态、设备、最新值、历史和 CSV 导出命令行工具 |
| `src/protocol/modbus.c` | C17 Modbus TCP `0x03` 编解码、MBAP 长度和事务匹配 |
| `src/storage/storage.cpp` | SQLite 单线程所有权、事务写入、outbox、查询和容量清理 |
| `src/uplink/mqtt_client.cpp` | 最小 MQTT 3.1.1 客户端、发送窗口、ACK 和退避重连 |
| `src/common/config.cpp` | 严格 `key=value` 配置解析、关联和边界校验 |
| `tools/device_sim/` | Modbus TCP 模拟及分段、超时、断链、异常帧注入 |
| `tools/receiver/` | 接收端幂等入库、业务 ACK 和 ACK 丢失注入 |
| `tools/broker/` | 自动化测试使用的最小本地 MQTT Broker |
| `tests/` | Modbus 单元测试与端到端故障恢复测试 |
| `deploy/edgelink.service` | 最小权限 systemd 服务示例 |

---

## 配置

配置文件采用严格的 `key=value` 格式。未知键、重复 ID 和越界值会使启动失败，而不是静默使用默认值。设备和点位记录可以重复出现：

```text
device=id,host,port,unit_id,poll_ms,timeout_ms
point=device_id,point_key,address,uint16|int16,scale,unit
```

同一设备的点位会合并为一次连续寄存器读取，因此地址跨度不能超过 Modbus `0x03` 的 125 个寄存器上限。`point_key` 在整个网关中必须唯一。

```bash
./build/edgectl config-check config/edgelink.conf
```

队列和存储保护的主要参数：

```text
event_queue_capacity=1024
event_queue_high_watermark=768
event_queue_low_watermark=512
control_queue_capacity=128
storage_budget_bytes=1073741824
storage_reserve_bytes=16777216
history_retention_ms=86400000
```

存储占用达到预算或文件系统可用空间低于保留值时，网关先 checkpoint WAL，再清理超过保留期的已确认历史；仍不足时从最旧的已确认记录继续清理。outbox 中尚未收到业务 ACK 的事件永远不会被容量策略删除。如果只有未确认事件可用，网关会进入 `storage_full` 状态并暂停采集，待 ACK 释放空间后自动恢复。

---

## 验证结果

### 自动化覆盖

| 测试 | 覆盖内容 | 结果 |
|---|---|---|
| `modbus_unit` | 请求编码、TCP 分段、异常响应、事务不匹配、协议号/长度/字节数错误、125 寄存器边界 | 通过 |
| `config_check` | 示例配置结构、设备/点位关联、唯一 ID、类型和寄存器跨度 | 通过 |
| `integration_smoke` | 双设备采集、设备故障隔离、异常帧、QoS 1、ACK 丢失、幂等入库、Broker 中断、`SIGKILL` 恢复和补传 | 通过 |
| `integration_capacity` | 小容量预算、采集暂停、未确认事件保护、ACK 后空间恢复和自动续采 | 通过 |

2026-09-21 在独立 Conda 环境中完成验证：Python 3.11.16、GCC/G++ 15.2.0、GLIBC 2.17 sysroot、CMake 4.4.3、Ninja 1.13.2、SQLite 3.53.4。普通 Debug 构建 4/4 通过，单次全量测试约 7 秒；ASan/UBSan 构建 4/4 通过，单次约 11 秒。时间会随机器负载变化。

这些结果证明故障路径可以自动、重复地执行，并不等价于 24 小时稳定性或目标硬件性能结论。本项目没有填写未经测量的吞吐、P95 延迟、CPU 或 RSS 数字。

### 复现测试

```bash
# 普通 Debug
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON
cmake --build build
ctest --test-dir build --output-on-failure

# AddressSanitizer + UndefinedBehaviorSanitizer
cmake -S . -B build-asan -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug \
  -DBUILD_TESTING=ON \
  -DEDGELINK_ENABLE_SANITIZERS=ON
cmake --build build-asan
ctest --test-dir build-asan --output-on-failure
```

---

## 设计取舍

**主线程集中管理 epoll 和设备状态。** 每台设备维护独立的连接、事务号、超时和退避状态。一台设备断链只会推动自己的状态机，不会阻塞其他设备或管理接口。`timerfd` 用于轮询调度和超时检测，`signalfd` 将 SIGINT/SIGTERM 转换为普通 fd 事件，避免在异步信号处理函数中操作复杂对象。

**协议解析使用独立 C17 模块。** Modbus TCP 是边界清晰的字节协议，C 接口便于单独测试，也能直接表达缓冲区、长度和返回状态。解析器不假设一次 `recv` 得到完整帧，并检查 MBAP 协议号、声明长度、事务号、Unit ID、功能码和寄存器数量。

**SQLite 连接只属于存储线程。** 设备 I/O、MQTT 和管理命令不会跨线程直接使用连接，避免依赖 SQLite 的隐式串行化。采集任务与控制任务使用两个有界队列，控制任务优先处理；最多 50 个采集事件合并为一个事务，以减少同步写成本，同时限制管理查询的等待时间。

**事务型 outbox 而不是“先存后发”两个独立步骤。** `events`、`readings` 和 `outbox` 在同一事务中提交，消除了“采集记录已保存但忘记创建待发送记录”的窗口。进程在提交后、发布前崩溃时，重启会重新读取 outbox。

**业务 ACK 位于 MQTT QoS 1 之上。** Broker 的 PUBACK 只证明消息到达 Broker。只有接收端完成幂等事务后返回的 ACK 才能证明业务数据进入目标数据库。代价是 ACK 丢失时会重复传输，因此接收端必须以持久化唯一键去重。

**高低水位反压而不是无界缓存。** 达到高水位时暂停新轮询，降至低水位才恢复，滞回区间避免在临界点反复启停。容量耗尽时也使用同一暂停机制，但 ACK、补传和连接维护继续运行，从而保留自恢复路径。

**容量清理以 outbox 为安全边界。** 存储占用按 SQLite 有效页面加 WAL 估算。清理查询只选择不在 outbox 中的事件，因此不会为了保持磁盘预算而牺牲尚未确认的数据。无法安全释放空间时，系统选择停止接收新数据并暴露状态，而不是静默丢弃旧积压。

**管理协议使用 Unix Domain Socket 和长度前缀。** 本机控制面不额外开放 TCP 端口。四字节大端长度前缀允许处理流式 Socket 分段，并为请求和分页设置明确上限。

---

## 已知限制

这些是当前项目的范围边界，不是隐藏的已完成功能。

- **仅支持 Modbus TCP `0x03`。** 没有实现 Modbus RTU、串口、写寄存器和其他功能码。
- **MQTT 客户端是窄协议实现。** 支持本项目需要的 MQTT 3.1.1 QoS 1、订阅、心跳和重连，但没有 TLS、账号密码、ACL、持久会话和完整协议兼容性。生产部署应替换为成熟客户端库。
- **测试 Broker 不是生产 Broker。** `tools/broker/minibroker.py` 只实现自动化用例需要的报文类型，不提供持久化、安全认证、集群或完整 MQTT 语义。
- **数据库只有版本拒绝，没有在线迁移。** 当前会拒绝未知 `db_version`，但尚未提供逐版本 schema migration 和在线备份命令。
- **没有目标硬件结论。** 尚未完成 ARM64 实机部署、存储介质耐久性验证和断电测试；x86_64 上的结果不能替代目标设备验收。
- **没有长稳与吞吐报告。** 尚未完成 24 小时稳定性、1000 点/秒、P95 延迟、CPU、RSS 和磁盘写放大测试。
- **JSON 解析范围受控。** MQTT ACK 和管理请求由项目自身生成，因此使用受限字段提取器，不是通用 JSON 解析器。
- **SIGKILL 仍有内存窗口。** 已经提交 SQLite 的数据可以恢复；尚未进入存储队列或尚未提交事务的内存事件无法承诺保留。

---

## 后续计划

如果继续把 EdgeLink 向生产网关推进，优先级如下：

1. **性能与长稳基线。** 增加多设备负载生成器，记录吞吐、P50/P95/P99、CPU、RSS、数据库增长和 24 小时错误计数。
2. **TLS 与认证。** 接入成熟 MQTT 库或 OpenSSL，增加证书校验、账号、ACL 和密钥部署流程。
3. **数据库迁移与备份。** 建立逐版本迁移、失败回滚和 SQLite Online Backup API 管理命令。
4. **ARM64 验证。** 增加交叉编译和 QEMU 冒烟测试，并在真实开发板上验证网络、文件系统和 systemd 部署。
5. **可观测性。** 增加结构化日志、Prometheus 指标和队列/补传/容量告警。

这些是明确延期的增强项，不应在完成并取得测试证据前写入已实现能力。

---

## 项目定位

这是一个学习和展示 Linux 系统编程能力的个人项目。它不与成熟工业网关、Mosquitto 或商业采集平台竞争；这些产品已经具备更完整的协议、安全、硬件适配和运维生态。

EdgeLink 的价值在于把几个容易只停留在概念层的主题做成了可运行闭环：`epoll` 非阻塞设备状态机、有界队列反压、SQLite 事务型 outbox、MQTT 业务 ACK、幂等接收、崩溃恢复和故障注入。如果你正在从 Linux/C++、嵌入式或基础设施岗位的角度阅读这个项目，最值得关注的是上面的设计取舍、恢复边界和自动化故障验证，而不只是功能列表。

更多细节见 [`docs/architecture.md`](docs/architecture.md)、[`docs/protocol.md`](docs/protocol.md)、[`docs/operations.md`](docs/operations.md) 和 [`docs/test-report.md`](docs/test-report.md)。
