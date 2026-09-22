# 架构与恢复边界

主线程拥有设备 Socket、管理 Socket、`epoll`、`timerfd` 和设备状态。存储线程独占 SQLite 连接；采集任务使用容量为 `event_queue_capacity` 的有界队列，ACK/查询使用容量为 `control_queue_capacity` 的独立优先控制队列。MQTT 线程只通过存储接口读取 outbox 和提交 ACK。

采集侧采用高低水位滞回：事件队列达到 `event_queue_high_watermark` 后不再发起新轮询，直到队列降至 `event_queue_low_watermark` 才恢复。已在途的设备响应仍可完成，存储、ACK、MQTT 重连和管理查询不随采集暂停。这样既限制积压，也避免在临界值附近频繁启停。

数据在 SQLite 事务提交后才属于可靠接收范围。事务同时插入 `events`、`readings` 和 `outbox`。MQTT PUBACK 仅表示 Broker 收包，不删除 outbox；只有接收端在自身事务提交后返回的业务 ACK 才会触发删除。

因此：本地提交后发布前崩溃会在重启后补传；接收端提交后 ACK 丢失会导致重复传输，但接收端唯一键保持一份；网关收到 ACK 后、删除事务生效前崩溃也只会产生可去重的重复。

当前 `SIGTERM` 顺序是停止采集循环、停止 MQTT、冲刷已经进入存储事件队列的记录，再关闭 SQLite。`SIGKILL` 不承诺保存尚未提交的内存事件。

## 存储容量保护

存储线程以 SQLite 已用页面（扣除 freelist）加 WAL 文件大小估算占用，并同时检查文件系统可用空间。达到 `storage_budget_bytes` 或低于 `storage_reserve_bytes` 时，会执行 WAL checkpoint，并分批清理不在 outbox 中的已确认事件：先清理早于 `history_retention_ms` 的记录，仍不足时再从最旧的已确认记录开始清理。

待业务 ACK 的事件不会被容量清理。若仅靠已确认历史无法释放足够空间，存储进入 `storage_full` 状态并拒绝新的采集批次，主循环暂停采集；ACK 继续处理。空间恢复到安全范围后状态自动解除并恢复轮询。`edgectl status` 暴露队列深度/拒绝计数、`collection_paused`、`storage_full`、当前估算占用和预算。
