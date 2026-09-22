# 运维说明

生产环境建议创建不可登录的 `edgelink` 用户，数据库目录仅允许该用户写入，管理 Socket 通过组权限 `0660` 开放。示例 systemd 单元启用了 `NoNewPrivileges`、`ProtectSystem` 和受限写目录。

SQLite 使用 WAL 与 `synchronous=FULL`。备份正在运行的网关时不能只复制 `.db` 文件；应停服务后复制，或后续使用 SQLite Online Backup API。身份元数据保存在数据库中，修改配置里的 `gateway_id` 不会悄悄接管旧数据库。

排障顺序：先看 `edgectl status` 的待发送量、MQTT 状态、`collection_paused` 和 `storage_full`，再看采集/控制队列深度与拒绝计数，然后看 `edgectl devices` 的超时、重连和解析计数，最后检查服务日志、端口连通性和数据库所在文件系统。

网关会在容量紧张时自动 checkpoint 并清理已确认历史，但不会删除待业务 ACK 的 outbox 数据。若 Broker 或接收端长期离线，未确认记录最终可能占满预算，此时 `storage_full=true` 且采集暂停是预期的保护行为。应恢复上报链路或扩容存储，不要直接删除数据库/WAL。恢复后 ACK 会释放 outbox，容量状态和采集会自动恢复。容量预算是保护上限而非归档策略，生产环境仍应监控数据库、WAL 与文件系统占用。
