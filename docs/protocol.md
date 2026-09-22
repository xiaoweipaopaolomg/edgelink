# 协议契约

## Modbus TCP

网关只支持 Read Holding Registers（功能码 `0x03`）。请求和响应使用标准 MBAP 头，不使用串口 CRC。每个连接最多一个未完成请求；响应必须匹配事务号和 Unit ID。响应寄存器为大端 16 位，配置决定按 `uint16` 或 `int16` 解释，再乘以 `scale`。

非法长度、协议 ID、功能码、异常响应、事务不匹配或寄存器数量不符会使该设备连接进入退避重连，不产生测量值。

## MQTT

- 事件主题：`edgelink/<gateway_id>/events`
- ACK 主题：`edgelink/<gateway_id>/acks`
- 事件和 ACK 均使用 QoS 1。
- 单条事件 JSON 上限 8 KiB。

事件唯一键为 `(gateway_id, store_epoch, event_seq)`。接收端必须先原子提交事件与全部测量值，再发布相同唯一键的 ACK。重复事件仍需回复 ACK；网关只删除精确匹配当前 `gateway_id` 和 `store_epoch` 的 outbox 行。

## 管理 IPC

Unix Domain Socket 上的每条请求和响应为 `4 字节大端长度 + UTF-8 JSON`。请求上限 16 KiB，历史单页最多 500 条。当前操作为 `status`、`devices`、`latest`、`history`。

