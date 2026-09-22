# 测试报告

## 自动化覆盖

- `modbus_unit`：请求编码、分段输入、正常响应、事务不匹配、异常响应、协议号/MBAP 长度/字节数异常和寄存器数量边界。
- `config_check`：示例配置的结构、唯一 ID、点位类型和寄存器跨度验证。
- `integration_smoke`：两个真实回环 TCP 设备的分段采集与隔离、异常长度帧、设备断链、SQLite 落库、QoS 1 上报、业务 ACK 丢失后的重复投递与接收端幂等、Broker 停机积压、带积压的网关 `SIGKILL`/重启恢复、补传清空和接收端 `integrity_check`。
- `integration_capacity`：小容量预算下触发 `storage_full` 与采集暂停，验证未确认记录不会被淘汰；恢复接收端后验证积压清空、容量状态解除和自动续采。

运行命令：

```bash
ctest --test-dir build --output-on-failure
```

本仓库不伪造 24 小时稳定性、P95 延迟、RSS 或压力吞吐数字。正式发布前应在目标机器上记录 CPU、内存、存储介质、编译类型和依赖版本，再执行设计说明书中的长稳与性能验收。

## 本次实现验证

2026-09-21 在 Linux x86_64、GCC 11.4.0、CMake 3.16+、SQLite 3.53 环境完成了初始版本的普通 Debug 与 AddressSanitizer + UndefinedBehaviorSanitizer 验证。端到端用例只证明上述故障路径可重复运行，不替代长稳和性能测试。

同日使用仓库 `environment.yml` 创建独立 Conda 环境 `edgelink-dev`，环境采用纯 `conda-forge`：Python 3.11.16、GCC/G++ 15.2.0、GLIBC 2.17 sysroot、CMake 4.4.3、Ninja 1.13.2、SQLite 3.53.4、pytest 9.1.1。曾发现 flexible channel priority 会混装 defaults GCC 与 conda-forge sysroot；最终通过 `nodefaults` 和显式 2.17 sysroot 消除该冲突。

补齐队列反压、容量保护和故障用例后，普通 Debug 构建 4/4 通过（6.71 秒），ASan/UBSan 构建 4/4 通过（10.72 秒）。其中 `integration_smoke` 约 3 秒，`integration_capacity` 在普通构建约 4 秒；时间会随机器负载变化。
