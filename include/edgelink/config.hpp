#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace edgelink {

// 单个采集点的静态定义。address 是设备寄存器地址，scale 在原始值
// 完成有符号/无符号解释后参与换算。
struct PointConfig {
  std::string device_id;
  std::string key;
  std::uint16_t address{0};
  bool is_signed{false};
  double scale{1.0};
  std::string unit;
};

// 一台 Modbus TCP 设备的连接、轮询和点位配置。
struct DeviceConfig {
  std::string id;
  std::string host;
  std::uint16_t port{502};
  std::uint8_t unit_id{1};
  int poll_ms{1000};
  int timeout_ms{1000};
  std::vector<PointConfig> points;
};

// 网关进程的完整配置。默认值同时作为可选配置项缺省时的行为契约。
struct Config {
  std::string gateway_id;
  std::string database_path;
  std::string socket_path;
  std::string mqtt_host{"127.0.0.1"};
  std::uint16_t mqtt_port{1883};
  int mqtt_ack_timeout_ms{5000};
  std::size_t mqtt_window{32};
  std::size_t event_queue_capacity{1024};
  // 0 表示由加载器按队列容量推导默认高/低水位。
  std::size_t event_queue_high_watermark{0};
  std::size_t event_queue_low_watermark{0};
  std::size_t control_queue_capacity{128};
  std::uint64_t storage_budget_bytes{1024ULL * 1024ULL * 1024ULL};
  std::uint64_t storage_reserve_bytes{16ULL * 1024ULL * 1024ULL};
  std::int64_t history_retention_ms{24LL * 60LL * 60LL * 1000LL};
  std::vector<DeviceConfig> devices;
};

// 读取严格的 key=value 配置，并在返回前完成关联和全量校验。
Config load_config(const std::string& path);

// 校验调用方构造的配置；失败时抛出 std::runtime_error。
void validate_config(const Config& config);

}  // namespace edgelink
