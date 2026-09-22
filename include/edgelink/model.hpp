#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace edgelink {

// 一个工程量测点。value 已完成寄存器类型解释和比例换算。
struct Measurement {
  std::string point_key;
  double value{0};
  std::string unit;
  std::string quality{"good"};
};

// 一次设备轮询形成一个事件，同次读取的全部点位共享事件序号和时间戳。
struct Event {
  std::int64_t event_seq{0};
  std::string device_id;
  std::int64_t received_at_ms{0};
  std::vector<Measurement> measurements;
};

// 对外拼接 JSON 时必须先转义用户可配置的字符串字段。
std::string json_escape(const std::string& input);

// 将持久化事件编码为上报协议 JSON。event_seq 应已由 SQLite 分配。
std::string event_to_json(const Event& event, const std::string& gateway_id,
                          const std::string& store_epoch);

}  // namespace edgelink
