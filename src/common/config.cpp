#include "edgelink/config.hpp"
#include "edgelink/model.hpp"

#include <algorithm>
#include <fstream>
#include <iomanip>
#include <limits>
#include <set>
#include <sstream>
#include <stdexcept>

namespace edgelink {
namespace {

// 配置文件刻意保持为简单严格的 key=value，避免静默接受拼写错误。
std::string trim(std::string text) {
  const auto first = text.find_first_not_of(" \t\r\n");
  if (first == std::string::npos) return {};
  const auto last = text.find_last_not_of(" \t\r\n");
  return text.substr(first, last - first + 1);
}

std::vector<std::string> split(const std::string& value, char delimiter) {
  std::vector<std::string> result;
  std::stringstream input(value);
  std::string item;
  while (std::getline(input, item, delimiter)) result.push_back(trim(item));
  return result;
}

long parse_long(const std::string& value, const std::string& field) {
  std::size_t used = 0;
  long parsed;
  try { parsed = std::stol(value, &used); }
  catch (...) { throw std::runtime_error("invalid integer for " + field + ": " + value); }
  if (used != value.size()) throw std::runtime_error("invalid integer for " + field + ": " + value);
  return parsed;
}

double parse_double(const std::string& value, const std::string& field) {
  std::size_t used = 0;
  double parsed;
  try { parsed = std::stod(value, &used); }
  catch (...) { throw std::runtime_error("invalid number for " + field + ": " + value); }
  if (used != value.size()) throw std::runtime_error("invalid number for " + field + ": " + value);
  return parsed;
}

}  // namespace

Config load_config(const std::string& path) {
  std::ifstream input(path);
  if (!input) throw std::runtime_error("cannot open config: " + path);
  Config config;
  std::string line;
  std::size_t line_number = 0;
  // 点位可能出现在设备定义之前，先暂存，文件读完后再建立关联。
  std::vector<PointConfig> points;
  while (std::getline(input, line)) {
    ++line_number;
    line = trim(line);
    if (line.empty() || line[0] == '#') continue;
    const auto equal = line.find('=');
    if (equal == std::string::npos)
      throw std::runtime_error("config line " + std::to_string(line_number) + " has no '='");
    const auto key = trim(line.substr(0, equal));
    const auto value = trim(line.substr(equal + 1));
    if (key == "gateway_id") config.gateway_id = value;
    else if (key == "database") config.database_path = value;
    else if (key == "socket") config.socket_path = value;
    else if (key == "mqtt_host") config.mqtt_host = value;
    else if (key == "mqtt_port") config.mqtt_port = static_cast<std::uint16_t>(parse_long(value, key));
    else if (key == "mqtt_ack_timeout_ms") config.mqtt_ack_timeout_ms = static_cast<int>(parse_long(value, key));
    else if (key == "mqtt_window") config.mqtt_window = static_cast<std::size_t>(parse_long(value, key));
    else if (key == "event_queue_capacity") config.event_queue_capacity = static_cast<std::size_t>(parse_long(value, key));
    else if (key == "event_queue_high_watermark") config.event_queue_high_watermark = static_cast<std::size_t>(parse_long(value, key));
    else if (key == "event_queue_low_watermark") config.event_queue_low_watermark = static_cast<std::size_t>(parse_long(value, key));
    else if (key == "control_queue_capacity") config.control_queue_capacity = static_cast<std::size_t>(parse_long(value, key));
    else if (key == "storage_budget_bytes") config.storage_budget_bytes = static_cast<std::uint64_t>(parse_long(value, key));
    else if (key == "storage_reserve_bytes") config.storage_reserve_bytes = static_cast<std::uint64_t>(parse_long(value, key));
    else if (key == "history_retention_ms") config.history_retention_ms = static_cast<std::int64_t>(parse_long(value, key));
    else if (key == "device") {
      const auto f = split(value, ',');
      if (f.size() != 6) throw std::runtime_error("device requires id,host,port,unit,poll_ms,timeout_ms");
      DeviceConfig d;
      d.id = f[0]; d.host = f[1];
      d.port = static_cast<std::uint16_t>(parse_long(f[2], "device.port"));
      d.unit_id = static_cast<std::uint8_t>(parse_long(f[3], "device.unit"));
      d.poll_ms = static_cast<int>(parse_long(f[4], "device.poll_ms"));
      d.timeout_ms = static_cast<int>(parse_long(f[5], "device.timeout_ms"));
      config.devices.push_back(std::move(d));
    } else if (key == "point") {
      const auto f = split(value, ',');
      if (f.size() != 6) throw std::runtime_error("point requires device,key,address,type,scale,unit");
      PointConfig p;
      p.device_id = f[0]; p.key = f[1];
      p.address = static_cast<std::uint16_t>(parse_long(f[2], "point.address"));
      if (f[3] == "int16") p.is_signed = true;
      else if (f[3] == "uint16") p.is_signed = false;
      else throw std::runtime_error("point type must be int16 or uint16");
      p.scale = parse_double(f[4], "point.scale"); p.unit = f[5];
      points.push_back(std::move(p));
    } else {
      // 未知键通常意味着配置拼写错误，直接拒绝比使用默认值更安全。
      throw std::runtime_error("unknown config key: " + key);
    }
  }
  // 将点位归属到设备，同时拒绝悬空的 device_id 引用。
  for (auto& point : points) {
    auto it = std::find_if(config.devices.begin(), config.devices.end(),
                           [&](const DeviceConfig& d) { return d.id == point.device_id; });
    if (it == config.devices.end()) throw std::runtime_error("point references unknown device: " + point.device_id);
    it->points.push_back(std::move(point));
  }
  // 未显式设置水位时按容量推导，保持低水位 < 高水位形成滞回区间。
  if (config.event_queue_high_watermark == 0)
    config.event_queue_high_watermark = std::max<std::size_t>(1, config.event_queue_capacity * 3 / 4);
  if (config.event_queue_low_watermark == 0)
    config.event_queue_low_watermark = config.event_queue_capacity / 2;
  validate_config(config);
  return config;
}

void validate_config(const Config& config) {
  // 所有边界在启动阶段一次性检查，运行线程不再处理不合法配置。
  if (config.gateway_id.empty()) throw std::runtime_error("gateway_id is required");
  if (config.database_path.empty()) throw std::runtime_error("database is required");
  if (config.socket_path.empty()) throw std::runtime_error("socket is required");
  if (config.devices.empty()) throw std::runtime_error("at least one device is required");
  if (config.mqtt_port == 0 || config.mqtt_window == 0 || config.mqtt_window > 1024)
    throw std::runtime_error("invalid MQTT port/window");
  if (config.event_queue_capacity == 0 || config.event_queue_capacity > 100000)
    throw std::runtime_error("invalid event_queue_capacity");
  if (config.event_queue_high_watermark > config.event_queue_capacity ||
      config.event_queue_low_watermark >= config.event_queue_high_watermark)
    throw std::runtime_error("queue watermarks must satisfy low < high <= capacity");
  if (config.control_queue_capacity == 0 || config.control_queue_capacity > 100000)
    throw std::runtime_error("invalid control_queue_capacity");
  if (config.storage_budget_bytes < 64 * 1024 ||
      config.storage_reserve_bytes > config.storage_budget_bytes ||
      config.history_retention_ms < 0)
    throw std::runtime_error("invalid storage capacity settings");
  std::set<std::string> devices, points;
  for (const auto& d : config.devices) {
    if (d.id.empty() || d.host.empty() || d.port == 0 || d.poll_ms < 50 || d.timeout_ms < 50 || d.points.empty())
      throw std::runtime_error("invalid device: " + d.id);
    if (!devices.insert(d.id).second) throw std::runtime_error("duplicate device: " + d.id);
    // 当前实现把同一设备的点位合并为一次连续 0x03 请求，因此跨度受 125 限制。
    std::uint32_t min_addr = std::numeric_limits<std::uint16_t>::max(), max_addr = 0;
    for (const auto& p : d.points) {
      if (p.key.empty() || !points.insert(p.key).second)
        throw std::runtime_error("empty or duplicate point: " + p.key);
      min_addr = std::min<std::uint32_t>(min_addr, p.address);
      max_addr = std::max<std::uint32_t>(max_addr, p.address);
    }
    if (max_addr - min_addr + 1 > 125)
      throw std::runtime_error("device register span exceeds 125: " + d.id);
  }
}

std::string json_escape(const std::string& input) {
  // 仅输出 JSON 允许的转义形式；控制字符统一编码为 \u00xx。
  std::ostringstream out;
  for (unsigned char c : input) {
    switch (c) {
      case '\"': out << "\\\""; break;
      case '\\': out << "\\\\"; break;
      case '\b': out << "\\b"; break;
      case '\f': out << "\\f"; break;
      case '\n': out << "\\n"; break;
      case '\r': out << "\\r"; break;
      case '\t': out << "\\t"; break;
      default:
        if (c < 0x20) out << "\\u" << std::hex << std::setw(4) << std::setfill('0') << static_cast<int>(c);
        else out << c;
    }
  }
  return out.str();
}

std::string event_to_json(const Event& event, const std::string& gateway_id,
                          const std::string& store_epoch) {
  // gateway_id + store_epoch + event_seq 构成跨重启稳定的业务幂等键。
  std::ostringstream out;
  out << std::setprecision(15)
      << "{\"schema_version\":1,\"gateway_id\":\"" << json_escape(gateway_id)
      << "\",\"store_epoch\":\"" << json_escape(store_epoch)
      << "\",\"event_seq\":" << event.event_seq << ",\"device_id\":\""
      << json_escape(event.device_id) << "\",\"received_at_ms\":" << event.received_at_ms
      << ",\"measurements\":[";
  for (std::size_t i = 0; i < event.measurements.size(); ++i) {
    const auto& m = event.measurements[i];
    if (i) out << ',';
    out << "{\"point_key\":\"" << json_escape(m.point_key) << "\",\"value\":" << m.value
        << ",\"unit\":\"" << json_escape(m.unit) << "\",\"quality\":\""
        << json_escape(m.quality) << "\"}";
  }
  out << "]}";
  return out.str();
}

}  // namespace edgelink
