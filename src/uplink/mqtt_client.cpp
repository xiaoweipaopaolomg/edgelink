#include "edgelink/mqtt_client.hpp"
#include "edgelink/model.hpp"

#include <arpa/inet.h>
#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <map>
#include <stdexcept>
#include <vector>

namespace edgelink {
namespace {

using Clock = std::chrono::steady_clock;

// MQTT 二字节整数和 UTF-8 字符串都使用网络字节序长度前缀。
void append_u16(std::vector<std::uint8_t>& out, std::uint16_t value) {
  out.push_back(static_cast<std::uint8_t>(value >> 8));
  out.push_back(static_cast<std::uint8_t>(value));
}

void append_string(std::vector<std::uint8_t>& out, const std::string& value) {
  if (value.size() > 65535) throw std::runtime_error("MQTT string too long");
  append_u16(out, static_cast<std::uint16_t>(value.size()));
  out.insert(out.end(), value.begin(), value.end());
}

std::vector<std::uint8_t> packet(std::uint8_t header,
                                 const std::vector<std::uint8_t>& body) {
  // Remaining Length 采用最多四字节的 128 进制变长编码。
  std::vector<std::uint8_t> result{header};
  std::size_t remaining = body.size();
  do {
    std::uint8_t byte = remaining % 128;
    remaining /= 128;
    if (remaining) byte |= 0x80;
    result.push_back(byte);
  } while (remaining);
  result.insert(result.end(), body.begin(), body.end());
  return result;
}

bool send_all(int fd, const std::vector<std::uint8_t>& data) {
  // TCP 可能短写；MSG_NOSIGNAL 避免对端关闭时用 SIGPIPE 终止进程。
  std::size_t offset = 0;
  while (offset < data.size()) {
    const auto written = send(fd, data.data() + offset, data.size() - offset, MSG_NOSIGNAL);
    if (written > 0) offset += static_cast<std::size_t>(written);
    else if (written < 0 && errno == EINTR) continue;
    else return false;
  }
  return true;
}

int connect_tcp(const std::string& host, std::uint16_t port) {
  // 遍历 getaddrinfo 结果，因而同时支持 IPv4、IPv6 和主机名。
  addrinfo hints{}; hints.ai_family = AF_UNSPEC; hints.ai_socktype = SOCK_STREAM;
  addrinfo* list = nullptr;
  const auto port_text = std::to_string(port);
  if (getaddrinfo(host.c_str(), port_text.c_str(), &hints, &list) != 0) return -1;
  int fd = -1;
  for (auto* address = list; address; address = address->ai_next) {
    fd = socket(address->ai_family, address->ai_socktype | SOCK_CLOEXEC, address->ai_protocol);
    if (fd >= 0 && connect(fd, address->ai_addr, address->ai_addrlen) == 0) break;
    if (fd >= 0) close(fd);
    fd = -1;
  }
  freeaddrinfo(list);
  return fd;
}

bool read_packet(int fd, std::uint8_t& header, std::vector<std::uint8_t>& body,
                 int timeout_ms) {
  // 限时读取单个完整 MQTT 包；长度上限保护测试客户端不被异常 Broker 拖垮。
  pollfd pfd{fd, POLLIN, 0};
  if (poll(&pfd, 1, timeout_ms) <= 0) return false;
  auto read_byte = [&](std::uint8_t& byte) {
    for (;;) {
      const auto count = recv(fd, &byte, 1, 0);
      if (count == 1) return true;
      if (count < 0 && errno == EINTR) continue;
      return false;
    }
  };
  if (!read_byte(header)) return false;
  std::size_t length = 0, multiplier = 1;
  for (int i = 0; i < 4; ++i) {
    std::uint8_t byte;
    if (!read_byte(byte)) return false;
    length += (byte & 0x7f) * multiplier;
    if (!(byte & 0x80)) break;
    multiplier *= 128;
    if (i == 3) return false;
  }
  if (length > 16384) return false;
  body.resize(length);
  std::size_t offset = 0;
  while (offset < length) {
    const auto count = recv(fd, body.data() + offset, length - offset, 0);
    if (count > 0) offset += static_cast<std::size_t>(count);
    else if (count < 0 && errno == EINTR) continue;
    else return false;
  }
  return true;
}

std::vector<std::uint8_t> connect_packet(const std::string& client_id) {
  std::vector<std::uint8_t> body;
  // MQTT 3.1.1、Clean Session、30 秒 keep-alive。
  append_string(body, "MQTT"); body.push_back(4); body.push_back(2);
  append_u16(body, 30); append_string(body, client_id);
  return packet(0x10, body);
}

std::vector<std::uint8_t> subscribe_packet(std::uint16_t id,
                                           const std::string& topic) {
  std::vector<std::uint8_t> body; append_u16(body, id); append_string(body, topic);
  body.push_back(1); return packet(0x82, body);
}

std::vector<std::uint8_t> publish_packet(std::uint16_t id,
                                         const std::string& topic,
                                         const std::string& payload) {
  // 固定头 0x32 表示 QoS 1 PUBLISH；业务可靠性仍由应用层 ACK 保证。
  std::vector<std::uint8_t> body; append_string(body, topic); append_u16(body, id);
  body.insert(body.end(), payload.begin(), payload.end());
  return packet(0x32, body);
}

bool get_json_string(const std::string& json, const std::string& key,
                     std::string& value) {
  // ACK 格式由本项目控制且字段简单，使用受限提取避免引入通用 JSON 依赖。
  const auto marker = "\"" + key + "\":\"";
  const auto start = json.find(marker);
  if (start == std::string::npos) return false;
  const auto begin = start + marker.size();
  const auto end = json.find('"', begin);
  if (end == std::string::npos) return false;
  value = json.substr(begin, end - begin);
  return true;
}

bool get_json_integer(const std::string& json, const std::string& key,
                      std::int64_t& value) {
  const auto marker = "\"" + key + "\":";
  const auto start = json.find(marker);
  if (start == std::string::npos) return false;
  try { value = std::stoll(json.substr(start + marker.size())); return true; }
  catch (...) { return false; }
}

}  // namespace

MqttClient::MqttClient(std::string host, std::uint16_t port, int ack_timeout_ms,
                       std::size_t window, Storage& storage)
    : host_(std::move(host)), port_(port), ack_timeout_ms_(ack_timeout_ms),
      window_(window), storage_(storage) {}

MqttClient::~MqttClient() { stop(); }
void MqttClient::start() { stopping_ = false; thread_ = std::thread(&MqttClient::run, this); }
void MqttClient::stop() {
  stopping_ = true;
  if (thread_.joinable()) thread_.join();
}

void MqttClient::run() {
  int backoff_ms = 500;
  while (!stopping_) {
    const int fd = connect_tcp(host_, port_);
    if (fd < 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
      backoff_ms = std::min(backoff_ms * 2, 30000); continue;
    }
    const std::string ack_topic = "edgelink/" + storage_.gateway_id() + "/acks";
    const std::string event_topic = "edgelink/" + storage_.gateway_id() + "/events";
    std::uint8_t header = 0; std::vector<std::uint8_t> body;
    // 完成 CONNECT/CONNACK 与 ACK 主题订阅后，才对外报告 connected。
    if (!send_all(fd, connect_packet("edgelinkd-" + storage_.gateway_id())) ||
        !read_packet(fd, header, body, 3000) || (header >> 4) != 2 || body.size() != 2 || body[1] != 0 ||
        !send_all(fd, subscribe_packet(1, ack_topic)) ||
        !read_packet(fd, header, body, 3000) || (header >> 4) != 9) {
      close(fd); continue;
    }
    connected_ = true; backoff_ms = 500;
    // key 为持久化事件序号，value 为最近一次发送时间；超时后允许同事件重发。
    std::map<std::int64_t, Clock::time_point> in_flight;
    std::uint16_t packet_id = 2;
    auto last_ping = Clock::now();
    bool healthy = true;
    while (!stopping_ && healthy) {
      const auto now = Clock::now();
      // 每轮只取有限窗口，避免断线积压一次性占满发送内存。
      const auto events = storage_.load_outbox(window_);
      for (const auto& event : events) {
        const auto found = in_flight.find(event.event_seq);
        if (found != in_flight.end() &&
            std::chrono::duration_cast<std::chrono::milliseconds>(now - found->second).count() < ack_timeout_ms_)
          continue;
        if (packet_id == 0) packet_id = 1;
        const auto payload = event_to_json(event, storage_.gateway_id(), storage_.store_epoch());
        if (payload.size() > 8192 || !send_all(fd, publish_packet(packet_id++, event_topic, payload))) {
          healthy = false; break;
        }
        in_flight[event.event_seq] = now;
      }
      pollfd pfd{fd, POLLIN, 0};
      const int available = poll(&pfd, 1, 200);
      if (available < 0 && errno != EINTR) healthy = false;
      else if (available > 0) {
        if (!read_packet(fd, header, body, 1000)) { healthy = false; break; }
        const auto type = header >> 4;
        if (type == 3 && body.size() >= 2) {
          const std::size_t topic_len = (static_cast<std::size_t>(body[0]) << 8) | body[1];
          std::size_t offset = 2 + topic_len;
          if (offset > body.size()) { healthy = false; break; }
          std::uint16_t incoming_id = 0;
          if (((header >> 1) & 3) > 0) {
            if (offset + 2 > body.size()) { healthy = false; break; }
            incoming_id = static_cast<std::uint16_t>((body[offset] << 8) | body[offset + 1]); offset += 2;
          }
          const std::string payload(body.begin() + static_cast<std::ptrdiff_t>(offset), body.end());
          std::string gateway, epoch; std::int64_t seq = 0;
          // 三元身份全部匹配才接受 ACK，防止旧数据库或其他网关误确认。
          if (get_json_string(payload, "gateway_id", gateway) &&
              get_json_string(payload, "store_epoch", epoch) &&
              get_json_integer(payload, "event_seq", seq) &&
              gateway == storage_.gateway_id() && epoch == storage_.store_epoch()) {
            storage_.acknowledge(seq); in_flight.erase(seq); ++acknowledged_;
          }
          if (incoming_id) {
            std::vector<std::uint8_t> ack_body; append_u16(ack_body, incoming_id);
            healthy = send_all(fd, packet(0x40, ack_body));
          }
        }
      }
      // 周期心跳也用于尽早发现半开 TCP 连接。
      if (now - last_ping > std::chrono::seconds(10)) {
        healthy = send_all(fd, packet(0xc0, {})); last_ping = now;
      }
    }
    connected_ = false; close(fd);
    if (!stopping_) std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
  }
}

}  // namespace edgelink
