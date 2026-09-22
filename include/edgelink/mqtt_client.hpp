#pragma once

#include "edgelink/storage.hpp"

#include <atomic>
#include <cstdint>
#include <string>
#include <thread>

namespace edgelink {

// 项目所需的最小 MQTT 3.1.1 客户端。
//
// 客户端运行在独立线程中，只通过 Storage 的线程安全接口访问 outbox；
// Broker PUBACK 不代表业务落库，只有接收端业务 ACK 才会删除 outbox。
class MqttClient {
 public:
  MqttClient(std::string host, std::uint16_t port, int ack_timeout_ms,
             std::size_t window, Storage& storage);
  ~MqttClient();
  void start();
  void stop();
  bool connected() const { return connected_.load(); }
  std::uint64_t acknowledged() const { return acknowledged_.load(); }

 private:
  // 负责连接、订阅、窗口发送、ACK 接收、心跳和退避重连的线程主循环。
  void run();
  std::string host_;
  std::uint16_t port_;
  int ack_timeout_ms_;
  std::size_t window_;
  Storage& storage_;
  std::thread thread_;
  std::atomic<bool> stopping_{false};
  std::atomic<bool> connected_{false};
  std::atomic<std::uint64_t> acknowledged_{0};
};

}  // namespace edgelink
