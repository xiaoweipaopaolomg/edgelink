#include "edgelink/config.hpp"
#include "edgelink/modbus.h"
#include "edgelink/mqtt_client.hpp"
#include "edgelink/storage.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <signal.h>
#include <sys/epoll.h>
#include <sys/signalfd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/timerfd.h>
#include <sys/un.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <limits>
#include <map>
#include <sstream>
#include <stdexcept>
#include <unordered_map>
#include <vector>

namespace {
using Clock = std::chrono::steady_clock;

// 业务时间使用 Unix 毫秒；超时和退避统一使用不受系统校时影响的 steady_clock。
std::int64_t utc_ms() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch()).count();
}

struct DeviceRuntime {
  // 每台设备独立推进状态机，一台设备超时不会阻塞其他设备。
  enum class State { Disconnected, Connecting, Online, Backoff };
  const edgelink::DeviceConfig* config{};
  State state{State::Disconnected};
  int fd{-1};
  std::vector<std::uint8_t> input;
  std::vector<std::uint8_t> output;
  std::size_t output_offset{0};
  std::uint16_t transaction{0};
  bool awaiting{false};
  Clock::time_point deadline{};
  Clock::time_point next_action{};
  int backoff_ms{500};
  std::uint64_t timeouts{0}, reconnects{0}, parse_errors{0};
  std::int64_t last_success_ms{0};
  std::uint16_t register_start{0}, register_count{0};
};

struct Client {
  // 管理连接同样采用非阻塞收发，输入/输出缓冲区分别保存半帧和短写状态。
  std::vector<std::uint8_t> input, output;
  std::size_t output_offset{0};
};

const char* state_name(DeviceRuntime::State state) {
  switch (state) {
    case DeviceRuntime::State::Disconnected: return "DISCONNECTED";
    case DeviceRuntime::State::Connecting: return "CONNECTING";
    case DeviceRuntime::State::Online: return "ONLINE";
    case DeviceRuntime::State::Backoff: return "BACKOFF";
  }
  return "UNKNOWN";
}

void epoll_add(int epoll_fd, int fd, std::uint32_t events) {
  epoll_event event{}; event.events = events; event.data.fd = fd;
  if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &event) != 0)
    throw std::runtime_error("epoll add: " + std::string(strerror(errno)));
}

void epoll_modify(int epoll_fd, int fd, std::uint32_t events) {
  epoll_event event{}; event.events = events; event.data.fd = fd;
  if (epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &event) != 0 && errno != ENOENT)
    throw std::runtime_error("epoll modify: " + std::string(strerror(errno)));
}

void close_device(int epoll_fd, DeviceRuntime& device, Clock::time_point now,
                  bool timed_out = false) {
  // 所有失败统一进入指数退避；成功连接后由 begin_connect/连接完成路径复位。
  if (device.fd >= 0) { epoll_ctl(epoll_fd, EPOLL_CTL_DEL, device.fd, nullptr); close(device.fd); }
  device.fd = -1; device.input.clear(); device.output.clear(); device.output_offset = 0;
  device.awaiting = false; device.state = DeviceRuntime::State::Backoff;
  device.next_action = now + std::chrono::milliseconds(device.backoff_ms);
  device.backoff_ms = std::min(device.backoff_ms * 2, 30000);
  if (timed_out) ++device.timeouts;
}

bool begin_connect(int epoll_fd, DeviceRuntime& device, Clock::time_point now) {
  addrinfo hints{}; hints.ai_family = AF_UNSPEC; hints.ai_socktype = SOCK_STREAM;
  addrinfo* list = nullptr;
  const auto port = std::to_string(device.config->port);
  if (getaddrinfo(device.config->host.c_str(), port.c_str(), &hints, &list) != 0) {
    device.state = DeviceRuntime::State::Backoff;
    device.next_action = now + std::chrono::milliseconds(device.backoff_ms); return false;
  }
  int fd = -1; int result = -1;
  for (auto* address = list; address; address = address->ai_next) {
    fd = socket(address->ai_family, address->ai_socktype | SOCK_NONBLOCK | SOCK_CLOEXEC,
                address->ai_protocol);
    if (fd < 0) continue;
    // 非阻塞 connect 返回 EINPROGRESS 是正常状态，完成结果由 EPOLLOUT + SO_ERROR 判断。
    result = connect(fd, address->ai_addr, address->ai_addrlen);
    if (result == 0 || errno == EINPROGRESS) break;
    close(fd); fd = -1;
  }
  freeaddrinfo(list);
  if (fd < 0) {
    device.state = DeviceRuntime::State::Backoff;
    device.next_action = now + std::chrono::milliseconds(device.backoff_ms); return false;
  }
  device.fd = fd; device.state = result == 0 ? DeviceRuntime::State::Online : DeviceRuntime::State::Connecting;
  device.deadline = now + std::chrono::milliseconds(device.config->timeout_ms);
  const std::uint32_t events = EPOLLIN | EPOLLRDHUP |
                               (result == 0 ? 0U : static_cast<std::uint32_t>(EPOLLOUT));
  epoll_add(epoll_fd, fd, events);
  if (result == 0) { device.next_action = now; device.backoff_ms = 500; ++device.reconnects; }
  return true;
}

std::string json_string(const std::string& json, const std::string& key) {
  // 管理协议由 edgectl 生成且字段受限，因此仅提取所需的简单 JSON 标量。
  const auto marker = "\"" + key + "\":\"";
  const auto start = json.find(marker);
  if (start == std::string::npos) return {};
  const auto begin = start + marker.size(), end = json.find('"', begin);
  return end == std::string::npos ? std::string{} : json.substr(begin, end - begin);
}

std::int64_t json_integer(const std::string& json, const std::string& key,
                          std::int64_t fallback) {
  const auto marker = "\"" + key + "\":";
  const auto start = json.find(marker);
  if (start == std::string::npos) return fallback;
  try { return std::stoll(json.substr(start + marker.size())); }
  catch (...) { return fallback; }
}

std::vector<std::uint8_t> frame_response(const std::string& json) {
  // UDS 管理协议使用四字节大端长度前缀，允许可靠处理流式 Socket 分段。
  std::vector<std::uint8_t> framed(4 + json.size());
  const auto size = static_cast<std::uint32_t>(json.size());
  framed[0] = static_cast<std::uint8_t>(size >> 24); framed[1] = static_cast<std::uint8_t>(size >> 16);
  framed[2] = static_cast<std::uint8_t>(size >> 8); framed[3] = static_cast<std::uint8_t>(size);
  std::copy(json.begin(), json.end(), framed.begin() + 4); return framed;
}

std::string management_response(const std::string& request,
                                const std::vector<DeviceRuntime>& devices,
                                edgelink::Storage& storage,
                                const edgelink::MqttClient& mqtt,
                                bool collection_paused) {
  const auto op = json_string(request, "op");
  if (op == "status") {
    // status 聚合跨线程原子快照，不在事件循环中直接访问 SQLite。
    const auto stats = storage.stats();
    std::ostringstream out;
    out << "{\"ok\":true,\"gateway_id\":\"" << edgelink::json_escape(storage.gateway_id())
        << "\",\"store_epoch\":\"" << storage.store_epoch() << "\",\"mqtt_connected\":"
        << (mqtt.connected() ? "true" : "false") << ",\"committed_events\":" << stats.committed
        << ",\"pending_events\":" << stats.pending << ",\"event_queue_depth\":" << stats.queue_depth
        << ",\"control_queue_depth\":" << stats.control_queue_depth
        << ",\"control_rejected\":" << stats.control_rejected
        << ",\"rejected_events\":" << stats.rejected
        << ",\"collection_paused\":" << (collection_paused ? "true" : "false")
        << ",\"storage_full\":" << (stats.storage_full ? "true" : "false")
        << ",\"storage_bytes\":" << stats.storage_bytes
        << ",\"storage_budget_bytes\":" << stats.storage_budget_bytes
        << ",\"business_acks\":" << mqtt.acknowledged() << '}';
    return out.str();
  }
  if (op == "devices") {
    std::ostringstream out; out << "{\"ok\":true,\"items\":[";
    for (std::size_t i = 0; i < devices.size(); ++i) {
      const auto& d = devices[i]; if (i) out << ',';
      out << "{\"device_id\":\"" << edgelink::json_escape(d.config->id) << "\",\"state\":\""
          << state_name(d.state) << "\",\"last_success_ms\":" << d.last_success_ms
          << ",\"timeouts\":" << d.timeouts << ",\"reconnects\":" << d.reconnects
          << ",\"parse_errors\":" << d.parse_errors << '}';
    }
    out << "]}"; return out.str();
  }
  if (op == "latest") return storage.query_json("latest");
  if (op == "history") {
    auto limit = json_integer(request, "limit", 100); limit = std::max<std::int64_t>(1, std::min<std::int64_t>(limit, 500));
    return storage.query_json("history", json_string(request, "point_key"),
                              json_integer(request, "from_ms", 0), json_integer(request, "to_ms", INT64_MAX),
                              static_cast<std::size_t>(limit), json_integer(request, "after_seq", 0));
  }
  return "{\"ok\":false,\"error\":\"unknown operation\"}";
}

int create_management_socket(const std::string& path) {
  const auto parent = std::filesystem::path(path).parent_path();
  if (!parent.empty()) std::filesystem::create_directories(parent);
  // 清理上次异常退出留下的 Socket 节点；数据库等持久化文件不受影响。
  unlink(path.c_str());
  const int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (fd < 0) throw std::runtime_error("create management socket failed");
  sockaddr_un address{}; address.sun_family = AF_UNIX;
  if (path.size() >= sizeof(address.sun_path)) throw std::runtime_error("management socket path too long");
  std::strncpy(address.sun_path, path.c_str(), sizeof(address.sun_path) - 1);
  if (bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 || listen(fd, 16) != 0) {
    close(fd); throw std::runtime_error("bind/listen management socket: " + std::string(strerror(errno)));
  }
  chmod(path.c_str(), 0660);
  return fd;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 3 || std::string(argv[1]) != "--config") {
    std::cerr << "usage: edgelinkd --config PATH\n"; return 2;
  }
  try {
    auto config = edgelink::load_config(argv[2]);
    // 启动顺序保证存储身份和表结构验证成功后才启动 MQTT 与设备采集。
    edgelink::Storage storage(config.database_path, config.gateway_id,
                              config.event_queue_capacity,
                              config.control_queue_capacity,
                              config.storage_budget_bytes,
                              config.storage_reserve_bytes,
                              config.history_retention_ms);
    storage.start();
    edgelink::MqttClient mqtt(config.mqtt_host, config.mqtt_port, config.mqtt_ack_timeout_ms,
                              config.mqtt_window, storage);
    mqtt.start();

    // timerfd、signalfd、设备 Socket 和管理 Socket 全部纳入同一个 epoll 循环。
    const int epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd < 0) throw std::runtime_error("epoll_create1 failed");
    const int timer_fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    itimerspec timer{}; timer.it_value.tv_nsec = 50000000; timer.it_interval.tv_nsec = 50000000;
    if (timer_fd < 0 || timerfd_settime(timer_fd, 0, &timer, nullptr) != 0)
      throw std::runtime_error("timerfd setup failed");
    // 将终止信号转换为 fd 事件，避免异步信号处理函数访问非安全对象。
    sigset_t mask; sigemptyset(&mask); sigaddset(&mask, SIGINT); sigaddset(&mask, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &mask, nullptr);
    const int signal_fd = signalfd(-1, &mask, SFD_NONBLOCK | SFD_CLOEXEC);
    if (signal_fd < 0) throw std::runtime_error("signalfd setup failed");
    const int listener_fd = create_management_socket(config.socket_path);
    epoll_add(epoll_fd, timer_fd, EPOLLIN); epoll_add(epoll_fd, signal_fd, EPOLLIN);
    epoll_add(epoll_fd, listener_fd, EPOLLIN);

    std::vector<DeviceRuntime> devices;
    for (const auto& d : config.devices) {
      DeviceRuntime runtime; runtime.config = &d; runtime.next_action = Clock::now();
      auto minmax = std::minmax_element(d.points.begin(), d.points.end(),
        [](const auto& a, const auto& b) { return a.address < b.address; });
      // 同一设备的点位合并为一段连续寄存器读取，减少请求次数。
      runtime.register_start = minmax.first->address;
      runtime.register_count = static_cast<std::uint16_t>(minmax.second->address - minmax.first->address + 1);
      devices.push_back(std::move(runtime));
    }
    std::unordered_map<int, std::size_t> device_by_fd;
    std::map<int, Client> clients;
    bool stopping = false;
    bool collection_paused = false;
    std::cout << "edgelinkd started gateway=" << config.gateway_id << " epoch=" << storage.store_epoch() << std::endl;

    while (!stopping) {
      epoll_event events[64];
      const int count = epoll_wait(epoll_fd, events, 64, 1000);
      if (count < 0 && errno == EINTR) continue;
      if (count < 0) throw std::runtime_error("epoll_wait failed");
      for (int i = 0; i < count; ++i) {
        const int fd = events[i].data.fd; const auto flags = events[i].events;
        if (fd == signal_fd) { signalfd_siginfo info{}; read(fd, &info, sizeof(info)); stopping = true; continue; }
        if (fd == timer_fd) {
          std::uint64_t expirations; read(fd, &expirations, sizeof(expirations));
          const auto now = Clock::now();
          const auto storage_stats = storage.stats();
          // 高低水位形成滞回：暂停只阻止新轮询，不妨碍连接维护、ACK 和补传。
          if (storage_stats.storage_full ||
              storage_stats.queue_depth >= config.event_queue_high_watermark)
            collection_paused = true;
          else if (collection_paused &&
                   storage_stats.queue_depth <= config.event_queue_low_watermark)
            collection_paused = false;
          for (std::size_t index = 0; index < devices.size(); ++index) {
            auto& d = devices[index];
            // 定时事件只推动到期状态，实际网络收发仍由对应 fd 事件驱动。
            if ((d.state == DeviceRuntime::State::Disconnected || d.state == DeviceRuntime::State::Backoff) && now >= d.next_action) {
              if (begin_connect(epoll_fd, d, now)) device_by_fd[d.fd] = index;
            } else if (d.state == DeviceRuntime::State::Connecting && now >= d.deadline) {
              device_by_fd.erase(d.fd); close_device(epoll_fd, d, now, true);
            } else if (d.state == DeviceRuntime::State::Online && d.awaiting && now >= d.deadline) {
              device_by_fd.erase(d.fd); close_device(epoll_fd, d, now, true);
            } else if (!collection_paused &&
                       d.state == DeviceRuntime::State::Online && !d.awaiting &&
                       d.output.empty() && now >= d.next_action) {
              std::uint8_t request[12]; ++d.transaction;
              edgelink_modbus_build_read03(d.transaction, d.config->unit_id, d.register_start, d.register_count, request);
              d.output.assign(request, request + sizeof(request)); d.output_offset = 0;
              epoll_modify(epoll_fd, d.fd, EPOLLIN | EPOLLOUT | EPOLLRDHUP);
            }
          }
          continue;
        }
        if (fd == listener_fd) {
          // 限制同时在线的管理客户端数量，防止本地连接耗尽文件描述符。
          while (clients.size() < 32) {
            const int client = accept4(fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (client < 0) break;
            clients.emplace(client, Client{}); epoll_add(epoll_fd, client, EPOLLIN | EPOLLRDHUP);
          }
          continue;
        }
        auto dit = device_by_fd.find(fd);
        if (dit != device_by_fd.end()) {
          auto& d = devices[dit->second]; const auto now = Clock::now();
          if (flags & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) { device_by_fd.erase(dit); close_device(epoll_fd, d, now); continue; }
          if (d.state == DeviceRuntime::State::Connecting && (flags & EPOLLOUT)) {
            // 可写仅表示 connect 已结束，SO_ERROR 才是最终连接结果。
            int error = 0; socklen_t length = sizeof(error); getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &length);
            if (error) { device_by_fd.erase(fd); close_device(epoll_fd, d, now); continue; }
            d.state = DeviceRuntime::State::Online; d.next_action = now; d.backoff_ms = 500; ++d.reconnects;
            epoll_modify(epoll_fd, fd, EPOLLIN | EPOLLRDHUP);
          }
          if ((flags & EPOLLOUT) && !d.output.empty()) {
            // 保存 offset 处理非阻塞 Socket 短写；请求完整发出后才开始响应超时计时。
            const auto written = send(fd, d.output.data() + d.output_offset, d.output.size() - d.output_offset, MSG_NOSIGNAL);
            if (written > 0) d.output_offset += static_cast<std::size_t>(written);
            if (d.output_offset == d.output.size()) {
              d.output.clear(); d.output_offset = 0; d.awaiting = true;
              d.deadline = now + std::chrono::milliseconds(d.config->timeout_ms);
              epoll_modify(epoll_fd, fd, EPOLLIN | EPOLLRDHUP);
            }
          }
          if (flags & EPOLLIN) {
            std::uint8_t buffer[1024];
            for (;;) {
              const auto received = recv(fd, buffer, sizeof(buffer), 0);
              if (received > 0) d.input.insert(d.input.end(), buffer, buffer + received);
              else if (received < 0 && errno == EINTR) continue;
              else if (received < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
              else { device_by_fd.erase(fd); close_device(epoll_fd, d, now); break; }
            }
            if (d.fd < 0) continue;
            // 响应缓冲设硬上限，异常长度或不发送完整帧的对端会被断开重连。
            if (d.input.size() > 1024) { ++d.parse_errors; device_by_fd.erase(fd); close_device(epoll_fd, d, now); continue; }
            std::size_t frame_size = 0;
            const int framed = edgelink_modbus_frame_size(d.input.data(), d.input.size(), &frame_size);
            if (framed == EDGELINK_MODBUS_INVALID) { ++d.parse_errors; device_by_fd.erase(fd); close_device(epoll_fd, d, now); continue; }
            if (framed == EDGELINK_MODBUS_OK) {
              std::vector<std::uint16_t> registers(d.register_count); std::size_t register_count = 0; std::uint8_t exception = 0;
              const int parsed = edgelink_modbus_parse_read03(d.input.data(), frame_size, d.transaction, d.config->unit_id,
                                                               registers.data(), registers.size(), &register_count, &exception);
              if (parsed != EDGELINK_MODBUS_OK || register_count != d.register_count) {
                ++d.parse_errors; device_by_fd.erase(fd); close_device(epoll_fd, d, now); continue;
              }
              // 一次 Modbus 响应对应一个原子事件，所有点位共享采集时间。
              edgelink::Event event; event.device_id = d.config->id; event.received_at_ms = utc_ms();
              for (const auto& p : d.config->points) {
                const auto raw = registers[p.address - d.register_start];
                const double value = p.is_signed ? static_cast<double>(static_cast<std::int16_t>(raw)) * p.scale
                                                 : static_cast<double>(raw) * p.scale;
                event.measurements.push_back({p.key, value, p.unit, "good"});
              }
              // 只有成功进入有界存储队列才记为成功，队列满时由反压机制暂停后续轮询。
              if (storage.submit(std::move(event))) d.last_success_ms = utc_ms();
              d.awaiting = false;
              d.next_action = now + std::chrono::milliseconds(d.config->poll_ms);
              d.input.erase(d.input.begin(), d.input.begin() + static_cast<std::ptrdiff_t>(frame_size));
            }
          }
          continue;
        }
        auto cit = clients.find(fd);
        if (cit != clients.end()) {
          auto& client = cit->second; bool close_client = flags & (EPOLLERR | EPOLLHUP | EPOLLRDHUP);
          if ((flags & EPOLLIN) && client.output.empty()) {
            std::uint8_t buffer[4096]; const auto received = recv(fd, buffer, sizeof(buffer), 0);
            if (received > 0) client.input.insert(client.input.end(), buffer, buffer + received); else close_client = true;
            if (client.input.size() >= 4) {
              const std::size_t length = (static_cast<std::size_t>(client.input[0]) << 24) | (static_cast<std::size_t>(client.input[1]) << 16) |
                                         (static_cast<std::size_t>(client.input[2]) << 8) | client.input[3];
              // 管理请求限制为 16 KiB，响应发送后主动关闭，协议保持一问一答。
              if (length > 16384) close_client = true;
              else if (client.input.size() >= 4 + length) {
                const std::string request(client.input.begin() + 4, client.input.begin() + static_cast<std::ptrdiff_t>(4 + length));
                client.output = frame_response(
                    management_response(request, devices, storage, mqtt,
                                        collection_paused));
                epoll_modify(epoll_fd, fd, EPOLLOUT | EPOLLRDHUP);
              }
            }
          }
          if ((flags & EPOLLOUT) && !client.output.empty()) {
            const auto written = send(fd, client.output.data() + client.output_offset, client.output.size() - client.output_offset, MSG_NOSIGNAL);
            if (written > 0) client.output_offset += static_cast<std::size_t>(written);
            if (client.output_offset == client.output.size()) close_client = true;
          }
          if (close_client) { epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr); close(fd); clients.erase(cit); }
        }
      }
    }
    // 先退出事件循环并关闭采集 fd，再停止 MQTT，最后冲刷存储事件队列。
    for (auto& item : clients) close(item.first);
    for (auto& d : devices) if (d.fd >= 0) close(d.fd);
    close(listener_fd); unlink(config.socket_path.c_str()); close(signal_fd); close(timer_fd); close(epoll_fd);
    mqtt.stop(); storage.stop();
    std::cout << "edgelinkd stopped" << std::endl;
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "edgelinkd: " << error.what() << '\n'; return 1;
  }
}
