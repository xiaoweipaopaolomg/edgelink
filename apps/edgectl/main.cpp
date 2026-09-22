#include "edgelink/config.hpp"
#include "edgelink/model.hpp"

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

// send/recv 都允许短传输，封装成定长操作供长度前缀协议复用。
void send_all(int fd, const void* data, std::size_t size) {
  const auto* bytes = static_cast<const std::uint8_t*>(data);
  while (size) {
    const auto count = send(fd, bytes, size, MSG_NOSIGNAL);
    if (count > 0) { bytes += count; size -= static_cast<std::size_t>(count); }
    else if (count < 0 && errno == EINTR) continue;
    else throw std::runtime_error("send failed");
  }
}

void receive_all(int fd, void* data, std::size_t size) {
  auto* bytes = static_cast<std::uint8_t*>(data);
  while (size) {
    const auto count = recv(fd, bytes, size, 0);
    if (count > 0) { bytes += count; size -= static_cast<std::size_t>(count); }
    else if (count < 0 && errno == EINTR) continue;
    else throw std::runtime_error("receive failed");
  }
}

std::string request(const std::string& socket_path, const std::string& json) {
  // 每条管理连接只承载一个请求，调用结束即关闭，客户端无需维护连接状态。
  if (json.size() > 16384) throw std::runtime_error("request too large");
  const int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd < 0) throw std::runtime_error("socket failed");
  sockaddr_un address{}; address.sun_family = AF_UNIX;
  if (socket_path.size() >= sizeof(address.sun_path)) throw std::runtime_error("socket path too long");
  std::strncpy(address.sun_path, socket_path.c_str(), sizeof(address.sun_path) - 1);
  if (connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
    const auto message = std::string("connect ") + socket_path + ": " + strerror(errno);
    close(fd); throw std::runtime_error(message);
  }
  // 与服务端约定四字节大端长度前缀，避免依赖换行等内容分隔符。
  const auto length = static_cast<std::uint32_t>(json.size());
  const std::uint8_t prefix[4] = {static_cast<std::uint8_t>(length >> 24), static_cast<std::uint8_t>(length >> 16),
                                  static_cast<std::uint8_t>(length >> 8), static_cast<std::uint8_t>(length)};
  send_all(fd, prefix, sizeof(prefix)); send_all(fd, json.data(), json.size());
  std::uint8_t response_prefix[4]; receive_all(fd, response_prefix, 4);
  const std::size_t response_size = (static_cast<std::size_t>(response_prefix[0]) << 24) |
                                    (static_cast<std::size_t>(response_prefix[1]) << 16) |
                                    (static_cast<std::size_t>(response_prefix[2]) << 8) | response_prefix[3];
  if (response_size > 1024 * 1024) { close(fd); throw std::runtime_error("response too large"); }
  std::string response(response_size, '\0'); receive_all(fd, response.data(), response.size()); close(fd);
  return response;
}

std::string string_field(const std::string& object, const std::string& key) {
  // 导出仅解析服务端自身生成的扁平对象，不作为通用 JSON 解析器使用。
  const auto marker = "\"" + key + "\":\""; const auto start = object.find(marker);
  if (start == std::string::npos) return {};
  const auto begin = start + marker.size(), end = object.find('"', begin);
  return end == std::string::npos ? std::string{} : object.substr(begin, end - begin);
}

std::string number_field(const std::string& object, const std::string& key) {
  const auto marker = "\"" + key + "\":"; const auto start = object.find(marker);
  if (start == std::string::npos) return {};
  const auto begin = start + marker.size(), end = object.find_first_of(",}", begin);
  return object.substr(begin, end - begin);
}

std::string csv_escape(const std::string& value) {
  // RFC 4180 风格：包含分隔符、引号或换行时用双引号包围，并将引号加倍。
  if (value.find_first_of(",\"\n\r") == std::string::npos) return value;
  std::string out = "\"";
  for (char c : value) out += c == '"' ? "\"\"" : std::string(1, c);
  return out + '"';
}

int export_csv(const std::string& socket_path, const std::string& point,
               const std::string& output_path) {
  std::ofstream output(output_path);
  if (!output) throw std::runtime_error("cannot create export file: " + output_path);
  output << "event_seq,point_key,received_at_ms,value,unit,quality\n";
  std::int64_t after = 0; std::size_t total = 0;
  // event_seq 作为稳定游标分页，避免大量历史一次进入内存。
  for (;;) {
    const auto query = "{\"op\":\"history\",\"point_key\":\"" + edgelink::json_escape(point) +
                       "\",\"from_ms\":0,\"to_ms\":" + std::to_string(INT64_MAX) +
                       ",\"limit\":500,\"after_seq\":" + std::to_string(after) + '}';
    const auto response = request(socket_path, query);
    std::size_t cursor = response.find("\"items\":["); std::size_t page_count = 0;
    if (cursor == std::string::npos) throw std::runtime_error("invalid history response: " + response);
    while ((cursor = response.find("{\"point_key\"", cursor)) != std::string::npos) {
      const auto end = response.find('}', cursor); if (end == std::string::npos) break;
      const auto object = response.substr(cursor, end - cursor + 1);
      const auto seq_text = number_field(object, "event_seq");
      if (seq_text.empty()) break;
      after = std::stoll(seq_text);
      output << seq_text << ',' << csv_escape(string_field(object, "point_key")) << ','
             << number_field(object, "received_at_ms") << ',' << number_field(object, "value") << ','
             << csv_escape(string_field(object, "unit")) << ',' << csv_escape(string_field(object, "quality")) << '\n';
      cursor = end + 1; ++page_count; ++total;
    }
    if (page_count < 500) break;
  }
  std::cout << "exported " << total << " rows to " << output_path << '\n'; return 0;
}

void usage() {
  std::cerr << "usage:\n"
            << "  edgectl config-check PATH\n"
            << "  edgectl [--socket PATH] status|devices|latest\n"
            << "  edgectl [--socket PATH] history POINT [FROM_MS TO_MS LIMIT AFTER_SEQ]\n"
            << "  edgectl [--socket PATH] export POINT OUTPUT.csv\n";
}

}  // namespace

int main(int argc, char** argv) {
  try {
    if (argc == 3 && std::string(argv[1]) == "config-check") {
      const auto config = edgelink::load_config(argv[2]);
      std::cout << "configuration valid: " << config.gateway_id << ", " << config.devices.size() << " device(s)\n";
      return 0;
    }
    int index = 1; std::string socket_path = "var/edgelink.sock";
    if (index + 1 < argc && std::string(argv[index]) == "--socket") { socket_path = argv[index + 1]; index += 2; }
    if (index >= argc) { usage(); return 2; }
    const std::string command = argv[index++];
    if (command == "status" || command == "devices" || command == "latest") {
      std::cout << request(socket_path, "{\"op\":\"" + command + "\"}") << '\n'; return 0;
    }
    if (command == "history" && index < argc) {
      const std::string point = argv[index++];
      const std::string from = index < argc ? argv[index++] : "0";
      const std::string to = index < argc ? argv[index++] : std::to_string(INT64_MAX);
      const std::string limit = index < argc ? argv[index++] : "100";
      const std::string after = index < argc ? argv[index++] : "0";
      std::cout << request(socket_path, "{\"op\":\"history\",\"point_key\":\"" + edgelink::json_escape(point) +
                           "\",\"from_ms\":" + from + ",\"to_ms\":" + to + ",\"limit\":" + limit +
                           ",\"after_seq\":" + after + '}') << '\n';
      return 0;
    }
    if (command == "export" && index + 1 < argc) return export_csv(socket_path, argv[index], argv[index + 1]);
    usage(); return 2;
  } catch (const std::exception& error) {
    std::cerr << "edgectl: " << error.what() << '\n'; return 1;
  }
}
