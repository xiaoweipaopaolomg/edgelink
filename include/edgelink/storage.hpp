#pragma once

#include "edgelink/model.hpp"

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <future>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace edgelink {

// 可无锁读取的累计指标与在互斥锁下采样的队列深度快照。
struct StorageStats {
  std::uint64_t committed{0};
  std::uint64_t rejected{0};
  std::size_t queue_depth{0};
  std::size_t control_queue_depth{0};
  std::uint64_t control_rejected{0};
  std::int64_t pending{0};
  std::uint64_t storage_bytes{0};
  std::uint64_t storage_budget_bytes{0};
  bool storage_full{false};
};

// SQLite 单线程所有权封装。
//
// 业务线程只提交任务，不直接操作连接。采集事件与 ACK/查询分别进入有界
// 队列，控制任务优先处理，避免积压采集长期阻塞补传确认和管理查询。
class Storage {
 public:
  Storage(std::string path, std::string gateway_id, std::size_t event_capacity,
          std::size_t control_capacity, std::uint64_t storage_budget_bytes,
          std::uint64_t storage_reserve_bytes,
          std::int64_t history_retention_ms);
  ~Storage();
  Storage(const Storage&) = delete;
  Storage& operator=(const Storage&) = delete;

  void start();
  void stop();
  // 入队成功不等于已经落盘；返回 false 表示队列满或存储线程已停止。
  bool submit(Event event);

  // 以下接口由 MQTT/管理线程调用，经控制队列同步或异步交给存储线程。
  std::vector<Event> load_outbox(std::size_t limit);
  void acknowledge(std::int64_t event_seq);
  std::string query_json(const std::string& operation,
                         const std::string& point_key = {},
                         std::int64_t from_ms = 0,
                         std::int64_t to_ms = INT64_MAX,
                         std::size_t limit = 100,
                         std::int64_t after_seq = 0);
  StorageStats stats() const;
  const std::string& store_epoch() const { return store_epoch_; }
  const std::string& gateway_id() const { return gateway_id_; }

 private:
  // Stop 不受普通控制队列容量限制，确保析构和正常退出一定能够唤醒线程。
  enum class TaskKind { Insert, Ack, LoadOutbox, Query, Stop };
  struct Task {
    TaskKind kind;
    Event event;
    std::int64_t seq{0};
    std::size_t limit{0};
    std::string operation;
    std::string point_key;
    std::int64_t from_ms{0};
    std::int64_t to_ms{INT64_MAX};
    std::int64_t after_seq{0};
    std::shared_ptr<std::promise<std::vector<Event>>> events_promise;
    std::shared_ptr<std::promise<std::string>> string_promise;
  };

  void run();
  void initialize(void* db);
  bool push(Task task, bool control);
  // 在写入前预留容量；清理只允许触及已经收到业务 ACK 的历史事件。
  bool ensure_capacity(void* db, std::uint64_t needed_bytes, bool allow_cleanup);
  void refresh_capacity(void* db);

  std::string path_;
  std::string gateway_id_;
  std::string store_epoch_;
  std::size_t event_capacity_;
  std::size_t control_capacity_;
  std::uint64_t storage_budget_bytes_;
  std::uint64_t storage_reserve_bytes_;
  std::int64_t history_retention_ms_;
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  std::deque<Task> control_tasks_;  // ACK、outbox 读取和管理查询。
  std::deque<Task> event_tasks_;    // 设备采集事件。
  std::thread thread_;
  bool running_{false};
  bool ready_{false};
  std::string init_error_;
  std::atomic<std::uint64_t> committed_{0};
  std::atomic<std::uint64_t> rejected_{0};
  std::atomic<std::uint64_t> control_rejected_{0};
  std::atomic<std::int64_t> pending_{0};
  std::atomic<std::uint64_t> storage_bytes_{0};
  std::atomic<bool> storage_full_{false};
};

}  // namespace edgelink
