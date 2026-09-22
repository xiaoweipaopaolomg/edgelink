#include "edgelink/storage.hpp"
#include "edgelink/model.hpp"

#include <sqlite3.h>

#include <chrono>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <random>
#include <sstream>
#include <stdexcept>

namespace edgelink {
namespace {

// 将 SQLite 返回码统一转换为异常，保证事务错误沿同一控制流处理。
void check(int rc, sqlite3* db, const char* what) {
  if (rc != SQLITE_OK && rc != SQLITE_DONE && rc != SQLITE_ROW)
    throw std::runtime_error(std::string(what) + ": " + sqlite3_errmsg(db));
}

void exec(sqlite3* db, const char* sql) {
  char* error = nullptr;
  const int rc = sqlite3_exec(db, sql, nullptr, nullptr, &error);
  if (rc != SQLITE_OK) {
    const std::string message = error ? error : sqlite3_errmsg(db);
    sqlite3_free(error);
    throw std::runtime_error(message);
  }
}

// sqlite3_stmt 的局部 RAII 封装，避免异常路径遗漏 finalize。
class Statement {
 public:
  Statement(sqlite3* db, const char* sql) : db_(db) {
    check(sqlite3_prepare_v2(db, sql, -1, &stmt_, nullptr), db, "prepare");
  }
  ~Statement() { sqlite3_finalize(stmt_); }
  sqlite3_stmt* get() { return stmt_; }
  void reset() {
    check(sqlite3_reset(stmt_), db_, "reset");
    sqlite3_clear_bindings(stmt_);
  }
 private:
  sqlite3* db_;
  sqlite3_stmt* stmt_{nullptr};
};

std::string make_uuid() {
  // store_epoch 只要求在本项目身份域内保持高概率唯一，不承担密码学用途。
  std::random_device source;
  std::mt19937_64 random(source());
  std::uniform_int_distribution<std::uint32_t> dist;
  std::uint32_t values[4] = {dist(random), dist(random), dist(random), dist(random)};
  values[1] = (values[1] & 0xffff0fffU) | 0x00004000U;
  values[2] = (values[2] & 0x3fffffffU) | 0x80000000U;
  std::ostringstream out;
  out << std::hex << std::setfill('0') << std::setw(8) << values[0] << '-'
      << std::setw(4) << (values[1] >> 16) << '-' << std::setw(4)
      << (values[1] & 0xffff) << '-' << std::setw(4) << (values[2] >> 16)
      << '-' << std::setw(4) << (values[2] & 0xffff) << std::setw(8) << values[3];
  return out.str();
}

std::string column_text(sqlite3_stmt* stmt, int column) {
  const auto* text = sqlite3_column_text(stmt, column);
  return text ? reinterpret_cast<const char*>(text) : "";
}

}  // namespace

Storage::Storage(std::string path, std::string gateway_id,
                 std::size_t event_capacity, std::size_t control_capacity,
                 std::uint64_t storage_budget_bytes,
                 std::uint64_t storage_reserve_bytes,
                 std::int64_t history_retention_ms)
    : path_(std::move(path)), gateway_id_(std::move(gateway_id)),
      event_capacity_(event_capacity), control_capacity_(control_capacity),
      storage_budget_bytes_(storage_budget_bytes),
      storage_reserve_bytes_(storage_reserve_bytes),
      history_retention_ms_(history_retention_ms) {}

Storage::~Storage() { stop(); }

void Storage::start() {
  // 等待存储线程完成建库和身份校验，避免其他线程在数据库未就绪时工作。
  std::unique_lock<std::mutex> lock(mutex_);
  if (running_) return;
  running_ = true;
  ready_ = false;
  init_error_.clear();
  thread_ = std::thread(&Storage::run, this);
  cv_.wait(lock, [&] { return ready_; });
  if (!init_error_.empty()) {
    lock.unlock();
    if (thread_.joinable()) thread_.join();
    throw std::runtime_error(init_error_);
  }
}

void Storage::stop() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!running_) {
      if (thread_.joinable()) thread_.join();
      return;
    }
    // Stop 直接插入控制队列头部，不受容量限制；run() 会先排空采集队列。
    Task task{};
    task.kind = TaskKind::Stop;
    control_tasks_.push_front(std::move(task));
  }
  cv_.notify_one();
  if (thread_.joinable()) thread_.join();
}

bool Storage::push(Task task, bool control) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!running_) return false;
  // 两类队列分别限长，防止设备洪峰挤占 ACK 和管理请求所需的内存。
  if (control && control_tasks_.size() >= control_capacity_) {
    ++control_rejected_;
    return false;
  }
  if (!control && event_tasks_.size() >= event_capacity_) {
    ++rejected_; return false;
  }
  (control ? control_tasks_ : event_tasks_).push_back(std::move(task));
  cv_.notify_one();
  return true;
}

bool Storage::submit(Event event) {
  Task task{};
  task.kind = TaskKind::Insert;
  task.event = std::move(event);
  return push(std::move(task), false);
}

std::vector<Event> Storage::load_outbox(std::size_t limit) {
  auto promise = std::make_shared<std::promise<std::vector<Event>>>();
  auto future = promise->get_future();
  Task task{};
  task.kind = TaskKind::LoadOutbox;
  task.limit = limit;
  task.events_promise = promise;
  if (!push(std::move(task), true)) return {};
  return future.get();
}

void Storage::acknowledge(std::int64_t event_seq) {
  Task task{};
  task.kind = TaskKind::Ack;
  task.seq = event_seq;
  push(std::move(task), true);
}

std::string Storage::query_json(const std::string& operation,
                                const std::string& point_key,
                                std::int64_t from_ms, std::int64_t to_ms,
                                std::size_t limit, std::int64_t after_seq) {
  auto promise = std::make_shared<std::promise<std::string>>();
  auto future = promise->get_future();
  Task task{};
  task.kind = TaskKind::Query;
  task.operation = operation; task.point_key = point_key;
  task.from_ms = from_ms; task.to_ms = to_ms; task.limit = limit;
  task.after_seq = after_seq; task.string_promise = promise;
  if (!push(std::move(task), true)) return "{\"ok\":false,\"error\":\"storage stopped\"}";
  return future.get();
}

StorageStats Storage::stats() const {
  // 队列容器受 mutex_ 保护，其余累计量使用原子变量供状态查询采样。
  std::lock_guard<std::mutex> lock(mutex_);
  return {committed_.load(), rejected_.load(), event_tasks_.size(),
          control_tasks_.size(), control_rejected_.load(), pending_.load(),
          storage_bytes_.load(), storage_budget_bytes_, storage_full_.load()};
}

void Storage::refresh_capacity(void* opaque) {
  auto* db = static_cast<sqlite3*>(opaque);
  auto pragma_value = [&](const char* sql) -> std::uint64_t {
    Statement statement(db, sql);
    if (sqlite3_step(statement.get()) != SQLITE_ROW) return 0;
    return static_cast<std::uint64_t>(sqlite3_column_int64(statement.get(), 0));
  };
  // freelist 页面已经可复用，不应重复计入有效数据占用；WAL 需单独统计。
  const auto page_count = pragma_value("PRAGMA page_count");
  const auto freelist_count = pragma_value("PRAGMA freelist_count");
  const auto page_size = pragma_value("PRAGMA page_size");
  std::uint64_t wal_bytes = 0;
  std::error_code error;
  const auto wal_path = path_ + "-wal";
  if (std::filesystem::exists(wal_path, error))
    wal_bytes = std::filesystem::file_size(wal_path, error);
  const auto used_pages = page_count > freelist_count ? page_count - freelist_count : 0;
  storage_bytes_ = used_pages * page_size + wal_bytes;
}

bool Storage::ensure_capacity(void* opaque, std::uint64_t needed_bytes,
                              bool allow_cleanup) {
  auto* db = static_cast<sqlite3*>(opaque);
  auto available_space = [&]() -> std::uint64_t {
    std::error_code error;
    auto parent = std::filesystem::path(path_).parent_path();
    if (parent.empty()) parent = ".";
    const auto info = std::filesystem::space(parent, error);
    return error ? UINT64_MAX : info.available;
  };
  auto fits = [&](std::uint64_t target) {
    return storage_bytes_.load() + needed_bytes <= target &&
           available_space() >= storage_reserve_bytes_ + needed_bytes;
  };

  refresh_capacity(db);
  // 从满状态恢复时使用 80% 低水位，避免容量边界附近反复暂停/恢复。
  const auto target = storage_full_.load()
                          ? storage_budget_bytes_ * 8 / 10
                          : storage_budget_bytes_;
  if (fits(target)) {
    storage_full_ = false;
    return true;
  }
  if (!allow_cleanup) {
    storage_full_ = true;
    return false;
  }

  // 先截断 WAL，很多短事务造成的表面占用可在不删除业务数据时释放。
  int log_frames = 0, checkpointed = 0;
  sqlite3_wal_checkpoint_v2(db, nullptr, SQLITE_CHECKPOINT_TRUNCATE,
                            &log_frames, &checkpointed);
  refresh_capacity(db);
  if (fits(target)) {
    storage_full_ = false;
    return true;
  }

  const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();
  const auto cutoff = now_ms - history_retention_ms_;
  // 第一阶段尊重历史保留期；仍不足时才清理最旧的已确认历史。
  // LEFT JOIN outbox 是安全边界：待业务 ACK 的事件永远不会被删除。
  bool aggressive = false;
  for (int pass = 0; pass < 32 && !fits(target); ++pass) {
    Statement remove_old(
        db, aggressive
                ? "DELETE FROM events WHERE event_seq IN ("
                  "SELECT e.event_seq FROM events e LEFT JOIN outbox o USING(event_seq) "
                  "WHERE o.event_seq IS NULL ORDER BY e.event_seq LIMIT 500)"
                : "DELETE FROM events WHERE event_seq IN ("
                  "SELECT e.event_seq FROM events e LEFT JOIN outbox o USING(event_seq) "
                  "WHERE o.event_seq IS NULL AND e.received_at_ms<?1 "
                  "ORDER BY e.event_seq LIMIT 500)");
    if (!aggressive) sqlite3_bind_int64(remove_old.get(), 1, cutoff);
    exec(db, "BEGIN IMMEDIATE");
    const int rc = sqlite3_step(remove_old.get());
    if (rc != SQLITE_DONE) {
      exec(db, "ROLLBACK");
      check(rc, db, "capacity cleanup");
    }
    const int changed = sqlite3_changes(db);
    exec(db, "COMMIT");
    if (changed == 0) {
      if (!aggressive) { aggressive = true; continue; }
      break;
    }
    log_frames = 0; checkpointed = 0;
    sqlite3_wal_checkpoint_v2(db, nullptr, SQLITE_CHECKPOINT_TRUNCATE,
                              &log_frames, &checkpointed);
    refresh_capacity(db);
  }
  refresh_capacity(db);
  const bool okay = fits(target);
  storage_full_ = !okay;
  return okay;
}

void Storage::initialize(void* opaque) {
  auto* db = static_cast<sqlite3*>(opaque);
  // WAL 允许读取与写入并行，FULL 同步保证已提交事件具备崩溃恢复语义。
  exec(db, "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=3000;");
  exec(db,
       "CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);"
       "CREATE TABLE IF NOT EXISTS events("
       " event_seq INTEGER PRIMARY KEY AUTOINCREMENT, device_id TEXT NOT NULL,"
       " received_at_ms INTEGER NOT NULL, schema_version INTEGER NOT NULL DEFAULT 1);"
       "CREATE TABLE IF NOT EXISTS readings("
       " event_seq INTEGER NOT NULL REFERENCES events(event_seq) ON DELETE CASCADE,"
       " point_key TEXT NOT NULL, received_at_ms INTEGER NOT NULL, value REAL NOT NULL,"
       " unit TEXT NOT NULL, quality TEXT NOT NULL, PRIMARY KEY(event_seq,point_key));"
       "CREATE INDEX IF NOT EXISTS readings_history_idx ON readings(point_key,received_at_ms,event_seq);"
       "CREATE TABLE IF NOT EXISTS outbox("
       " event_seq INTEGER PRIMARY KEY REFERENCES events(event_seq) ON DELETE RESTRICT);"
       "INSERT OR IGNORE INTO meta(key,value) VALUES('db_version','1');");

  // 明确拒绝未知版本，防止新旧程序对同一数据库做不兼容解释。
  Statement version(db, "SELECT value FROM meta WHERE key='db_version'");
  if (sqlite3_step(version.get()) != SQLITE_ROW || column_text(version.get(), 0) != "1")
    throw std::runtime_error("unsupported database version");

  // gateway_id 防止误接管其他网关数据库；store_epoch 区分重建后的序号空间。
  Statement identity(db, "SELECT value FROM meta WHERE key=?1");
  sqlite3_bind_text(identity.get(), 1, "gateway_id", -1, SQLITE_STATIC);
  int rc = sqlite3_step(identity.get());
  if (rc == SQLITE_ROW) {
    if (column_text(identity.get(), 0) != gateway_id_)
      throw std::runtime_error("configured gateway_id conflicts with database identity");
  } else {
    Statement insert(db, "INSERT INTO meta(key,value) VALUES('gateway_id',?1)");
    sqlite3_bind_text(insert.get(), 1, gateway_id_.c_str(), -1, SQLITE_TRANSIENT);
    check(sqlite3_step(insert.get()), db, "insert gateway identity");
  }
  identity.reset();
  sqlite3_bind_text(identity.get(), 1, "store_epoch", -1, SQLITE_STATIC);
  rc = sqlite3_step(identity.get());
  if (rc == SQLITE_ROW) store_epoch_ = column_text(identity.get(), 0);
  else {
    store_epoch_ = make_uuid();
    Statement insert(db, "INSERT INTO meta(key,value) VALUES('store_epoch',?1)");
    sqlite3_bind_text(insert.get(), 1, store_epoch_.c_str(), -1, SQLITE_TRANSIENT);
    check(sqlite3_step(insert.get()), db, "insert store epoch");
  }
  Statement pending(db, "SELECT count(*) FROM outbox");
  if (sqlite3_step(pending.get()) == SQLITE_ROW) pending_ = sqlite3_column_int64(pending.get(), 0);
}

void Storage::run() {
  sqlite3* db = nullptr;
  try {
    const auto parent = std::filesystem::path(path_).parent_path();
    if (!parent.empty()) std::filesystem::create_directories(parent);
    check(sqlite3_open_v2(path_.c_str(), &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
                          nullptr), db, "open database");
    initialize(db);
    // initialize() 返回后临时语句均已析构，此时 checkpoint 不会被自身阻塞。
    ensure_capacity(db, 0, true);
  } catch (const std::exception& e) {
    if (db) sqlite3_close(db);
    std::lock_guard<std::mutex> lock(mutex_);
    init_error_ = e.what(); ready_ = true; running_ = false;
    cv_.notify_all();
    return;
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    ready_ = true;
    cv_.notify_all();
  }

  try {
    Statement insert_event(db, "INSERT INTO events(device_id,received_at_ms) VALUES(?1,?2)");
    Statement insert_reading(db, "INSERT INTO readings(event_seq,point_key,received_at_ms,value,unit,quality) VALUES(?1,?2,?3,?4,?5,?6)");
    Statement insert_outbox(db, "INSERT INTO outbox(event_seq) VALUES(?1)");
    Statement delete_outbox(db, "DELETE FROM outbox WHERE event_seq=?1");

    for (;;) {
      Task task;
      {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [&] { return !control_tasks_.empty() || !event_tasks_.empty(); });
        // ACK/查询优先于采集写入，缩短补传确认和管理命令的排队时间。
        if (!control_tasks_.empty()) {
          task = std::move(control_tasks_.front()); control_tasks_.pop_front();
        } else {
          task = std::move(event_tasks_.front()); event_tasks_.pop_front();
        }
      }
      if (task.kind == TaskKind::Stop) {
        // 正常退出先冲刷已接受的采集事件；SIGKILL 只保证此前已提交的数据。
        std::lock_guard<std::mutex> lock(mutex_);
        if (event_tasks_.empty()) break;
        control_tasks_.push_back(std::move(task));
        task = std::move(event_tasks_.front());
        event_tasks_.pop_front();
      }
      if (task.kind == TaskKind::Insert) {
        // 一次事务最多合并 50 个事件，兼顾 fsync 成本和控制任务响应时间。
        std::vector<Event> batch;
        batch.push_back(std::move(task.event));
        {
          std::lock_guard<std::mutex> lock(mutex_);
          while (batch.size() < 50 && !event_tasks_.empty()) {
            batch.push_back(std::move(event_tasks_.front().event));
            event_tasks_.pop_front();
          }
        }
        // 预估仅用于写前保护；实际占用在提交后重新从 SQLite/WAL 采样。
        std::uint64_t estimated_bytes = 0;
        for (const auto& event : batch)
          estimated_bytes += 512 + event.measurements.size() * 256;
        if (!ensure_capacity(db, estimated_bytes, true)) {
          rejected_ += batch.size();
          continue;
        }
        // events、readings、outbox 必须原子提交，禁止出现“已采集但不可补传”。
        exec(db, "BEGIN IMMEDIATE");
        try {
          for (const auto& event : batch) {
            sqlite3_bind_text(insert_event.get(), 1, event.device_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int64(insert_event.get(), 2, event.received_at_ms);
            check(sqlite3_step(insert_event.get()), db, "insert event");
            const auto seq = sqlite3_last_insert_rowid(db);
            insert_event.reset();
            for (const auto& measurement : event.measurements) {
              sqlite3_bind_int64(insert_reading.get(), 1, seq);
              sqlite3_bind_text(insert_reading.get(), 2, measurement.point_key.c_str(), -1, SQLITE_TRANSIENT);
              sqlite3_bind_int64(insert_reading.get(), 3, event.received_at_ms);
              sqlite3_bind_double(insert_reading.get(), 4, measurement.value);
              sqlite3_bind_text(insert_reading.get(), 5, measurement.unit.c_str(), -1, SQLITE_TRANSIENT);
              sqlite3_bind_text(insert_reading.get(), 6, measurement.quality.c_str(), -1, SQLITE_TRANSIENT);
              check(sqlite3_step(insert_reading.get()), db, "insert reading");
              insert_reading.reset();
            }
            sqlite3_bind_int64(insert_outbox.get(), 1, seq);
            check(sqlite3_step(insert_outbox.get()), db, "insert outbox");
            insert_outbox.reset();
          }
          exec(db, "COMMIT");
          committed_ += batch.size(); pending_ += static_cast<std::int64_t>(batch.size());
          refresh_capacity(db);
        } catch (const std::exception& error) {
          try { exec(db, "ROLLBACK"); } catch (...) {}
          rejected_ += batch.size(); storage_full_ = true;
          std::cerr << "storage insert rejected: " << error.what() << std::endl;
        }
      } else if (task.kind == TaskKind::Ack) {
        // 只有接收端业务 ACK 才删除 outbox；MQTT PUBACK 不进入此路径。
        sqlite3_bind_int64(delete_outbox.get(), 1, task.seq);
        check(sqlite3_step(delete_outbox.get()), db, "delete outbox");
        const int changed = sqlite3_changes(db);
        delete_outbox.reset();
        if (changed) {
          --pending_;
          // 事件脱离 outbox 后才具备被容量策略清理的资格。
          ensure_capacity(db, 0, true);
        }
      } else if (task.kind == TaskKind::LoadOutbox) {
        // 始终按 event_seq 顺序补传，便于接收端观察稳定顺序并控制窗口。
        std::vector<Event> result;
        Statement events(db, "SELECT e.event_seq,e.device_id,e.received_at_ms FROM outbox o JOIN events e USING(event_seq) ORDER BY e.event_seq LIMIT ?1");
        sqlite3_bind_int64(events.get(), 1, static_cast<sqlite3_int64>(task.limit));
        while (sqlite3_step(events.get()) == SQLITE_ROW) {
          Event event;
          event.event_seq = sqlite3_column_int64(events.get(), 0);
          event.device_id = column_text(events.get(), 1);
          event.received_at_ms = sqlite3_column_int64(events.get(), 2);
          Statement readings(db, "SELECT point_key,value,unit,quality FROM readings WHERE event_seq=?1 ORDER BY point_key");
          sqlite3_bind_int64(readings.get(), 1, event.event_seq);
          while (sqlite3_step(readings.get()) == SQLITE_ROW)
            event.measurements.push_back({column_text(readings.get(), 0), sqlite3_column_double(readings.get(), 1),
                                          column_text(readings.get(), 2), column_text(readings.get(), 3)});
          result.push_back(std::move(event));
        }
        task.events_promise->set_value(std::move(result));
      } else if (task.kind == TaskKind::Query) {
        // 查询也由本线程执行，保证 SQLite 连接不跨线程且结果与提交顺序一致。
        std::ostringstream out;
        out << std::setprecision(15);
        if (task.operation == "latest") {
          Statement q(db, "SELECT r.point_key,r.value,r.unit,r.quality,r.received_at_ms,r.event_seq FROM readings r JOIN (SELECT point_key,max(event_seq) seq FROM readings GROUP BY point_key) x ON x.point_key=r.point_key AND x.seq=r.event_seq ORDER BY r.point_key");
          out << "{\"ok\":true,\"items\":["; bool first = true;
          while (sqlite3_step(q.get()) == SQLITE_ROW) {
            if (!first) out << ',';
            first = false;
            out << "{\"point_key\":\"" << json_escape(column_text(q.get(),0)) << "\",\"value\":" << sqlite3_column_double(q.get(),1)
                << ",\"unit\":\"" << json_escape(column_text(q.get(),2)) << "\",\"quality\":\"" << json_escape(column_text(q.get(),3))
                << "\",\"received_at_ms\":" << sqlite3_column_int64(q.get(),4) << ",\"event_seq\":" << sqlite3_column_int64(q.get(),5) << '}';
          }
          out << "]}";
        } else if (task.operation == "history") {
          Statement q(db, "SELECT point_key,value,unit,quality,received_at_ms,event_seq FROM readings WHERE point_key=?1 AND received_at_ms>=?2 AND received_at_ms<=?3 AND event_seq>?4 ORDER BY event_seq LIMIT ?5");
          sqlite3_bind_text(q.get(),1,task.point_key.c_str(),-1,SQLITE_TRANSIENT);
          sqlite3_bind_int64(q.get(),2,task.from_ms); sqlite3_bind_int64(q.get(),3,task.to_ms);
          sqlite3_bind_int64(q.get(),4,task.after_seq); sqlite3_bind_int64(q.get(),5,static_cast<sqlite3_int64>(task.limit));
          out << "{\"ok\":true,\"items\":["; bool first = true;
          while (sqlite3_step(q.get()) == SQLITE_ROW) {
            if (!first) out << ',';
            first = false;
            out << "{\"point_key\":\"" << json_escape(column_text(q.get(),0)) << "\",\"value\":" << sqlite3_column_double(q.get(),1)
                << ",\"unit\":\"" << json_escape(column_text(q.get(),2)) << "\",\"quality\":\"" << json_escape(column_text(q.get(),3))
                << "\",\"received_at_ms\":" << sqlite3_column_int64(q.get(),4) << ",\"event_seq\":" << sqlite3_column_int64(q.get(),5) << '}';
          }
          out << "]}";
        } else out << "{\"ok\":false,\"error\":\"unsupported storage query\"}";
        task.string_promise->set_value(out.str());
      }
    }
  } catch (const std::exception& e) {
    std::cerr << "storage error: " << e.what() << std::endl;
  }
  sqlite3_close(db);
  std::lock_guard<std::mutex> lock(mutex_);
  running_ = false;
}

}  // namespace edgelink
