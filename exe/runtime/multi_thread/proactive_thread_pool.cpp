#include "proactive_thread_pool.hpp"

#include <cassert>

namespace exe::runtime::multi_thread {

thread_local ProactiveThreadPool* ProactiveThreadPool::tls_pool_ = nullptr;
thread_local ProactiveThreadPool::Worker* ProactiveThreadPool::tls_worker_ = nullptr;

// ---------------- Coordinator ----------------

void ProactiveThreadPool::Coordinator::NotifyOnSubmit() {
  if (sleeping_.load(std::memory_order_acquire) == 0) {
    return;
  }

  while (sleeping_.load(std::memory_order_acquire) != 0) {
    Worker* w = nullptr;

    lock_.Lock();
    if (sleepers_head_ != nullptr) {
      w = sleepers_head_;
      sleepers_head_ = sleepers_head_->sleep_next_;
      w->sleep_next_ = nullptr;
    }
    lock_.Unlock();

    if (w == nullptr) {
      return;
    }

    if (w->TryWakeByCoordinator()) {
      sleeping_.fetch_sub(1, std::memory_order_release);
      w->Wake();
      return;
    }
  }
}

void ProactiveThreadPool::Coordinator::CancelSleepCount() {
  sleeping_.fetch_sub(1, std::memory_order_release);
}

void ProactiveThreadPool::Coordinator::AddSleeper(Worker* w) {
  lock_.Lock();
  w->sleep_next_ = sleepers_head_;
  sleepers_head_ = w;
  sleeping_.fetch_add(1, std::memory_order_release);
  lock_.Unlock();
}

bool ProactiveThreadPool::Coordinator::TryEnterSpinning() {
  size_t cur = spinners_.load(std::memory_order_relaxed);
  while (cur < max_spinners_) {
    if (spinners_.compare_exchange_weak(cur, cur + 1,
                                        std::memory_order_acq_rel,
                                        std::memory_order_relaxed)) {
      return true;
    }
  }
  return false;
}

void ProactiveThreadPool::Coordinator::LeaveSpinning() {
  spinners_.fetch_sub(1, std::memory_order_release);
}

void ProactiveThreadPool::Coordinator::CancelOneSleep() {
  sleeping_.fetch_sub(1, std::memory_order_release);
}

void ProactiveThreadPool::Coordinator::WakeAll() {
  while (sleeping_.load(std::memory_order_acquire) != 0) {
    Worker* w = nullptr;

    lock_.Lock();
    if (sleepers_head_ != nullptr) {
      w = sleepers_head_;
      sleepers_head_ = sleepers_head_->sleep_next_;
      w->sleep_next_ = nullptr;
    }
    lock_.Unlock();

    if (w == nullptr) {
      return;
    }

    if (w->TryWakeByCoordinator()) {
      sleeping_.fetch_sub(1, std::memory_order_release);
      w->Wake();
    }
  }
}

// ---------------- Worker ----------------

ProactiveThreadPool::Worker::Worker(ProactiveThreadPool* pool, size_t index)
    : pool_(pool),
      index_(index) {
  std::random_device rd;
  uint64_t seed = (uint64_t(rd()) << 32) ^ uint64_t(rd()) ^ uint64_t(index);
  rng_.seed(seed);
}

void ProactiveThreadPool::Worker::Start() {
  thread_ = std::thread([this] {
    ProactiveThreadPool::tls_pool_ = pool_;
    ProactiveThreadPool::tls_worker_ = this;
    Run();
    ProactiveThreadPool::tls_worker_ = nullptr;
    ProactiveThreadPool::tls_pool_ = nullptr;
  });
}

void ProactiveThreadPool::Worker::Join() {
  if (thread_.joinable()) {
    thread_.join();
  }
}

void ProactiveThreadPool::Worker::Wake() {
  pool_->stats_.wakes.fetch_add(1, std::memory_order_relaxed);
  park_event_.WakeOne();
}

bool ProactiveThreadPool::Worker::TryWakeByCoordinator() {
  return sleep_state_.exchange(0, std::memory_order_acq_rel) == 1;
}

bool ProactiveThreadPool::Worker::TryCancelSleepBySelf() {
  uint8_t expected = 1;
  return sleep_state_.compare_exchange_strong(expected, 0, std::memory_order_acq_rel);
}

void ProactiveThreadPool::Worker::Push(task::TaskBase* t, task::SchedulingHint hint) {
  if (hint == task::SchedulingHint::Next) {
    task::TaskBase* prev = lifo_.exchange(t, std::memory_order_acq_rel);
    if (prev) {
      if (!local_.TryPush(prev)) {
        pool_->OffloadLocalToGlobal(*this);
        if (!local_.TryPush(prev)) {
          pool_->global_.PushOne(prev);
        }
      }
    }
    return;
  }

  if (hint == task::SchedulingHint::Yield) {
    pool_->global_.PushOne(t);
    return;
  }

  if (!local_.TryPush(t)) {
    pool_->OffloadLocalToGlobal(*this);
    if (!local_.TryPush(t)) {
      pool_->global_.PushOne(t);
    }
  }
}

void ProactiveThreadPool::Worker::Run() {
  while (true) {
    if (pool_->stopping_.load(std::memory_order_acquire) &&
        pool_->tasks_in_system_.load(std::memory_order_acquire) == 0) {
      return;
    }

    task::TaskBase* t = pool_->PickNext(*this);
    if (t) {
      lifo_runs_ = 0;
      t->Run();

      size_t left =
          pool_->tasks_in_system_.fetch_sub(1, std::memory_order_acq_rel) - 1;
      if (pool_->stopping_.load(std::memory_order_acquire) && left == 0) {
        pool_->coord_.WakeAll();
      }
      continue;
    }

    if (pool_->stopping_.load(std::memory_order_acquire) &&
        pool_->tasks_in_system_.load(std::memory_order_acquire) == 0) {
      return;
    }

    uint32_t epoch = park_event_.Epoch();

    sleep_state_.store(1, std::memory_order_release);
    pool_->coord_.AddSleeper(this);

    if (task::TaskBase* t2 = pool_->PickNext(*this)) {
      if (TryCancelSleepBySelf()) {
        pool_->coord_.CancelSleepCount();
      }

      lifo_runs_ = 0;
      t2->Run();

      size_t left = pool_->tasks_in_system_.fetch_sub(1, std::memory_order_acq_rel) - 1;
      if (pool_->stopping_.load(std::memory_order_acquire) && left == 0) {
        pool_->coord_.WakeAll();
      }
      continue;
    }

    pool_->stats_.parks.fetch_add(1, std::memory_order_relaxed);
    park_event_.Park(epoch);

    if (TryCancelSleepBySelf()) {
      pool_->coord_.CancelSleepCount();
    }
  }
}

// ---------------- ProactiveThreadPool ----------------

ProactiveThreadPool::ProactiveThreadPool(size_t threads)
    : threads_(threads == 0 ? std::thread::hardware_concurrency() : threads),
      coord_(threads_) {
  workers_.reserve(threads_);
  for (size_t i = 0; i < threads_; ++i) {
    workers_.push_back(std::make_unique<Worker>(this, i));
  }
}

ProactiveThreadPool::~ProactiveThreadPool() {
  Stop();
}

void ProactiveThreadPool::Start() {
  stopping_.store(false, std::memory_order_release);
  for (auto& w : workers_) {
    w->Start();
  }
}

void ProactiveThreadPool::Stop() {
  bool expected = false;
  if (!stopping_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
    return;
  }

  coord_.WakeAll();
  for (auto& w : workers_) {
    w->Wake();
  }
  for (auto& w : workers_) {
    w->Join();
  }
}

void ProactiveThreadPool::Submit(task::TaskBase* t, task::SchedulingHint hint) {
  assert(t != nullptr);

  tasks_in_system_.fetch_add(1, std::memory_order_release);

  if (hint == task::SchedulingHint::Next) {
    stats_.submits_next.fetch_add(1, std::memory_order_relaxed);
  } else if (hint == task::SchedulingHint::Yield) {
    stats_.submits_yield.fetch_add(1, std::memory_order_relaxed);
  }

  if (tls_pool_ == this && tls_worker_ != nullptr) {
    stats_.submits_internal.fetch_add(1, std::memory_order_relaxed);
    tls_worker_->Push(t, hint);
  } else {
    stats_.submits_external.fetch_add(1, std::memory_order_relaxed);
    global_.PushOne(t);
  }

  coord_.NotifyOnSubmit();
}

bool ProactiveThreadPool::Here() const {
  return tls_pool_ == this;
}

ProactiveThreadPool::Stats ProactiveThreadPool::GetStats() const {
  Stats s;
  s.picked_lifo = stats_.picked_lifo.load(std::memory_order_relaxed);
  s.picked_local = stats_.picked_local.load(std::memory_order_relaxed);
  s.picked_global = stats_.picked_global.load(std::memory_order_relaxed);
  s.picked_steal = stats_.picked_steal.load(std::memory_order_relaxed);

  s.global_poll_61 = stats_.global_poll_61.load(std::memory_order_relaxed);

  s.steals_attempted = stats_.steals_attempted.load(std::memory_order_relaxed);
  s.steals_succeeded = stats_.steals_succeeded.load(std::memory_order_relaxed);

  s.offload_calls = stats_.offload_calls.load(std::memory_order_relaxed);
  s.offloaded_tasks = stats_.offloaded_tasks.load(std::memory_order_relaxed);

  s.grabbed_from_global_batches =
      stats_.grabbed_from_global_batches.load(std::memory_order_relaxed);
  s.grabbed_from_global_tasks =
      stats_.grabbed_from_global_tasks.load(std::memory_order_relaxed);

  s.parks = stats_.parks.load(std::memory_order_relaxed);
  s.wakes = stats_.wakes.load(std::memory_order_relaxed);

  s.submits_external = stats_.submits_external.load(std::memory_order_relaxed);
  s.submits_internal = stats_.submits_internal.load(std::memory_order_relaxed);
  s.submits_next = stats_.submits_next.load(std::memory_order_relaxed);
  s.submits_yield = stats_.submits_yield.load(std::memory_order_relaxed);
  return s;
}

// ---------------- Scheduling ----------------

task::TaskBase* ProactiveThreadPool::PickNext(Worker& self) {
  ++self.tick_;

  if (self.tick_ % 61 == 0) {
    stats_.global_poll_61.fetch_add(1, std::memory_order_relaxed);
    if (task::TaskBase* g = global_.PopOne()) {
      stats_.picked_global.fetch_add(1, std::memory_order_relaxed);
      return g;
    }
  }

  if (self.lifo_runs_ < 16) {
    task::TaskBase* rn = self.lifo_.exchange(nullptr, std::memory_order_acq_rel);
    if (rn != nullptr) {
      ++self.lifo_runs_;
      stats_.picked_lifo.fetch_add(1, std::memory_order_relaxed);
      return rn;
    }
  } else {
    task::TaskBase* rn = self.lifo_.exchange(nullptr, std::memory_order_acq_rel);
    if (rn != nullptr) {
      self.lifo_runs_ = 0;
      if (!self.local_.TryPush(rn)) {
        OffloadLocalToGlobal(self);
        if (!self.local_.TryPush(rn)) {
          global_.PushOne(rn);
        }
      }
    }
  }

  if (task::TaskBase* t = self.local_.TryPop()) {
    stats_.picked_local.fetch_add(1, std::memory_order_relaxed);
    return t;
  }

  if (task::TaskBase* t = TryGetFromGlobal(self)) {
    return t;
  }

  if (task::TaskBase* t = TryStealBatch(self)) {
    return t;
  }

  return nullptr;
}

task::TaskBase* ProactiveThreadPool::TryGetFromGlobal(Worker& self) {
  constexpr size_t kBatch = 32;
  auto batch = global_.PopBatch(kBatch);
  if (batch.count == 0) {
    return nullptr;
  }

  stats_.grabbed_from_global_batches.fetch_add(1, std::memory_order_relaxed);
  stats_.grabbed_from_global_tasks.fetch_add(batch.count, std::memory_order_relaxed);

  if (batch.count > 1) {
    coord_.MaybeWakeOne();
  }

  task::TaskBase* first = batch.head;
  task::TaskBase* cur = first->next;
  first->next = nullptr;

  while (cur != nullptr) {
    task::TaskBase* next = cur->next;
    cur->next = nullptr;

    if (!self.local_.TryPush(cur)) {
      OffloadLocalToGlobal(self);
      if (!self.local_.TryPush(cur)) {
        global_.PushOne(cur);
      }
    }

    cur = next;
  }

  stats_.picked_global.fetch_add(1, std::memory_order_relaxed);
  return first;
}

task::TaskBase* ProactiveThreadPool::TryStealBatch(Worker& self) {
  if (threads_ <= 1) {
    return nullptr;
  }

  if (!coord_.TryEnterSpinning()) {
    return nullptr;
  }

  struct LeaveSpinningGuard {
    Coordinator& c;
    ~LeaveSpinningGuard() { c.LeaveSpinning(); }
  } guard{coord_};

  const size_t attempts = threads_ * 2;

  for (size_t i = 0; i < attempts; ++i) {
    stats_.steals_attempted.fetch_add(1, std::memory_order_relaxed);

    size_t victim = static_cast<size_t>(self.rng_() % (threads_ - 1));
    if (victim >= self.index_) {
      victim += 1;
    }

    Worker& v = *workers_[victim];

    // Safer "batch": repeat StealOne a few times.
    // This keeps correctness even if StealMany is not fully proven.
    constexpr size_t kMaxSteal = 16;
    task::TaskBase* first = nullptr;
    size_t got = 0;

    for (; got < kMaxSteal; ++got) {
      task::TaskBase* t = v.local_.StealOne();
      if (t == nullptr) {
        break;
      }
      t->next = nullptr;
      if (first == nullptr) {
        first = t;
      } else {
        if (!self.local_.TryPush(t)) {
          OffloadLocalToGlobal(self);
          if (!self.local_.TryPush(t)) {
            global_.PushOne(t);
          }
        }
      }
    }

    if (first == nullptr) {
      continue;
    }

    stats_.steals_succeeded.fetch_add(1, std::memory_order_relaxed);
    if (got > 1) {
      coord_.MaybeWakeOne();
    }

    stats_.picked_steal.fetch_add(1, std::memory_order_relaxed);
    return first;
  }

  return nullptr;
}

void ProactiveThreadPool::OffloadLocalToGlobal(Worker& self) {
  stats_.offload_calls.fetch_add(1, std::memory_order_relaxed);

  constexpr size_t kMax = Worker::kLocalCapacity / 2;
  task::TaskBase* buf[kMax];

  size_t n = self.local_.GrabHalf(buf, kMax);
  if (n == 0) {
    return;
  }

  stats_.offloaded_tasks.fetch_add(n, std::memory_order_relaxed);

  task::TaskBase* head = buf[0];
  task::TaskBase* tail = buf[0];
  head->next = nullptr;

  for (size_t i = 1; i < n; ++i) {
    buf[i]->next = nullptr;
    tail->next = buf[i];
    tail = buf[i];
  }

  global_.PushBatch(head, tail);

  constexpr size_t kWakeThreshold = 8;
  if (n >= kWakeThreshold) {
    coord_.MaybeWakeOne();
  }
}

}  // namespace exe::runtime::multi_thread

