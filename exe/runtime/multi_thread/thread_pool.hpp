#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <random>
#include <thread>
#include <vector>

#include <exe/runtime/task/scheduler.hpp>
#include <exe/runtime/task/hint.hpp>
#include <exe/runtime/task/task.hpp>

#include <exe/thread/event.hpp>      // у тебя Event (std::atomic::wait) или аналог
#include <exe/thread/spinlock.hpp>   // твой SpinLock

#include "global_queue.hpp"
#include "ws_queue.hpp"

namespace exe::runtime::multi_thread {

class ThreadPool final : public task::IScheduler {
 public:
  struct Stats {
    uint64_t picked_lifo = 0;
    uint64_t picked_local = 0;
    uint64_t picked_global = 0;
    uint64_t picked_steal = 0;

    uint64_t global_poll_61 = 0;

    uint64_t steals_attempted = 0;
    uint64_t steals_succeeded = 0;

    uint64_t offload_calls = 0;
    uint64_t offloaded_tasks = 0;

    uint64_t grabbed_from_global_batches = 0;
    uint64_t grabbed_from_global_tasks = 0;

    uint64_t parks = 0;
    uint64_t wakes = 0;

    uint64_t submits_external = 0;
    uint64_t submits_internal = 0;
    uint64_t submits_next = 0;
    uint64_t submits_yield = 0;
  };

  Stats GetStats() const;

  explicit ThreadPool(size_t threads);
  ~ThreadPool();

  void Start();
  void Stop();

  // IScheduler
  void Submit(task::TaskBase* t, task::SchedulingHint hint) override;

  bool Here() const;

 private:

  struct AtomicStats {
    std::atomic<uint64_t> picked_lifo{0};
    std::atomic<uint64_t> picked_local{0};
    std::atomic<uint64_t> picked_global{0};
    std::atomic<uint64_t> picked_steal{0};

    std::atomic<uint64_t> global_poll_61{0};

    std::atomic<uint64_t> steals_attempted{0};
    std::atomic<uint64_t> steals_succeeded{0};

    std::atomic<uint64_t> offload_calls{0};
    std::atomic<uint64_t> offloaded_tasks{0};

    std::atomic<uint64_t> grabbed_from_global_batches{0};
    std::atomic<uint64_t> grabbed_from_global_tasks{0};

    std::atomic<uint64_t> parks{0};
    std::atomic<uint64_t> wakes{0};

    std::atomic<uint64_t> submits_external{0};
    std::atomic<uint64_t> submits_internal{0};
    std::atomic<uint64_t> submits_next{0};
    std::atomic<uint64_t> submits_yield{0};
  };

  AtomicStats stats_;

  struct Worker;

  class Coordinator {
   public:
    explicit Coordinator(size_t workers)
        : max_spinners_(workers > 1 ? (workers + 1) / 2 : 1) {}

    void NotifyOnSubmit();
    void AddSleeper(Worker* w);
    void CancelOneSleep();

    void MaybeWakeOne() {
        // fast-path: если никто не спит, не трогаем lock/список
        if (sleeping_.load(std::memory_order_acquire) == 0) {
            return;
        }
        NotifyOnSubmit();  // разбудит максимум одного реального sleeper-а
    }


    bool TryEnterSpinning();
    void CancelSleepCount();
    void LeaveSpinning();
    void WakeAll();

   private:
    exe::thread::SpinLock lock_;
    Worker* sleepers_head_{nullptr};
    std::atomic<size_t> sleeping_{0};

    const size_t max_spinners_;
    std::atomic<size_t> spinners_{0};
  };

  struct Worker {
    explicit Worker(ThreadPool* pool, size_t index);

    void Start();
    void Join();

    void Wake();
    bool TryWakeByCoordinator();
    bool TryCancelSleepBySelf();

    // push with hint handling (Next -> LIFO slot)
    void Push(task::TaskBase* t, task::SchedulingHint hint);

    void Run();

    Worker* sleep_next_{nullptr};

    ThreadPool* pool_{nullptr};
    const size_t index_{0};

    std::atomic<task::TaskBase*> lifo_{nullptr};
    uint32_t tick_{0};
    uint32_t lifo_runs_{0};

    static constexpr size_t kLocalCapacity = 256;
    WorkStealingQueue<kLocalCapacity> local_;

    exe::thread::Event park_event_;   // твой event (на std::atomic::wait)
    std::mt19937_64 rng_;
    std::thread thread_;
    std::atomic<uint8_t> sleep_state_{0}; // 0 = awake, 1 = registered-as-sleeper
  };

  task::TaskBase* PickNext(Worker& self);
  task::TaskBase* TryGetFromGlobal(Worker& self);
  task::TaskBase* TrySteal(Worker& self);

  void OffloadLocalToGlobal(Worker& self);

  static thread_local ThreadPool* tls_pool_;
  static thread_local Worker* tls_worker_;

  const size_t threads_;
  std::vector<std::unique_ptr<Worker>> workers_;

  Coordinator coord_;
  GlobalQueue global_;

  std::atomic<bool> stopping_{false};
  std::atomic<size_t> tasks_in_system_{0}; // количество задач в системе
};

}  // namespace exe::runtime::multi_thread
