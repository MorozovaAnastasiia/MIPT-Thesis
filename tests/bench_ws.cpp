#include <exe/runtime/multi_thread/thread_pool.hpp>
#include <exe/runtime/view.hpp>

#include <exe/runtime/submit_task.hpp>

#include <exe/thread/wait_group.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

// ---------------- helpers ----------------

using Clock = std::chrono::steady_clock;

static inline uint64_t NsSince(Clock::time_point t0) {
  return (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count();
}

static inline double Ms(uint64_t ns) {
  return (double)ns / 1e6;
}

static void PrintStats(const exe::runtime::multi_thread::ThreadPool::Stats& s) {
  std::cout
      << "picked: lifo=" << s.picked_lifo
      << " local=" << s.picked_local
      << " global=" << s.picked_global
      << " steal=" << s.picked_steal
      << "\n"
      << "steal: attempts=" << s.steals_attempted
      << " ok=" << s.steals_succeeded
      << "\n"
      << "global: poll61=" << s.global_poll_61
      << " batches=" << s.grabbed_from_global_batches
      << " tasks=" << s.grabbed_from_global_tasks
      << "\n"
      << "offload: calls=" << s.offload_calls
      << " tasks=" << s.offloaded_tasks
      << "\n"
      << "park/wake: parks=" << s.parks
      << " wakes=" << s.wakes
      << "\n"
      << "submits: ext=" << s.submits_external
      << " int=" << s.submits_internal
      << " next=" << s.submits_next
      << " yield=" << s.submits_yield
      << "\n";
}

// ---------------- workloads ----------------

// 1) Fork-Join: много независимых коротких задач
static void Workload_ForkJoin(exe::runtime::View view, int n_tasks) {
  exe::thread::WaitGroup wg;
  wg.Add(n_tasks);

  for (int i = 0; i < n_tasks; ++i) {
    exe::runtime::SubmitTask(view, [&] {
      // маленькая "работа"
      for (int k = 0; k < 200; ++k) {
        asm volatile("" ::: "memory");
      }
      wg.Done();
    });
  }

  wg.Wait();
}

// 2) Next ping-pong + параллельные global-задачи (имитация mutex/channel wakeups)
static void Workload_NextPingPong_Fairness(exe::runtime::View view, int pings, int global_tasks) {
  std::atomic<int> ping{0};
  std::atomic<int> global_done{0};

  std::function<void()> A, B;

  A = [&] {
    int x = ping.fetch_add(1, std::memory_order_relaxed) + 1;
    if (x >= pings) return;
    exe::runtime::SubmitTask(view, exe::runtime::task::SchedulingHint::Next, B);
  };

  B = [&] {
    int x = ping.fetch_add(1, std::memory_order_relaxed) + 1;
    if (x >= pings) return;
    exe::runtime::SubmitTask(view, exe::runtime::task::SchedulingHint::Next, A);
  };

  exe::runtime::SubmitTask(view, exe::runtime::task::SchedulingHint::Next, A);

  // внешний поток -> в global
  std::thread ext([&] {
    for (int i = 0; i < global_tasks; ++i) {
      exe::runtime::SubmitTask(view, [&] {
        global_done.fetch_add(1, std::memory_order_relaxed);
      });
    }
  });

  // ждём "логически": пока ping дошёл и глобальные задачи выполнены
  // (без дедлайна, потому что это бенч; но можно поставить если надо)
  while (ping.load(std::memory_order_relaxed) < pings ||
         global_done.load(std::memory_order_relaxed) < global_tasks) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  ext.join();
}

// 3) Yield storm: из воркера генерим много yield-задач, пока владелец занят
static void Workload_YieldStorm(exe::runtime::View view, int n_yields) {
  exe::thread::WaitGroup wg;
  wg.Add(n_yields + 1);

  std::atomic<bool> release{false};

  exe::runtime::SubmitTask(view, [&] {
    for (int i = 0; i < n_yields; ++i) {
      exe::runtime::SubmitTask(
          view, exe::runtime::task::SchedulingHint::Yield, [&] {
            // короткая работа
            asm volatile("" ::: "memory");
            wg.Done();
          });
    }

    // держим воркер занятым
    while (!release.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }

    wg.Done();
  });

  // ждём, пока yield-задачи в основном отработают
  // (в бенче можно просто wg.Wait(), но так мы гарантируем, что именно другие воркеры работали)
  auto deadline = Clock::now() + std::chrono::seconds(5);
  while (Clock::now() < deadline) {
    // если осталось мало — выходим раньше
    // (у WaitGroup нет метода "сколько осталось", поэтому просто подождём чуть)
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    break;
  }

  release.store(true, std::memory_order_release);
  wg.Wait();
}

// ---------------- bench runner ----------------

static void RunOne(const std::string& name,
                   exe::runtime::multi_thread::ThreadPool& pool,
                   exe::runtime::View view,
                   std::function<void()> fn) {
  // лёгкий warmup (помогает убрать cold-start эффекты)
  Workload_ForkJoin(view, 1000);

  auto t0 = Clock::now();
  fn();
  uint64_t ns = NsSince(t0);

  std::cout << "\n=== " << name << " ===\n";
  std::cout << "time: " << Ms(ns) << " ms\n";
  PrintStats(pool.GetStats());
}

int main(int argc, char** argv) {
  size_t threads = std::thread::hardware_concurrency();
  if (threads == 0) threads = 4;

  if (argc >= 2) {
    threads = (size_t)std::stoul(argv[1]);
  }

  exe::runtime::multi_thread::ThreadPool pool(threads);
  pool.Start();

  // View: (tasks scheduler, timers scheduler)
  // Если timers у тебя не нужны, можно передать nullptr (если твой View позволяет).
  // Обычно проще: View{&pool, nullptr}
  exe::runtime::View view{&pool, nullptr};

  // Подстройки объёма под машину
  const int fork_tasks = 300000;
  const int pings = 200000;
  const int global_tasks = 20000;
  const int yields = 200000;

  RunOne("ForkJoin", pool, view, [&] {
    Workload_ForkJoin(view, fork_tasks);
  });

  RunOne("NextPingPong_Fairness", pool, view, [&] {
    Workload_NextPingPong_Fairness(view, pings, global_tasks);
  });

  RunOne("YieldStorm", pool, view, [&] {
    Workload_YieldStorm(view, yields);
  });

  pool.Stop();
  return 0;
}
