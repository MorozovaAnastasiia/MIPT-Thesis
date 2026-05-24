#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <set>
#include <thread>

#include <exe/runtime/submit_task.hpp>
#include <exe/runtime/view.hpp>
#include <exe/runtime/task/hint.hpp>

#include <exe/thread/wait_group.hpp>
#include <exe/runtime/multi_thread/thread_pool.hpp>

struct TestFailure { std::string msg; };

#define ASSERT_TRUE(cond) \
  do { if (!(cond)) throw TestFailure{std::string("ASSERT_TRUE failed: ") + #cond}; } while (0)

#define ASSERT_EQ(a, b) \
  do { auto _a = (a); auto _b = (b); if (!(_a == _b)) throw TestFailure{"ASSERT_EQ failed"}; } while (0)

#define TEST(name) void name()

static void RunTest(const char* name, void (*fn)()) {
  try { fn(); std::cout << "[ OK ] " << name << "\n"; }
  catch (const TestFailure& e) { std::cout << "[FAIL] " << name << ": " << e.msg << "\n"; std::exit(1); }
}

using exe::thread::WaitGroup;
using exe::runtime::multi_thread::ThreadPool;

static exe::runtime::View MakeView(ThreadPool& pool) {
  // timer::IScheduler* пока можно nullptr, лишь бы Timers(view) не вызывали
  return exe::runtime::View{&pool, nullptr};
}

TEST(AllTasksRun) {
  ThreadPool pool(4);
  pool.Start();
  auto view = MakeView(pool);

  constexpr int kN = 100000;
  std::atomic<int> counter{0};
  WaitGroup wg;
  wg.Add(kN);

  for (int i = 0; i < kN; ++i) {
    exe::runtime::SubmitTask(view, [&] {
      counter.fetch_add(1, std::memory_order_relaxed);
      wg.Done();
    });
  }

  wg.Wait();
  pool.Stop();
  ASSERT_EQ(counter.load(std::memory_order_relaxed), kN);
}

TEST(UsesMultipleThreads) {
  ThreadPool pool(4);
  pool.Start();
  auto view = MakeView(pool);

  constexpr int kN = 200000;
  WaitGroup wg;
  wg.Add(kN);

  std::mutex m;
  std::set<std::thread::id> seen;

  exe::runtime::SubmitTask(view, [&] {
    for (int i = 0; i < kN; ++i) {
      exe::runtime::SubmitTask(view, [&] {
        { std::lock_guard<std::mutex> g(m); seen.insert(std::this_thread::get_id()); }
        wg.Done();
      });
    }
  });

  wg.Wait();
  pool.Stop();
  ASSERT_TRUE(seen.size() >= 2);
}

TEST(YieldGoesToGlobalAndRunsWhileOwnerBlocked) {
  ThreadPool pool(2);
  pool.Start();
  auto view = MakeView(pool);

  constexpr int kN = 20000;
  std::atomic<int> done{0};
  std::atomic<bool> release{false};

  WaitGroup wg;
  wg.Add(kN + 1); // +1 за блокирующую задачу

  // Эта задача займёт один воркер и будет ждать release
  exe::runtime::SubmitTask(view, [&] {
    for (int i = 0; i < kN; ++i) {
      exe::runtime::SubmitTask(view, exe::runtime::task::SchedulingHint::Yield, [&] {
        done.fetch_add(1, std::memory_order_relaxed);
        wg.Done();
      });
    }

    // держим воркер занятым
    while (!release.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    wg.Done();
  });

  // Ждём, что yield-задачи выполнятся ДО release (значит их исполнил другой воркер)
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (done.load(std::memory_order_relaxed) < kN &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  ASSERT_EQ(done.load(std::memory_order_relaxed), kN);

  release.store(true, std::memory_order_release);
  wg.Wait();

  pool.Stop();
}

TEST(NoLostWakeups_Stress) {
  ThreadPool pool(4);
  pool.Start();
  auto view = MakeView(pool);

  constexpr int kRounds = 5000;
  for (int i = 0; i < kRounds; ++i) {
    WaitGroup wg;
    wg.Add(1);

    // даём воркерам шанс уйти в park
    std::this_thread::sleep_for(std::chrono::microseconds(50));

    exe::runtime::SubmitTask(view, [&] {
      wg.Done();
    });

    wg.Wait();
  }

  pool.Stop();
  ASSERT_TRUE(true);
}

TEST(StopWaitsForTasks) {
  ThreadPool pool(4);
  pool.Start();
  auto view = MakeView(pool);

  constexpr int kN = 5000;
  std::atomic<int> started{0};
  std::atomic<int> finished{0};

  WaitGroup wg;
  wg.Add(kN);

  for (int i = 0; i < kN; ++i) {
    exe::runtime::SubmitTask(view, [&] {
      started.fetch_add(1, std::memory_order_relaxed);

      // небольшая работа, чтобы Stop попадал "во время"
      for (int j = 0; j < 2000; ++j) {
        asm volatile("" ::: "memory");
      }

      finished.fetch_add(1, std::memory_order_relaxed);
      wg.Done();
    });
  }

  // вызываем stop пока задачи ещё выполняются
  pool.Stop();

  // Stop должен был дождаться завершения всех задач
  ASSERT_EQ(finished.load(std::memory_order_relaxed), kN);
}

TEST(GlobalDoesNotStarve) {
  ThreadPool pool(4);
  pool.Start();
  auto view = MakeView(pool);

  std::atomic<bool> stop_spam{false};
  std::atomic<int> global_done{0};

  // Локальный спам, но с троттлингом, чтобы не плодить миллионы задач
  exe::runtime::SubmitTask(view, [&] {
    while (!stop_spam.load(std::memory_order_acquire)) {
      exe::runtime::SubmitTask(view, [] {
        asm volatile("" ::: "memory");
      });

      // throttle: не даём бесконечно накапливать очередь
      for (int k = 0; k < 64; ++k) {
        std::this_thread::yield();
      }
    }
  });

  constexpr int kGlobal = 2000;

  std::thread external([&] {
    for (int i = 0; i < kGlobal; ++i) {
      exe::runtime::SubmitTask(view, [&] {
        global_done.fetch_add(1, std::memory_order_relaxed);
      });
    }
  });

  // Дедлайн делаем щедрее: это тест fairness, не бенчмарк
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (global_done.load(std::memory_order_relaxed) < kGlobal &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  stop_spam.store(true, std::memory_order_release);
  external.join();

  pool.Stop();

  ASSERT_EQ(global_done.load(std::memory_order_relaxed), kGlobal);
}

TEST(BurstAfterSleepUsesMultipleThreads) {
  ThreadPool pool(4);
  pool.Start();
  auto view = MakeView(pool);

  // даём воркерам шанс реально заснуть
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  constexpr int kN = 200000;
  exe::thread::WaitGroup wg;
  wg.Add(kN);

  std::mutex m;
  std::set<std::thread::id> seen;

  for (int i = 0; i < kN; ++i) {
    exe::runtime::SubmitTask(view, [&] {
      {
        std::lock_guard<std::mutex> g(m);
        seen.insert(std::this_thread::get_id());
      }
      wg.Done();
    });
  }

  wg.Wait();
  pool.Stop();

  // Если каскада нет, иногда бывает "долго раскачивается".
  // С каскадом обычно видим >=2 потока стабильно.
  ASSERT_TRUE(seen.size() >= 2);
}

TEST(OffloadDoesNotLoseTasks) {
  ThreadPool pool(4);
  pool.Start();
  auto view = MakeView(pool);

  constexpr int kN = 200000;
  std::atomic<int> cnt{0};
  WaitGroup wg;
  wg.Add(kN);

  for (int i = 0; i < kN; ++i) {
    exe::runtime::SubmitTask(view, [&]{
      cnt.fetch_add(1, std::memory_order_relaxed);
      wg.Done();
    });
  }

  wg.Wait();
  pool.Stop();
  ASSERT_EQ(cnt.load(std::memory_order_relaxed), kN);
}

TEST(NextPingPongDoesNotStarveGlobal) {
  ThreadPool pool(2);
  pool.Start();
  auto view = MakeView(pool);

  std::atomic<int> ping{0};
  std::atomic<int> global_done{0};
  constexpr int kPings = 20000;
  constexpr int kGlobal = 2000;

  std::function<void()> A, B;

  A = [&] {
    int x = ping.fetch_add(1, std::memory_order_relaxed) + 1;
    if (x >= kPings) return;
    exe::runtime::SubmitTask(view, exe::runtime::task::SchedulingHint::Next, B);
  };

  B = [&] {
    int x = ping.fetch_add(1, std::memory_order_relaxed) + 1;
    if (x >= kPings) return;
    exe::runtime::SubmitTask(view, exe::runtime::task::SchedulingHint::Next, A);
  };

  exe::runtime::SubmitTask(view, exe::runtime::task::SchedulingHint::Next, A);

  std::thread ext([&] {
    for (int i = 0; i < kGlobal; ++i) {
      exe::runtime::SubmitTask(view, [&] {
        global_done.fetch_add(1, std::memory_order_relaxed);
      });
    }
  });

  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while ((ping.load(std::memory_order_relaxed) < kPings ||
          global_done.load(std::memory_order_relaxed) < kGlobal) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  ext.join();
  pool.Stop();

  ASSERT_TRUE(ping.load(std::memory_order_relaxed) >= kPings);
  ASSERT_EQ(global_done.load(std::memory_order_relaxed), kGlobal);
}


int main() {
  RunTest("AllTasksRun", &AllTasksRun);
  RunTest("UsesMultipleThreads", &UsesMultipleThreads);
  RunTest("YieldGoesToGlobalAndRunsWhileOwnerBlocked", &YieldGoesToGlobalAndRunsWhileOwnerBlocked);
  RunTest("NoLostWakeups_Stress", &NoLostWakeups_Stress);
  RunTest("GlobalDoesNotStarve", &GlobalDoesNotStarve);
  RunTest("BurstAfterSleepUsesMultipleThreads", &BurstAfterSleepUsesMultipleThreads);
  RunTest("OffloadDoesNotLoseTasks", &OffloadDoesNotLoseTasks);
  RunTest("NextPingPongDoesNotStarveGlobal", &NextPingPongDoesNotStarveGlobal);
  std::cout << "All tests passed.\n";
  return 0;
}
