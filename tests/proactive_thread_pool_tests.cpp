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
#include <exe/runtime/multi_thread/proactive_thread_pool.hpp>

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
using exe::runtime::multi_thread::ProactiveThreadPool;

static exe::runtime::View MakeView(ProactiveThreadPool& pool) {
  return exe::runtime::View{&pool, nullptr};
}

TEST(AllTasksRun) {
  ProactiveThreadPool pool(4);
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
  ProactiveThreadPool pool(4);
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

TEST(StopWaitsForTasks) {
  ProactiveThreadPool pool(4);
  pool.Start();
  auto view = MakeView(pool);

  constexpr int kN = 5000;
  std::atomic<int> finished{0};
  WaitGroup wg;
  wg.Add(kN);

  for (int i = 0; i < kN; ++i) {
    exe::runtime::SubmitTask(view, [&] {
      for (int j = 0; j < 2000; ++j) {
        asm volatile("" ::: "memory");
      }
      finished.fetch_add(1, std::memory_order_relaxed);
      wg.Done();
    });
  }

  pool.Stop();
  ASSERT_EQ(finished.load(std::memory_order_relaxed), kN);
}

int main() {
  RunTest("AllTasksRun", &AllTasksRun);
  RunTest("UsesMultipleThreads", &UsesMultipleThreads);
  RunTest("StopWaitsForTasks", &StopWaitsForTasks);
  std::cout << "All tests passed.\n";
  return 0;
}

