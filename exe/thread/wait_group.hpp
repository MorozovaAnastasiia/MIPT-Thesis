#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>

namespace exe::thread {

class WaitGroup {
 public:
  void Add(int delta) {
    int old = counter_.fetch_add(delta, std::memory_order_acq_rel);
    int now = old + delta;

    if (now == 0) {
      // Будим всех, кто ждет
      std::lock_guard<std::mutex> guard(m_);
      cv_.notify_all();
    }
  }

  void Done() {
    Add(-1);
  }

  void Wait() {
    if (counter_.load(std::memory_order_acquire) == 0) {
      return;
    }

    std::unique_lock<std::mutex> lock(m_);
    cv_.wait(lock, [&] {
      return counter_.load(std::memory_order_acquire) == 0;
    });
  }

 private:
    std::atomic<int> counter_{0};
    std::mutex m_;
    std::condition_variable cv_;
};

}  // namespace exe::thread
