#pragma once

#include <atomic>
#include "cpu_relax.hpp"

namespace exe::thread {

class SpinLock {
 public:
  void Lock() {
    while (locked_.exchange(true, std::memory_order_acquire)) {
      while (locked_.load(std::memory_order_relaxed)) {
        CpuRelax();
      }
    }
  }

  bool TryLock() {
    bool expected = false;
    return locked_.compare_exchange_strong(
        expected, true,
        std::memory_order_acquire,
        std::memory_order_relaxed);
  }

  void Unlock() {
    locked_.store(false, std::memory_order_release);
  }

  // Lockable
  void lock() { Lock(); }              // NOLINT
  bool try_lock() { return TryLock(); } // NOLINT
  void unlock() { Unlock(); }          // NOLINT

 private:
  std::atomic<bool> locked_{false};
};

}  // namespace exe::thread
