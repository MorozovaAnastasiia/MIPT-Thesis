#pragma once

#include <atomic>
#include <cstddef>

#include <exe/runtime/task/task.hpp>

namespace exe::runtime::multi_thread {

// Bounded Chase–Lev deque for TaskBase*
// Owner: TryPush/TryPop
// Thieves: StealOne/StealMany
template <size_t Capacity>
class WorkStealingQueue {
 public:
  WorkStealingQueue() {
    for (size_t i = 0; i < Capacity; ++i) {
      slots_[i].store(nullptr, std::memory_order_relaxed);
    }
  }

  WorkStealingQueue(const WorkStealingQueue&) = delete;
  WorkStealingQueue& operator=(const WorkStealingQueue&) = delete;

  // Owner only
  bool TryPush(task::TaskBase* t) {
    size_t b = bottom_.load(std::memory_order_relaxed);
    size_t top = top_.load(std::memory_order_acquire);

    if (b - top >= Capacity) {
      return false;
    }

    slots_[b % Capacity].store(t, std::memory_order_relaxed);
    bottom_.store(b + 1, std::memory_order_release);
    return true;
  }

  // Owner only
  task::TaskBase* TryPop() {
    size_t b = bottom_.load(std::memory_order_relaxed);
    if (b == 0) {
      return nullptr;
    }

    b -= 1;
    bottom_.store(b, std::memory_order_relaxed);

    // Консервативно (можно потом оптимизировать)
    std::atomic_thread_fence(std::memory_order_seq_cst);

    size_t t = top_.load(std::memory_order_relaxed);
    if (t <= b) {
      task::TaskBase* task = slots_[b % Capacity].load(std::memory_order_acquire);

      if (t == b) {
        size_t expected = t;
        if (!top_.compare_exchange_strong(
                expected, t + 1,
                std::memory_order_seq_cst,
                std::memory_order_relaxed)) {
          task = nullptr;  // украли
        }
        bottom_.store(b + 1, std::memory_order_relaxed);
      }
      return task;
    } else {
      bottom_.store(t, std::memory_order_relaxed);
      return nullptr;
    }
  }

  // Thief
  task::TaskBase* StealOne() {
    size_t t = top_.load(std::memory_order_acquire);
    std::atomic_thread_fence(std::memory_order_seq_cst);
    size_t b = bottom_.load(std::memory_order_acquire);

    if (t >= b) return nullptr;

    task::TaskBase* task = slots_[t % Capacity].load(std::memory_order_acquire);
    size_t expected = t;
    if (top_.compare_exchange_strong(
            expected, t + 1,
            std::memory_order_seq_cst,
            std::memory_order_relaxed)) {
      return task;
    }
    return nullptr;
  }

    // Steal up to max tasks (claims range from top)
    size_t StealMany(task::TaskBase** out, size_t max) {
        if (max == 0) return 0;

        size_t t = top_.load(std::memory_order_acquire);
        std::atomic_thread_fence(std::memory_order_seq_cst);   // <-- ДОБАВИТЬ
        size_t b = bottom_.load(std::memory_order_acquire);

        size_t size = b - t;
        if (size == 0) return 0;

        size_t n = size / 2;
        if (n == 0) n = 1;
        if (n > max) n = max;

        size_t expected = t;
        if (!top_.compare_exchange_strong(expected, t + n,
                                            std::memory_order_seq_cst,
                                            std::memory_order_relaxed)) {
            return 0;
        }

        for (size_t i = 0; i < n; ++i) {
            out[i] = slots_[(t + i) % Capacity].load(std::memory_order_acquire);
        }
        return n;
    }

    // Owner only: снять примерно половину задач "с головы" (со стороны top)
    // и отдать их как пачку в out[0..max-1].
    // Возвращает сколько реально сняли.
    size_t GrabHalf(task::TaskBase** out, size_t max) {
    if (max == 0) return 0;

    size_t t = top_.load(std::memory_order_acquire);
    size_t b = bottom_.load(std::memory_order_acquire);
    size_t size = b - t;
    if (size <= 1) return 0;

    size_t n = size / 2;
    if (n > max) n = max;

    // owner забирает диапазон с "головы": двигаем top вперёд
    size_t expected = t;
    if (!top_.compare_exchange_strong(expected, t + n,
                                        std::memory_order_acq_rel,
                                        std::memory_order_relaxed)) {
        return 0;
    }

    for (size_t i = 0; i < n; ++i) {
        out[i] = slots_[(t + i) % Capacity].load(std::memory_order_acquire);
    }
        return n;
    }



  size_t ApproxSize() const {
    size_t t = top_.load(std::memory_order_acquire);
    size_t b = bottom_.load(std::memory_order_acquire);
    return b - t;
  }

 private:
  alignas(64) std::atomic<size_t> top_{0};
  alignas(64) std::atomic<size_t> bottom_{0};
  alignas(64) std::atomic<task::TaskBase*> slots_[Capacity];
};

}  // namespace exe::runtime::multi_thread
