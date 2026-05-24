#pragma once

#include <atomic>
#include <cstdint>

namespace exe::thread {

// Счетчик-событие: Park ждет смены epoch, Wake увеличивает epoch и будит.
class Event {
 public:
  // Возвращает текущую "версию" события (epoch)
  uint32_t Epoch() const noexcept {
    return epoch_.load(std::memory_order_acquire);
  }

  // Заснуть, пока epoch_ == expected (спуриос-вейкапы возможны — это нормально)
  void Park(uint32_t expected) noexcept {
    epoch_.wait(expected, std::memory_order_relaxed);
  }

  void WakeOne() noexcept {
    epoch_.fetch_add(1, std::memory_order_release);
    epoch_.notify_one();
  }

  void WakeAll() noexcept {
    epoch_.fetch_add(1, std::memory_order_release);
    epoch_.notify_all();
  }

 private:
  std::atomic<uint32_t> epoch_{0};
};

}  // namespace exe::thread
