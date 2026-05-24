#pragma once

#include <cstddef>
#include <utility>

namespace exe::util {

// Узел должен быть базовым классом для T
template <typename T>
struct IntrusiveSListNode {
  T* next{nullptr};
};

// Односвязный интрузивный список с head/tail.
// Поддерживает O(1) push_back, pop_front, append.
template <typename T>
class IntrusiveSList {
 public:
  IntrusiveSList() = default;

  IntrusiveSList(const IntrusiveSList&) = delete;
  IntrusiveSList& operator=(const IntrusiveSList&) = delete;

  bool Empty() const noexcept { return head_ == nullptr; }

  T* Front() noexcept { return head_; }
  const T* Front() const noexcept { return head_; }

  // Добавить элемент в конец
  void PushBack(T* node) noexcept {
    node->next = nullptr;
    if (tail_ == nullptr) {
      head_ = tail_ = node;
    } else {
      tail_->next = node;
      tail_ = node;
    }
    ++size_;
  }

  // Забрать элемент из начала
  T* PopFront() noexcept {
    if (head_ == nullptr) {
      return nullptr;
    }
    T* node = head_;
    head_ = head_->next;
    if (head_ == nullptr) {
      tail_ = nullptr;
    }
    node->next = nullptr;
    --size_;
    return node;
  }

  // Приклеить к концу другой список (и опустошить его) — O(1)
  void Append(IntrusiveSList& other) noexcept {
    if (other.head_ == nullptr) {
      return;
    }
    if (tail_ == nullptr) {
      head_ = other.head_;
      tail_ = other.tail_;
    } else {
      tail_->next = other.head_;
      tail_ = other.tail_;
    }
    size_ += other.size_;
    other.head_ = other.tail_ = nullptr;
    other.size_ = 0;
  }

  std::size_t Size() const noexcept { return size_; }

 private:
  T* head_{nullptr};
  T* tail_{nullptr};
  std::size_t size_{0};
};

}  // namespace exe::util
