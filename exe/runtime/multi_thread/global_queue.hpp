#pragma once

#include <cstddef>

#include <exe/runtime/task/task.hpp>
#include <exe/thread/spinlock.hpp>

namespace exe::runtime::multi_thread {

class GlobalQueue {
 public:
  void PushOne(task::TaskBase* t) {
    Guard g(lock_);
    t->next = nullptr;
    if (!tail_) {
      head_ = tail_ = t;
    } else {
      tail_->next = t;
      tail_ = t;
    }
  }

  void PushBatch(task::TaskBase* head, task::TaskBase* tail) {
    if (!head) return;
    Guard g(lock_);
    if (!head_) {
      head_ = head;
      tail_ = tail;
    } else {
      tail_->next = head;
      tail_ = tail;
    }
  }

  task::TaskBase* PopOne() {
    Guard g(lock_);
    if (!head_) return nullptr;
    task::TaskBase* t = head_;
    head_ = head_->next;
    if (!head_) tail_ = nullptr;
    t->next = nullptr;
    return t;
  }

  struct Batch {
    task::TaskBase* head{nullptr};
    task::TaskBase* tail{nullptr};
    size_t count{0};
  };

  Batch PopBatch(size_t max) {
    Batch b;
    if (max == 0) return b;

    Guard g(lock_);
    while (head_ && b.count < max) {
      task::TaskBase* t = head_;
      head_ = head_->next;
      if (!head_) tail_ = nullptr;
      t->next = nullptr;

      if (!b.head) b.head = b.tail = t;
      else { b.tail->next = t; b.tail = t; }
      ++b.count;
    }
    return b;
  }

 private:
  struct Guard {
    exe::thread::SpinLock& l;
    explicit Guard(exe::thread::SpinLock& ll) : l(ll) { l.Lock(); }
    ~Guard() { l.Unlock(); }
  };

  exe::thread::SpinLock lock_;
  task::TaskBase* head_{nullptr};
  task::TaskBase* tail_{nullptr};
};

}  // namespace exe::runtime::multi_thread
