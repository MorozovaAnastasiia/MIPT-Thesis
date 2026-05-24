#pragma once
#include <exe/util/intrusive_slist.hpp>

namespace exe::runtime::task {

struct ITask {
  virtual void Run() noexcept = 0;
 protected:
  ~ITask() = default;
};

struct TaskBase : ITask, exe::util::IntrusiveSListNode<TaskBase> {
  // next уже есть через базу
};

}  // namespace exe::runtime::task
