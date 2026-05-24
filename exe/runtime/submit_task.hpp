#pragma once

#include <exe/runtime/view/tasks.hpp>
#include <exe/runtime/task/hint.hpp>
#include <exe/runtime/task/task.hpp>

#include <type_traits>
#include <utility>
#include <exception>

namespace exe::runtime {

namespace detail {

template <typename F>
struct FnTask final : task::TaskBase {
  F f;

  explicit FnTask(F&& ff) : f(std::move(ff)) {}

  void Run() noexcept override {
    try {
      f();
    } catch (...) {
      std::terminate();
    }
    delete this;
  }
};

}  // namespace detail

//версия с hint
template <typename F>
void SubmitTask(View rt, task::SchedulingHint hint, F&& func) {
  using Fn = std::decay_t<F>;
  auto* node = new detail::FnTask<Fn>(Fn(std::forward<F>(func)));
  Tasks(rt).Submit(node, hint);
}

// версия без hint
template <typename F>
void SubmitTask(View rt, F&& func) {
  SubmitTask(rt, task::SchedulingHint::UpToYou, std::forward<F>(func));
}

}  // namespace exe::runtime
