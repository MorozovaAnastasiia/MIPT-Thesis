#pragma once

#include <utility>

namespace exe {

// https://go.dev/tour/flowcontrol/12

template <typename F>
class [[nodiscard]] Defer {
 public:
  explicit Defer(F&& f)
      : func_(std::move(f)) {
  }

  ~Defer() {
    func_();
  }

 private:
  F func_;
};

}  // namespace exe
