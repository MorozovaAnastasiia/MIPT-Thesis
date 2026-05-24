#pragma once

namespace exe::runtime::task {

enum class SchedulingHint {
  UpToYou = 1,
  Next = 2,
  Yield = 3,
};

}  // namespace exe::runtime::task
