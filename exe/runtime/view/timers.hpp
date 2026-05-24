#pragma once

#include <exe/runtime/view.hpp>
#include <exe/runtime/timer/fwd.hpp>

namespace exe::runtime {
timer::IScheduler& Timers(View v);
}  // namespace exe::runtime
