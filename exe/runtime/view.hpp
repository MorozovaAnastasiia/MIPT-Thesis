#pragma once

#include <exe/runtime/task/fwd.hpp>
#include <exe/runtime/timer/fwd.hpp>

#include <tuple>

namespace exe::runtime {

using View = std::tuple<
    task::IScheduler*,
    timer::IScheduler*
>;

}  // namespace exe::runtime
