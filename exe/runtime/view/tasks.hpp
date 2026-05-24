#pragma once

#include <exe/runtime/view.hpp>
#include <exe/runtime/task/scheduler.hpp>

namespace exe::runtime {

// Достаёт task scheduler из View (tuple)
task::IScheduler& Tasks(View v);

}  // namespace exe::runtime
