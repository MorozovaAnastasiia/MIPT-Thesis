#include "tasks.hpp"
#include "timers.hpp"

#include <cassert>
#include <tuple>

namespace exe::runtime {

template <typename Service>
Service& Get(View v) {
  auto* s = std::get<Service*>(v);
  assert(s);
  return *s;
}

task::IScheduler& Tasks(View v) {
  return Get<task::IScheduler>(v);
}

timer::IScheduler& Timers(View v) {
  return Get<timer::IScheduler>(v);
}

}  // namespace exe::runtime
