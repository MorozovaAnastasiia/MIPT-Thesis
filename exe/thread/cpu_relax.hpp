#pragma once

#include <thread>

#if defined(__x86_64__) || defined(_M_X64) || defined(__i386) || defined(_M_IX86)
  #include <immintrin.h>
#endif

namespace exe::thread {

inline void CpuRelax() {
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386) || defined(_M_IX86)
  _mm_pause();
#else
  // На не-x86 можно сделать yield (чуть дороже, но корректно)
  std::this_thread::yield();
#endif
}

}  // namespace exe::thread
