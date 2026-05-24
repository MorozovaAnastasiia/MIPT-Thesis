#include <exe/runtime/multi_thread/thread_pool.hpp>
#include <exe/runtime/multi_thread/proactive_thread_pool.hpp>
#include <exe/runtime/view.hpp>
#include <exe/runtime/submit_task.hpp>
#include <exe/thread/wait_group.hpp>

#include <atomic>
#include <chrono>
#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using Clock = std::chrono::steady_clock;

static inline uint64_t NsSince(Clock::time_point t0) {
  return (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count();
}

static inline double Ms(uint64_t ns) {
  return (double)ns / 1e6;
}

static inline double Us(uint64_t ns) {
  return (double)ns / 1e3;
}

static inline void BusyWork(int iters) {
  for (int k = 0; k < iters; ++k) {
    asm volatile("" ::: "memory");
  }
}

struct LatencySummary {
  double p50_us = 0;
  double p90_us = 0;
  double p99_us = 0;
  double max_us = 0;
  size_t samples = 0;
};

static LatencySummary SummarizeLatenciesUs(std::vector<uint64_t>& ns) {
  LatencySummary s;
  s.samples = ns.size();
  if (ns.empty()) return s;
  std::sort(ns.begin(), ns.end());

  auto pct = [&](double p) -> double {
    if (ns.empty()) return 0;
    size_t idx = (size_t)((p * (double)(ns.size() - 1)));
    return Us(ns[idx]);
  };

  s.p50_us = pct(0.50);
  s.p90_us = pct(0.90);
  s.p99_us = pct(0.99);
  s.max_us = Us(ns.back());
  return s;
}

// ---------------- workloads ----------------

// A) Many independent tasks (fork-join)
static void Workload_ForkJoin(exe::runtime::View view, int n_tasks, int work_iters) {
  exe::thread::WaitGroup wg;
  wg.Add(n_tasks);
  for (int i = 0; i < n_tasks; ++i) {
    exe::runtime::SubmitTask(view, [&] {
      BusyWork(work_iters);
      wg.Done();
    });
  }
  wg.Wait();
}

// A2) Same fork-join but samples per-task latency for a subset.
static LatencySummary Workload_ForkJoin_LatencySample(exe::runtime::View view,
                                                      int n_tasks,
                                                      int work_iters,
                                                      int sample_tasks) {
  if (sample_tasks <= 0) {
    Workload_ForkJoin(view, n_tasks, work_iters);
    return {};
  }

  sample_tasks = std::min(sample_tasks, n_tasks);
  std::vector<uint64_t> lat_ns((size_t)sample_tasks);
  std::atomic<int> idx{0};

  exe::thread::WaitGroup wg;
  wg.Add(n_tasks);
  for (int i = 0; i < n_tasks; ++i) {
    const auto start = Clock::now();
    exe::runtime::SubmitTask(view, [&, start] {
      BusyWork(work_iters);
      int j = idx.fetch_add(1, std::memory_order_relaxed);
      if (j < sample_tasks) {
        lat_ns[(size_t)j] = NsSince(start);
      }
      wg.Done();
    });
  }
  wg.Wait();

  // Only first sample_tasks entries are set, order doesn't matter for percentiles.
  return SummarizeLatenciesUs(lat_ns);
}

// B) Future-heavy DAG: level graph, tasks become runnable after indegree hits 0
static void Workload_DAG_LevelGraph(exe::runtime::View view,
                                    int levels,
                                    int width,
                                    int out_degree,
                                    int work_iters,
                                    uint64_t seed) {
  const int n = levels * width;
  if (n == 0) return;

  std::mt19937_64 rng(seed);

  std::vector<std::vector<int>> edges(n);
  std::vector<std::atomic<int>> indeg(n);
  for (int i = 0; i < n; ++i) indeg[i].store(0, std::memory_order_relaxed);

  // Connect only forward across levels.
  // First, guarantee every node in level (lvl+1) has at least one incoming edge.
  for (int lvl = 0; lvl < levels - 1; ++lvl) {
    for (int vw = 0; vw < width; ++vw) {
      int u = lvl * width + (vw % width);
      int v = (lvl + 1) * width + vw;
      edges[u].push_back(v);
    }
  }

  // Then add extra random edges to increase fan-in/out.
  const int extra = out_degree > 1 ? (out_degree - 1) : 0;
  for (int lvl = 0; lvl < levels - 1; ++lvl) {
    for (int w = 0; w < width; ++w) {
      int u = lvl * width + w;
      edges[u].reserve((size_t)(edges[u].size() + (size_t)extra));
      for (int k = 0; k < extra; ++k) {
        int v_w = (int)(rng() % (uint64_t)width);
        int v = (lvl + 1) * width + v_w;
        edges[u].push_back(v);
      }
    }
  }

  for (int u = 0; u < n; ++u) {
    for (int v : edges[u]) {
      indeg[v].fetch_add(1, std::memory_order_relaxed);
    }
  }

  exe::thread::WaitGroup wg;
  wg.Add(n);

  struct Runner {
    exe::runtime::View view;
    int work_iters;
    std::vector<std::vector<int>>* edges;
    std::vector<std::atomic<int>>* indeg;
    exe::thread::WaitGroup* wg;

    void Submit(int node) const {
      exe::runtime::SubmitTask(view, [self = *this, node] {
        BusyWork(self.work_iters);
        for (int v : (*self.edges)[node]) {
          int left = (*self.indeg)[v].fetch_sub(1, std::memory_order_acq_rel) - 1;
          if (left == 0) {
            self.Submit(v);
          }
        }
        self.wg->Done();
      });
    }
  };

  const Runner runner{view, work_iters, &edges, &indeg, &wg};

  // submit all roots (indegree 0)
  for (int i = 0; i < n; ++i) {
    if (indeg[i].load(std::memory_order_relaxed) == 0) {
      runner.Submit(i);
    }
  }

  wg.Wait();
}

// E) Skewed DAG: one heavy root produces many light leaves (shows imbalance)
static void Workload_SkewedDAG_HeavyRoot(exe::runtime::View view,
                                        int leaves,
                                        int heavy_work_iters,
                                        int leaf_work_iters) {
  exe::thread::WaitGroup wg;
  wg.Add(leaves + 1);

  exe::runtime::SubmitTask(view, [&] {
    BusyWork(heavy_work_iters);
    for (int i = 0; i < leaves; ++i) {
      exe::runtime::SubmitTask(view, [&] {
        BusyWork(leaf_work_iters);
        wg.Done();
      });
    }
    wg.Done();
  });

  wg.Wait();
}

// C) Long critical path + fan-out: each step spawns side tasks and one continuation
static void Workload_CriticalPath_FanOut(exe::runtime::View view,
                                         int chain_len,
                                         int fanout,
                                         int side_work_iters,
                                         int chain_work_iters) {
  exe::thread::WaitGroup wg;
  wg.Add(chain_len * (fanout + 1));

  std::function<void(int)> step;
  step = [&](int i) {
    // side work should be stealable
    for (int k = 0; k < fanout; ++k) {
      exe::runtime::SubmitTask(view, [&] {
        BusyWork(side_work_iters);
        wg.Done();
      });
    }

    // continuation prefers to stay hot on the same worker
    exe::runtime::SubmitTask(view, exe::runtime::task::SchedulingHint::Next, [&, i] {
      BusyWork(chain_work_iters);
      wg.Done();
      if (i + 1 < chain_len) {
        step(i + 1);
      }
    });
  };

  step(0);
  wg.Wait();
}

// D) Pipeline / async-RPC pattern: items pass through stages, we Yield between stages
static void Workload_Pipeline_Yield(exe::runtime::View view,
                                   int items,
                                   int stages,
                                   int stage_work_iters) {
  exe::thread::WaitGroup wg;
  wg.Add(items * stages);

  for (int it = 0; it < items; ++it) {
    struct Chain {
      exe::runtime::View view;
      exe::thread::WaitGroup* wg;
      int stages;
      int work;
      std::function<void(int)> stage;
    };

    auto chain = std::make_shared<Chain>();
    chain->view = view;
    chain->wg = &wg;
    chain->stages = stages;
    chain->work = stage_work_iters;

    chain->stage = [chain](int s) {
      exe::runtime::SubmitTask(chain->view, exe::runtime::task::SchedulingHint::Yield, [chain, s] {
        BusyWork(chain->work);
        chain->wg->Done();
        if (s + 1 < chain->stages) {
          chain->stage(s + 1);
        }
      });
    };

    chain->stage(0);
  }

  wg.Wait();
}

// D2) Pipeline + end-to-end per-item latency
static LatencySummary Workload_Pipeline_Latency(exe::runtime::View view,
                                                int items,
                                                int stages,
                                                int stage_work_iters) {
  std::vector<uint64_t> lat_ns((size_t)items);
  std::vector<Clock::time_point> t0((size_t)items);
  for (int i = 0; i < items; ++i) t0[(size_t)i] = Clock::now();

  exe::thread::WaitGroup wg;
  wg.Add(items * stages);

  for (int it = 0; it < items; ++it) {
    struct Chain {
      exe::runtime::View view;
      exe::thread::WaitGroup* wg;
      int stages;
      int work;
      int item;
      std::vector<uint64_t>* lat_ns;
      std::vector<Clock::time_point>* t0;
      std::function<void(int)> stage;
    };

    auto chain = std::make_shared<Chain>();
    chain->view = view;
    chain->wg = &wg;
    chain->stages = stages;
    chain->work = stage_work_iters;
    chain->item = it;
    chain->lat_ns = &lat_ns;
    chain->t0 = &t0;

    chain->stage = [chain](int s) {
      exe::runtime::SubmitTask(chain->view, exe::runtime::task::SchedulingHint::Yield, [chain, s] {
        BusyWork(chain->work);
        chain->wg->Done();
        if (s + 1 < chain->stages) {
          chain->stage(s + 1);
        } else {
          (*chain->lat_ns)[(size_t)chain->item] =
              (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(
                  Clock::now() - (*chain->t0)[(size_t)chain->item]).count();
        }
      });
    };

    chain->stage(0);
  }

  wg.Wait();
  return SummarizeLatenciesUs(lat_ns);
}

// F) Bursty arrivals: wait for workers to sleep, then submit burst and measure completion latency.
static LatencySummary Workload_BurstyArrivals(exe::runtime::View view,
                                              int burst_tasks,
                                              int work_iters) {
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  std::vector<uint64_t> lat_ns((size_t)burst_tasks);
  std::vector<Clock::time_point> t0((size_t)burst_tasks);
  for (int i = 0; i < burst_tasks; ++i) t0[(size_t)i] = Clock::now();

  exe::thread::WaitGroup wg;
  wg.Add(burst_tasks);
  for (int i = 0; i < burst_tasks; ++i) {
    exe::runtime::SubmitTask(view, [&, i] {
      BusyWork(work_iters);
      lat_ns[(size_t)i] =
          (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0[(size_t)i]).count();
      wg.Done();
    });
  }

  wg.Wait();
  return SummarizeLatenciesUs(lat_ns);
}

// G) Single producer flood: one worker generates many tiny tasks.
// This creates strong steal pressure and amplifies differences in steal strategy.
static LatencySummary Workload_SingleProducerStealPressure(exe::runtime::View view,
                                                           int tasks,
                                                           int task_work_iters,
                                                           int producer_pause_iters,
                                                           int sample_tasks) {
  if (tasks <= 0) {
    return {};
  }

  sample_tasks = std::min(sample_tasks, tasks);
  std::vector<uint64_t> lat_ns((size_t)sample_tasks);
  std::atomic<int> sample_idx{0};

  exe::thread::WaitGroup wg;
  wg.Add(tasks + 1);  // + producer

  exe::runtime::SubmitTask(view, [=, &wg, &lat_ns, &sample_idx] {
    for (int i = 0; i < tasks; ++i) {
      const auto start = Clock::now();
      exe::runtime::SubmitTask(view, [&, start] {
        BusyWork(task_work_iters);
        int j = sample_idx.fetch_add(1, std::memory_order_relaxed);
        if (j < sample_tasks) {
          lat_ns[(size_t)j] = NsSince(start);
        }
        wg.Done();
      });

      if (producer_pause_iters > 0) {
        BusyWork(producer_pause_iters);
      }
    }
    wg.Done();
  });

  wg.Wait();
  return SummarizeLatenciesUs(lat_ns);
}

// H) Periodic micro-bursts from an external producer thread.
// Repeated wake/sleep transitions often stress parking and wake-cascade logic.
static LatencySummary Workload_PeriodicMicroBursts(exe::runtime::View view,
                                                   int bursts,
                                                   int burst_tasks,
                                                   int gap_us,
                                                   int work_iters) {
  if (bursts <= 0 || burst_tasks <= 0) {
    return {};
  }

  const int total = bursts * burst_tasks;
  std::vector<uint64_t> lat_ns((size_t)total);
  std::vector<Clock::time_point> t0((size_t)total);

  exe::thread::WaitGroup wg;
  wg.Add(total);

  std::thread ext([&] {
    int idx = 0;
    for (int b = 0; b < bursts; ++b) {
      for (int i = 0; i < burst_tasks; ++i) {
        const int id = idx++;
        t0[(size_t)id] = Clock::now();
        exe::runtime::SubmitTask(view, [&, id] {
          BusyWork(work_iters);
          lat_ns[(size_t)id] =
              (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(
                  Clock::now() - t0[(size_t)id]).count();
          wg.Done();
        });
      }
      if (gap_us > 0) {
        std::this_thread::sleep_for(std::chrono::microseconds(gap_us));
      }
    }
  });

  wg.Wait();
  ext.join();
  return SummarizeLatenciesUs(lat_ns);
}

// ---------------- CSV helpers ----------------

template <typename Stats>
static void WriteCsvHeader(std::ostream& out) {
  out << "scheduler,workload,workload_param,threads,run,time_ms,"
         "lat_samples,lat_p50_us,lat_p90_us,lat_p99_us,lat_max_us,"
         "picked_lifo,picked_local,picked_global,picked_steal,"
         "steals_attempted,steals_succeeded,"
         "global_poll_61,grabbed_global_batches,grabbed_global_tasks,"
         "offload_calls,offloaded_tasks,"
         "parks,wakes,"
         "submits_external,submits_internal,submits_next,submits_yield\n";
  (void)sizeof(Stats);
}

template <typename Stats>
static void WriteCsvRow(std::ostream& out,
                        const std::string& scheduler,
                        const std::string& workload,
                        const std::string& workload_param,
                        size_t threads,
                        int run,
                        double time_ms,
                        const LatencySummary& lat,
                        const Stats& s) {
  out << scheduler << "," << workload << "," << workload_param << "," << threads << "," << run << "," << time_ms << ","
      << lat.samples << "," << lat.p50_us << "," << lat.p90_us << "," << lat.p99_us << "," << lat.max_us << ","
      << s.picked_lifo << "," << s.picked_local << "," << s.picked_global << "," << s.picked_steal << ","
      << s.steals_attempted << "," << s.steals_succeeded << ","
      << s.global_poll_61 << "," << s.grabbed_from_global_batches << "," << s.grabbed_from_global_tasks << ","
      << s.offload_calls << "," << s.offloaded_tasks << ","
      << s.parks << "," << s.wakes << ","
      << s.submits_external << "," << s.submits_internal << "," << s.submits_next << "," << s.submits_yield
      << "\n";
}

// ---------------- bench runner ----------------

struct WorkloadCase {
  std::string name;
  std::string param;
  std::function<LatencySummary(exe::runtime::View)> fn;
};

template <typename Pool, typename Stats>
static void RunPool(std::ostream& csv,
                    const std::string& scheduler,
                    size_t threads,
                    int runs,
                    const std::vector<WorkloadCase>& workloads) {
  for (int r = 0; r < runs; ++r) {
    Pool pool(threads);
    pool.Start();
    exe::runtime::View view{&pool, nullptr};

    // warmup: small fork-join
    Workload_ForkJoin(view, 2000, 50);

    for (const auto& w : workloads) {
      std::cout << "[" << scheduler << "] run=" << r << " workload=" << w.name
                << " param=" << w.param << "...\n";
      std::cout.flush();
      auto t0 = Clock::now();
      LatencySummary lat = w.fn(view);
      uint64_t ns = NsSince(t0);

      Stats st = pool.GetStats();
      WriteCsvRow(csv, scheduler, w.name, w.param, threads, r, Ms(ns), lat, st);
      csv.flush();
      std::cout << "  time_ms=" << Ms(ns) << "\n";
      std::cout.flush();
    }

    pool.Stop();
  }
}

static size_t ParseThreads(int argc, char** argv) {
  size_t threads = std::thread::hardware_concurrency();
  if (threads == 0) threads = 4;
  if (argc >= 2) {
    threads = (size_t)std::stoul(argv[1]);
  }
  return threads;
}

static int ParseRuns(int argc, char** argv) {
  int runs = 1;
  if (argc >= 3) {
    runs = std::stoi(argv[2]);
  }
  return runs;
}

static std::string ParseOut(int argc, char** argv) {
  std::string out = "ws_results.csv";
  if (argc >= 4) {
    out = argv[3];
  }
  return out;
}

int main(int argc, char** argv) {
  const size_t threads_arg = ParseThreads(argc, argv);
  const int runs = ParseRuns(argc, argv);
  const std::string out_path = ParseOut(argc, argv);

  std::ofstream csv(out_path);
  if (!csv) {
    std::cerr << "Failed to open output: " << out_path << "\n";
    return 1;
  }

  WriteCsvHeader<exe::runtime::multi_thread::ThreadPool::Stats>(csv);

  const size_t hw = std::max<size_t>(1, std::thread::hardware_concurrency());
  const size_t max_threads = std::max<size_t>(1, std::min(hw, threads_arg));
  std::vector<size_t> thread_grid;
  thread_grid.reserve(max_threads);
  for (size_t th = 1; th <= max_threads; ++th) {
    thread_grid.push_back(th);
  }

  // Parameter grids (small/medium/large)
  struct ForkParam { int tasks; int work; int sample; };
  const std::vector<ForkParam> fork_grid = {
      {40000, 80, 10000},
      {80000, 80, 10000},
  };

  struct DagParam { int levels; int width; int out; int work; };
  const std::vector<DagParam> dag_grid = {};

  struct ChainParam { int len; int fanout; int side; int chain; };
  const std::vector<ChainParam> chain_grid = {
      {6000, 5, 90, 25},
      {10000, 6, 90, 25},
  };

  struct PipeParam { int items; int stages; int work; };
  const std::vector<PipeParam> pipe_grid = {
      {1500, 6, 60},
      {3000, 6, 60},
  };

  struct SkewParam { int leaves; int heavy; int leaf; };
  const std::vector<SkewParam> skew_grid = {
      {40000, 20000, 30},
      {80000, 20000, 30},
  };

  struct BurstParam { int tasks; int work; };
  const std::vector<BurstParam> burst_grid = {
      {40000, 40},
      {80000, 40},
  };

  struct ProducerParam { int tasks; int work; int pause; int sample; };
  const std::vector<ProducerParam> producer_grid = {
      {120000, 20, 0, 15000},
      {200000, 16, 2, 15000},
  };

  struct MicroBurstParam { int bursts; int burst; int gap_us; int work; };
  const std::vector<MicroBurstParam> micro_burst_grid = {
      {100, 1200, 1500, 24},
      {120, 1200, 2500, 24},
  };

  // Build workload cases for CSV (name + param string + runner returning latency summary)
  std::vector<WorkloadCase> workloads;
  for (const auto& p : fork_grid) {
    workloads.push_back(WorkloadCase{
        "ForkJoin_small_tasks",
        "tasks=" + std::to_string(p.tasks) + ";work=" + std::to_string(p.work),
        [=](exe::runtime::View v) {
          return Workload_ForkJoin_LatencySample(v, p.tasks, p.work, p.sample);
        }});
  }
  for (const auto& p : dag_grid) {
    workloads.push_back(WorkloadCase{
        "DAG_future_heavy",
        "levels=" + std::to_string(p.levels) + ";width=" + std::to_string(p.width) +
            ";out=" + std::to_string(p.out) + ";work=" + std::to_string(p.work),
        [=](exe::runtime::View v) {
          Workload_DAG_LevelGraph(v, p.levels, p.width, p.out, p.work, 42);
          return LatencySummary{};
        }});
  }
  for (const auto& p : chain_grid) {
    workloads.push_back(WorkloadCase{
        "CriticalPath_fanout",
        "len=" + std::to_string(p.len) + ";fanout=" + std::to_string(p.fanout),
        [=](exe::runtime::View v) {
          Workload_CriticalPath_FanOut(v, p.len, p.fanout, p.side, p.chain);
          return LatencySummary{};
        }});
  }
  for (const auto& p : pipe_grid) {
    workloads.push_back(WorkloadCase{
        "Pipeline_asyncRPC",
        "items=" + std::to_string(p.items) + ";stages=" + std::to_string(p.stages),
        [=](exe::runtime::View v) {
          return Workload_Pipeline_Latency(v, p.items, p.stages, p.work);
        }});
  }
  for (const auto& p : skew_grid) {
    workloads.push_back(WorkloadCase{
        "SkewedDAG_heavy_root",
        "leaves=" + std::to_string(p.leaves) + ";heavy=" + std::to_string(p.heavy),
        [=](exe::runtime::View v) {
          Workload_SkewedDAG_HeavyRoot(v, p.leaves, p.heavy, p.leaf);
          return LatencySummary{};
        }});
  }
  for (const auto& p : burst_grid) {
    workloads.push_back(WorkloadCase{
        "BurstyArrivals_wake_cascade",
        "tasks=" + std::to_string(p.tasks) + ";work=" + std::to_string(p.work),
        [=](exe::runtime::View v) {
          return Workload_BurstyArrivals(v, p.tasks, p.work);
        }});
  }
  for (const auto& p : producer_grid) {
    workloads.push_back(WorkloadCase{
        "SingleProducer_steal_pressure",
        "tasks=" + std::to_string(p.tasks) + ";work=" + std::to_string(p.work) +
            ";pause=" + std::to_string(p.pause),
        [=](exe::runtime::View v) {
          return Workload_SingleProducerStealPressure(v, p.tasks, p.work, p.pause, p.sample);
        }});
  }
  for (const auto& p : micro_burst_grid) {
    workloads.push_back(WorkloadCase{
        "PeriodicMicroBursts_ext",
        "bursts=" + std::to_string(p.bursts) + ";burst=" + std::to_string(p.burst) +
            ";gap_us=" + std::to_string(p.gap_us) + ";work=" + std::to_string(p.work),
        [=](exe::runtime::View v) {
          return Workload_PeriodicMicroBursts(v, p.bursts, p.burst, p.gap_us, p.work);
        }});
  }

  // Run sweep
  for (size_t th : thread_grid) {
    if (th > hw) continue;
    if (threads_arg != 0 && th > threads_arg) continue;

    RunPool<exe::runtime::multi_thread::ThreadPool,
            exe::runtime::multi_thread::ThreadPool::Stats>(csv, "baseline_ws", th, runs, workloads);
    RunPool<exe::runtime::multi_thread::ProactiveThreadPool,
            exe::runtime::multi_thread::ProactiveThreadPool::Stats>(csv, "proactive_ws", th, runs, workloads);
  }

  std::cout << "Wrote CSV: " << out_path << "\n";
  return 0;
}
