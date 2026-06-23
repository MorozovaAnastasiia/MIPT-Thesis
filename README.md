# Proactive Work Stealing Scheduler

## Русская версия

### Описание

Этот проект посвящён исследованию и реализации планировщика задач на основе **Work Stealing** и сравнению классического подхода с модифицированной стратегией **Proactive Work Stealing**.

Work Stealing — это подход к балансировке нагрузки в многопоточном исполнении, при котором каждый worker имеет собственную очередь задач. Если worker заканчивает свою работу, он пытается “украсть” задачу из очереди другого worker’а. Такой подход хорошо работает для нерегулярного параллелизма, когда заранее неизвестно, как именно распределится нагрузка между потоками.

Цель проекта — реализовать базовый Work Stealing scheduler, добавить proactive-логику перераспределения работы и экспериментально оценить, в каких сценариях proactive-подход даёт выигрыш, а в каких его дополнительный overhead начинает доминировать.

### Основные идеи

В проекте рассматриваются две стратегии планирования:

1. **Baseline Work Stealing**
   Классическая модель, в которой worker забирает задачи из собственной очереди, а при её опустошении пытается украсть работу у другого worker’а.

2. **Proactive Work Stealing**
   Модифицированный подход, в котором перераспределение работы может происходить более агрессивно, до полного исчерпания локальной работы. Цель такого подхода — быстрее выравнивать нагрузку между worker’ами в сценариях с нерегулярным появлением задач, ожиданиями и зависимостями.

### Что реализовано

* Многопоточный task scheduler на основе Work Stealing.
* Локальные очереди задач для worker’ов.
* Механизм stealing между worker’ами.
* Реализация baseline и proactive стратегий.
* Набор синтетических benchmarks для разных классов нагрузки.
* Сбор latency и throughput метрик.
* Сравнение поведения алгоритмов при разном числе потоков.

### Классы benchmark’ов

Бенчмарки были выбраны не под один конкретный сервис, а по свойствам нагрузки, которые важны для планировщика: способ появления задач, размер задач, наличие зависимостей, ожидания, bursty traffic и степень доступного параллелизма.

#### Fork-Join Small Tasks

Сценарий, в котором одна задача порождает много маленьких независимых подзадач, а затем ждёт их завершения.

Такой workload моделирует ситуации, где есть fan-out/fan-in структура: валидации, агрегации, расчёт признаков, health-check gates, небольшие параллельные стадии pipeline’а.

Этот benchmark показывает цену scheduling overhead, stealing и join-синхронизации на мелких задачах.

#### Pipeline Async RPS

Сценарий, в котором задачи проходят через несколько стадий pipeline’а и могут ожидать внешние события: RPC, I/O, ack, timeout или completion callback.

Такой workload близок к backend-сервисам, где одновременно находится много операций “в полёте”, а полезная работа часто продолжается через continuation после завершения асинхронной операции.

#### Single Producer

Сценарий, в котором один producer создаёт burst задач, а несколько worker’ов должны эффективно разобрать эту работу.

Такой класс нагрузки возникает, когда одно событие порождает много однотипных подзадач: например, fan-out уведомлений, обработка батча, локальная диспетчеризация задач внутри control-plane сервиса.

#### Burst Arrivals

Сценарий с резким неравномерным поступлением задач.

Он нужен, чтобы проверить поведение scheduler’а не только в steady-state, но и при пиковых всплесках нагрузки, когда могут проявляться contention на очередях, атомиках, аллокаторе или wake-up механизмах.

#### Periodic Micro Bursts

Сценарий с короткими регулярными всплесками мелких задач.

Такой workload моделирует периодические фоновые работы: обновление метрик, обработку таймеров, maintenance tasks, health checks, короткие batch-задачи. Он особенно полезен для анализа tail latency, потому что overhead планировщика может оказаться сравним с полезной работой.

### Среда экспериментов

Эксперименты запускались на одной вычислительной машине:

* CPU: 40 vCPU
* RAM: 40 GB
* OS: Linux
* Количество worker threads варьировалось от 1 до 32
* Сравнивались baseline Work Stealing и Proactive Work Stealing
* Основные метрики: latency, p50/p95/p99, throughput

Важно: 40 vCPU не обязательно соответствуют 40 физическим ядрам. Поэтому отсутствие ускорения после определённого числа потоков может быть связано с насыщением доступного параллелизма, contention, виртуализацией, ограничениями памяти и overhead самого планировщика.

### Основные наблюдения

* Proactive Work Stealing может давать выигрыш в сценариях, где работа появляется неравномерно и есть возможность заранее уменьшить дисбаланс между worker’ами.
* На workloads с мелкими задачами дополнительный overhead proactive-логики может стать сравнимым с полезной работой.
* При увеличении числа потоков ускорение не обязано расти линейно: после точки насыщения дополнительные worker’ы увеличивают contention и могут ухудшать latency.
* Для bursty workloads важно учитывать не только throughput, но и tail latency.
* Proactive-подход не является универсально лучшим: его эффективность зависит от структуры задач, размера burst’ов, стоимости stealing и доступного параллелизма.

### Вывод

Проект показывает, что эффективность Work Stealing scheduler’а сильно зависит от класса нагрузки. Proactive Work Stealing может быть полезен для нерегулярных workloads с дисбалансом задач, но может проигрывать baseline на слишком мелких задачах или после насыщения числа потоков.

Главный результат проекта — не утверждение, что proactive strategy всегда быстрее, а выявление условий, при которых она действительно полезна, и границ её применимости.

---

## English Version

### Overview

This project explores the implementation and evaluation of a task scheduler based on **Work Stealing**, comparing a classic baseline approach with a modified **Proactive Work Stealing** strategy.

Work Stealing is a load balancing technique for parallel execution. Each worker owns a local task queue. When a worker runs out of local work, it attempts to steal tasks from another worker’s queue. This approach is commonly used for irregular parallel workloads where the amount of work is not evenly distributed in advance.

The goal of this project is to implement a baseline Work Stealing scheduler, add proactive work redistribution logic, and experimentally evaluate where the proactive strategy improves performance and where its additional overhead becomes dominant.

### Core ideas

The project compares two scheduling strategies:

1. **Baseline Work Stealing**
   A classic approach where each worker processes tasks from its local queue and attempts to steal work only when the local queue becomes empty.

2. **Proactive Work Stealing**
   A modified strategy that performs more aggressive work redistribution. The goal is to reduce load imbalance earlier, especially in workloads with irregular task generation, dependencies, waiting, and bursts of work.

### Implemented features

* Multithreaded task scheduler based on Work Stealing.
* Per-worker local task queues.
* Work stealing mechanism between workers.
* Baseline and proactive scheduling strategies.
* Synthetic benchmarks covering different workload classes.
* Latency and throughput measurements.
* Comparison across different worker thread counts.

### Benchmark classes

The benchmarks were selected based on workload properties that are important for a scheduler: how tasks are generated, task size, dependencies, waiting, burstiness, and the amount of available parallelism.

#### Fork-Join Small Tasks

A workload where one task spawns many small independent subtasks and then waits for their completion.

This models fan-out/fan-in patterns such as validation, aggregation, feature computation, health-check gates, and small parallel pipeline stages.

This benchmark highlights the cost of scheduling overhead, stealing, and join synchronization for small tasks.

#### Pipeline Async RPS

A workload where tasks go through multiple pipeline stages and may wait for external events such as RPC, I/O, acknowledgements, timeouts, or completion callbacks.

This pattern is close to backend services where many operations are in flight at the same time, and useful work often continues through continuations after asynchronous operations complete.

#### Single Producer

A workload where a single producer generates a burst of tasks, and multiple workers need to process them efficiently.

This pattern appears when a single event creates many similar subtasks, such as notification fan-out, batch processing, or local task dispatching inside a control-plane service.

#### Burst Arrivals

A workload with sudden non-uniform task arrivals.

This benchmark is used to evaluate scheduler behavior under peak load rather than only in steady-state conditions. It helps expose contention on queues, atomics, allocators, and wake-up mechanisms.

#### Periodic Micro Bursts

A workload with short, regular bursts of small tasks.

This models periodic background work such as metrics updates, timer processing, maintenance tasks, health checks, and small batch jobs. It is especially useful for analyzing tail latency, because scheduling overhead may become comparable to the actual useful work.

### Experimental environment

The experiments were run on a single compute machine:

* CPU: 40 vCPU
* RAM: 40 GB
* OS: Linux
* Worker thread count varied from 1 to 32
* Compared strategies: Baseline Work Stealing and Proactive Work Stealing
* Main metrics: latency, p50/p95/p99, throughput

Note: 40 vCPU does not necessarily mean 40 physical cores. Therefore, the absence of speedup after a certain number of threads may be caused by limited available parallelism, contention, virtualization effects, memory bandwidth limits, and scheduler overhead.

### Main observations

* Proactive Work Stealing can improve performance in workloads where tasks appear unevenly and early redistribution helps reduce worker imbalance.
* For workloads with very small tasks, the additional overhead of proactive logic may become comparable to the useful work.
* Increasing the number of threads does not guarantee linear speedup. After the saturation point, additional workers may increase contention and worsen latency.
* For bursty workloads, it is important to analyze not only throughput but also tail latency.
* The proactive strategy is not universally better: its effectiveness depends on task structure, burst size, stealing cost, and available parallelism.

### Conclusion

This project demonstrates that the effectiveness of a Work Stealing scheduler strongly depends on the workload class. Proactive Work Stealing can be useful for irregular workloads with task imbalance, but it may lose to the baseline strategy on very small tasks or after the workload reaches its parallelism saturation point.

The main result is not that proactive scheduling is always faster, but rather an analysis of when it helps, when it does not, and what practical limits it has.
