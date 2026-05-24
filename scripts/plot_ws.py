import csv
import sys
from collections import defaultdict


def mean(xs):
    return sum(xs) / len(xs) if xs else float("nan")

def median(xs):
    ys = sorted(xs)
    if not ys:
        return float("nan")
    n = len(ys)
    mid = n // 2
    return ys[mid] if (n % 2 == 1) else 0.5 * (ys[mid - 1] + ys[mid])

def safe(s: str) -> str:
    return "".join(ch if (ch.isalnum() or ch in "._-") else "_" for ch in s)

def aggregate(xs, mode: str):
    if mode == "median":
        return median(xs)
    return mean(xs)


def load_rows(path):
    rows = []
    with open(path, "r", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            r["threads"] = int(r["threads"])
            r["run"] = int(r["run"])
            r["time_ms"] = float(r["time_ms"])
            # latency fields may be empty for some workloads
            for k in ["lat_samples", "lat_p50_us", "lat_p90_us", "lat_p99_us", "lat_max_us"]:
                if k in r and r[k] != "":
                    try:
                        r[k] = float(r[k])
                    except Exception:
                        r[k] = 0.0
            rows.append(r)
    return rows

def write_svg_simple_line_chart(path, title, x_label, y_label, xs, series, y_refs=None):
    """
    series: list of (name, ys, color)
    y_refs: optional list of (y_value, color, label)
    """
    W, H = 900, 420
    pad_l, pad_r, pad_t, pad_b = 70, 20, 45, 55
    plot_w = W - pad_l - pad_r
    plot_h = H - pad_t - pad_b

    all_y = []
    for _, ys, _ in series:
        all_y.extend([y for y in ys if y == y])
    if y_refs:
        all_y.extend([y for y, _, _ in y_refs if y == y])
    y_min = min(all_y) if all_y else 0.0
    y_max = max(all_y) if all_y else 1.0
    if y_min == y_max:
        y_max = y_min + 1.0

    def x_px(x):
        i = xs.index(x)
        if len(xs) == 1:
            return pad_l + plot_w / 2
        return pad_l + (i / (len(xs) - 1)) * plot_w

    def y_px(y):
        return pad_t + (1.0 - ((y - y_min) / (y_max - y_min))) * plot_h

    def esc(t):
        return (t.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))

    # grid + axes
    lines = []
    lines.append('<?xml version="1.0" encoding="UTF-8"?>')
    lines.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">')
    lines.append('<rect x="0" y="0" width="100%" height="100%" fill="white"/>')
    lines.append(f'<text x="{W/2:.1f}" y="22" text-anchor="middle" font-family="sans-serif" font-size="16">{esc(title)}</text>')

    # axes
    lines.append(f'<line x1="{pad_l}" y1="{pad_t}" x2="{pad_l}" y2="{pad_t+plot_h}" stroke="#222" stroke-width="1"/>')
    lines.append(f'<line x1="{pad_l}" y1="{pad_t+plot_h}" x2="{pad_l+plot_w}" y2="{pad_t+plot_h}" stroke="#222" stroke-width="1"/>')

    # y ticks
    for k in range(5):
        yv = y_min + (k / 4) * (y_max - y_min)
        yp = y_px(yv)
        lines.append(f'<line x1="{pad_l-4}" y1="{yp:.1f}" x2="{pad_l}" y2="{yp:.1f}" stroke="#222" stroke-width="1"/>')
        lines.append(f'<text x="{pad_l-8}" y="{yp+4:.1f}" text-anchor="end" font-family="sans-serif" font-size="11">{yv:.3f}</text>')
        lines.append(f'<line x1="{pad_l}" y1="{yp:.1f}" x2="{pad_l+plot_w}" y2="{yp:.1f}" stroke="#eee" stroke-width="1"/>')

    # horizontal reference lines (e.g. speedup = 1.0)
    if y_refs:
        for yv, color, label in y_refs:
            if not (yv == yv):
                continue
            yp = y_px(yv)
            lines.append(
                f'<line x1="{pad_l}" y1="{yp:.1f}" x2="{pad_l+plot_w}" y2="{yp:.1f}" '
                f'stroke="{color}" stroke-width="1.5" stroke-dasharray="6,4"/>'
            )
            lines.append(
                f'<text x="{pad_l+plot_w-6}" y="{yp-4:.1f}" text-anchor="end" '
                f'font-family="sans-serif" font-size="11" fill="{color}">{esc(label)}</text>'
            )

    # x ticks
    for x in xs:
        xp = x_px(x)
        lines.append(f'<line x1="{xp:.1f}" y1="{pad_t+plot_h}" x2="{xp:.1f}" y2="{pad_t+plot_h+4}" stroke="#222" stroke-width="1"/>')
        lines.append(f'<text x="{xp:.1f}" y="{pad_t+plot_h+18}" text-anchor="middle" font-family="sans-serif" font-size="11">{esc(str(x))}</text>')

    # labels
    lines.append(f'<text x="{W/2:.1f}" y="{H-12}" text-anchor="middle" font-family="sans-serif" font-size="12">{esc(x_label)}</text>')
    lines.append(f'<text x="16" y="{H/2:.1f}" text-anchor="middle" font-family="sans-serif" font-size="12" transform="rotate(-90 16 {H/2:.1f})">{esc(y_label)}</text>')

    # series lines
    for name, ys, color in series:
        pts = []
        for x, y in zip(xs, ys):
            if y != y:
                continue
            pts.append((x_px(x), y_px(y)))
        if len(pts) >= 2:
            d = " ".join(f"{px:.1f},{py:.1f}" for px, py in pts)
            lines.append(f'<polyline fill="none" stroke="{color}" stroke-width="2" points="{d}"/>')
        for px, py in pts:
            lines.append(f'<circle cx="{px:.1f}" cy="{py:.1f}" r="3" fill="{color}"/>')

    # legend
    lx, ly = pad_l + 8, pad_t + 8
    for i, (name, _, color) in enumerate(series):
        y = ly + i * 16
        lines.append(f'<rect x="{lx}" y="{y-10}" width="10" height="10" fill="{color}"/>')
        lines.append(f'<text x="{lx+14}" y="{y-2}" font-family="sans-serif" font-size="11">{esc(name)}</text>')

    lines.append("</svg>")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

def write_svg_simple_bar_chart(path, title, x_label, y_label, labels, series, y_refs=None):
    """
    series: list of (name, values, color)
    y_refs: optional list of (y_value, color, label)
    """
    W, H = 1100, 460
    pad_l, pad_r, pad_t, pad_b = 70, 20, 45, 120
    plot_w = W - pad_l - pad_r
    plot_h = H - pad_t - pad_b

    all_y = []
    for _, vals, _ in series:
        all_y.extend([v for v in vals if v == v])
    if y_refs:
        all_y.extend([y for y, _, _ in y_refs if y == y])
    y_min = 0.0
    y_max = max(all_y) if all_y else 1.0
    if y_max == 0:
        y_max = 1.0

    def y_px(y):
        return pad_t + (1.0 - (y / y_max)) * plot_h

    def esc(t):
        return (t.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))

    n = len(labels)
    m = len(series)
    group_w = plot_w / max(1, n)
    bar_w = group_w / max(1, (m + 1))

    lines = []
    lines.append('<?xml version="1.0" encoding="UTF-8"?>')
    lines.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">')
    lines.append('<rect x="0" y="0" width="100%" height="100%" fill="white"/>')
    lines.append(f'<text x="{W/2:.1f}" y="22" text-anchor="middle" font-family="sans-serif" font-size="16">{esc(title)}</text>')

    # axes
    lines.append(f'<line x1="{pad_l}" y1="{pad_t}" x2="{pad_l}" y2="{pad_t+plot_h}" stroke="#222" stroke-width="1"/>')
    lines.append(f'<line x1="{pad_l}" y1="{pad_t+plot_h}" x2="{pad_l+plot_w}" y2="{pad_t+plot_h}" stroke="#222" stroke-width="1"/>')

    # y ticks
    for k in range(5):
        yv = (k / 4) * y_max
        yp = y_px(yv)
        lines.append(f'<line x1="{pad_l-4}" y1="{yp:.1f}" x2="{pad_l}" y2="{yp:.1f}" stroke="#222" stroke-width="1"/>')
        lines.append(f'<text x="{pad_l-8}" y="{yp+4:.1f}" text-anchor="end" font-family="sans-serif" font-size="11">{yv:.3f}</text>')
        lines.append(f'<line x1="{pad_l}" y1="{yp:.1f}" x2="{pad_l+plot_w}" y2="{yp:.1f}" stroke="#eee" stroke-width="1"/>')

    # horizontal reference lines (e.g. speedup = 1.0)
    if y_refs:
        for yv, color, label in y_refs:
            if not (yv == yv):
                continue
            yp = y_px(yv)
            lines.append(
                f'<line x1="{pad_l}" y1="{yp:.1f}" x2="{pad_l+plot_w}" y2="{yp:.1f}" '
                f'stroke="{color}" stroke-width="1.5" stroke-dasharray="6,4"/>'
            )
            lines.append(
                f'<text x="{pad_l+plot_w-6}" y="{yp-4:.1f}" text-anchor="end" '
                f'font-family="sans-serif" font-size="11" fill="{color}">{esc(label)}</text>'
            )

    # bars
    for i, lab in enumerate(labels):
        gx = pad_l + i * group_w
        for j, (name, vals, color) in enumerate(series):
            v = vals[i]
            x = gx + (j + 0.5) * bar_w
            y = y_px(v)
            h = (pad_t + plot_h) - y
            lines.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w*0.9:.1f}" height="{h:.1f}" fill="{color}"/>')

        # x label
        lines.append(f'<text x="{gx + group_w/2:.1f}" y="{pad_t+plot_h+18}" text-anchor="middle" '
                     f'font-family="sans-serif" font-size="11" transform="rotate(25 {gx + group_w/2:.1f} {pad_t+plot_h+18})">{esc(lab)}</text>')

    # labels
    lines.append(f'<text x="{W/2:.1f}" y="{H-12}" text-anchor="middle" font-family="sans-serif" font-size="12">{esc(x_label)}</text>')
    lines.append(f'<text x="16" y="{H/2:.1f}" text-anchor="middle" font-family="sans-serif" font-size="12" transform="rotate(-90 16 {H/2:.1f})">{esc(y_label)}</text>')

    # legend
    lx, ly = pad_l + 8, H - 70
    for i, (name, _, color) in enumerate(series):
        x = lx + i * 160
        lines.append(f'<rect x="{x}" y="{ly}" width="10" height="10" fill="{color}"/>')
        lines.append(f'<text x="{x+14}" y="{ly+9}" font-family="sans-serif" font-size="11">{esc(name)}</text>')

    lines.append("</svg>")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main():
    if len(sys.argv) < 2:
        print("usage: plot_ws.py <ws_results.csv> [out_prefix] [--agg=mean|median]")
        return 2

    path = sys.argv[1]
    prefix = "ws"
    agg_mode = "mean"
    for arg in sys.argv[2:]:
        if arg.startswith("--agg="):
            agg_mode = arg.split("=", 1)[1].strip().lower()
        elif prefix == "ws":
            prefix = arg
        else:
            print(f"unexpected argument: {arg}")
            return 2
    if agg_mode not in ("mean", "median"):
        print(f"unsupported aggregation mode: {agg_mode} (expected mean or median)")
        return 2

    rows = load_rows(path)

    # group: workload -> scheduler -> [times]
    times = defaultdict(lambda: defaultdict(list))
    # group: workload -> threads -> scheduler -> [times]
    times_by_threads = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    # latency: workload -> threads -> scheduler -> [p50]
    lat_p50 = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    # latency: workload -> threads -> scheduler -> [p99]
    lat_p99 = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    # latency by param: workload -> param -> threads -> scheduler -> [p99]
    lat_p99_by_param = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list))))
    # workload + param -> threads -> scheduler -> [times]
    times_by_param = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list))))

    for r in rows:
        times[r["workload"]][r["scheduler"]].append(r["time_ms"])
        times_by_threads[r["workload"]][r["threads"]][r["scheduler"]].append(r["time_ms"])
        times_by_param[r.get("workload", "")][r.get("workload_param", "")][r["threads"]][r["scheduler"]].append(r["time_ms"])
        if "lat_p50_us" in r and r.get("lat_samples", 0) and r.get("lat_p50_us", 0) > 0:
            lat_p50[r["workload"]][r["threads"]][r["scheduler"]].append(r["lat_p50_us"])
        if "lat_p99_us" in r and r.get("lat_samples", 0) and r.get("lat_p99_us", 0) > 0:
            lat_p99[r["workload"]][r["threads"]][r["scheduler"]].append(r["lat_p99_us"])
            lat_p99_by_param[r["workload"]][r.get("workload_param", "")][r["threads"]][r["scheduler"]].append(r["lat_p99_us"])

    # print a small, copy-pastable table and a "speedup" summary
    print("workload,baseline_ms,proactive_ms,speedup")
    for workload, by_sched in sorted(times.items()):
        b = aggregate(by_sched.get("baseline_ws", []), agg_mode)
        p = aggregate(by_sched.get("proactive_ws", []), agg_mode)
        speedup = (b / p) if (p and p == p) else float("nan")
        print(f"{workload},{b:.3f},{p:.3f},{speedup:.3f}")

    # Always generate SVG graphs (no matplotlib needed).
    # 1) Aggregate bars
    workloads = sorted(times.keys())
    baseline = [aggregate(times[w].get("baseline_ws", []), agg_mode) for w in workloads]
    proactive = [aggregate(times[w].get("proactive_ws", []), agg_mode) for w in workloads]
    write_svg_simple_bar_chart(
        f"{prefix}_times.svg",
        f"Work-stealing schedulers: baseline vs proactive ({agg_mode})",
        "workload",
        f"time (ms), {agg_mode} over runs",
        workloads,
        [("baseline_ws", baseline, "#4c78a8"), ("proactive_ws", proactive, "#f58518")],
    )
    print(f"\nsaved {prefix}_times.svg")

    speed = [b / p if p > 0 else float("nan") for b, p in zip(baseline, proactive)]
    write_svg_simple_bar_chart(
        f"{prefix}_speedup.svg",
        f"Speedup from proactive stealing ({agg_mode})",
        "workload",
        "speedup (baseline / proactive)",
        workloads,
        [("speedup", speed, "#54a24b")],
        y_refs=[(1.0, "#777", "no speedup (1.0)")],
    )
    print(f"saved {prefix}_speedup.svg")

    # 2) speedup vs threads (per workload, aggregated over params)
    for workload in sorted(times_by_threads.keys()):
        ths = sorted(times_by_threads[workload].keys())
        if len(ths) <= 1:
            continue
        b = [aggregate(times_by_threads[workload][t].get("baseline_ws", []), agg_mode) for t in ths]
        p = [aggregate(times_by_threads[workload][t].get("proactive_ws", []), agg_mode) for t in ths]
        # Compute p99 speedup robustly: aggregate speedups across params first,
        # then aggregate those speedups (avoids spikes from mixing bimodal params).
        speed = []
        for t, bb, pp in zip(ths, b, p):
            by_param_speedups = []
            for param, by_threads in lat_p99_by_param[workload].items():
                sched = by_threads.get(t, {})
                b_vals = sched.get("baseline_ws", [])
                p_vals = sched.get("proactive_ws", [])
                if not b_vals or not p_vals:
                    continue
                b_param = aggregate(b_vals, agg_mode)
                p_param = aggregate(p_vals, agg_mode)
                if p_param and p_param > 0:
                    by_param_speedups.append(b_param / p_param)
            if by_param_speedups:
                speed.append(aggregate(by_param_speedups, agg_mode))
            else:
                speed.append((bb / pp) if (pp and pp > 0) else float("nan"))
        write_svg_simple_line_chart(
            f"{prefix}_speedup_vs_threads_{safe(workload)}.svg",
            f"Speedup vs threads: {workload} ({agg_mode})",
            "threads",
            "speedup (baseline / proactive)",
            ths,
            [("speedup", speed, "#54a24b")],
            y_refs=[(1.0, "#777", "no speedup (1.0)")],
        )
        print(f"saved {prefix}_speedup_vs_threads_{safe(workload)}.svg")

    # 2b) speedup vs threads for each workload parameter (more detailed, less averaging)
    for workload in sorted(times_by_param.keys()):
        for param in sorted(times_by_param[workload].keys()):
            ths = sorted(times_by_param[workload][param].keys())
            if len(ths) <= 1:
                continue
            b = [aggregate(times_by_param[workload][param][t].get("baseline_ws", []), agg_mode) for t in ths]
            p = [aggregate(times_by_param[workload][param][t].get("proactive_ws", []), agg_mode) for t in ths]
            speed = [(bb / pp) if (pp and pp > 0) else float("nan") for bb, pp in zip(b, p)]
            tag = f"{safe(workload)}__{safe(param)}"
            write_svg_simple_line_chart(
                f"{prefix}_speedup_vs_threads_param_{tag}.svg",
                f"Speedup vs threads: {workload} ({param}, {agg_mode})",
                "threads",
                "speedup (baseline / proactive)",
                ths,
                [("speedup", speed, "#54a24b")],
                y_refs=[(1.0, "#777", "no speedup (1.0)")],
            )
            print(f"saved {prefix}_speedup_vs_threads_param_{tag}.svg")

    # 3) p50 latency vs threads (if available)
    for workload in sorted(lat_p50.keys()):
        ths = sorted(lat_p50[workload].keys())
        if len(ths) <= 1:
            continue
        b = [aggregate(lat_p50[workload][t].get("baseline_ws", []), agg_mode) for t in ths]
        p = [aggregate(lat_p50[workload][t].get("proactive_ws", []), agg_mode) for t in ths]
        if all((x != x) for x in b) and all((x != x) for x in p):
            continue
        write_svg_simple_line_chart(
            f"{prefix}_lat_p50_vs_threads_{safe(workload)}.svg",
            f"Latency p50 vs threads: {workload} ({agg_mode})",
            "threads",
            f"p50 latency (us), {agg_mode}",
            ths,
            [("baseline_ws", b, "#4c78a8"), ("proactive_ws", p, "#f58518")],
        )
        print(f"saved {prefix}_lat_p50_vs_threads_{safe(workload)}.svg")

    # 4) p99 latency vs threads + p99 speedup
    for workload in sorted(lat_p99.keys()):
        ths = sorted(lat_p99[workload].keys())
        if len(ths) <= 1:
            continue
        b = [aggregate(lat_p99[workload][t].get("baseline_ws", []), agg_mode) for t in ths]
        p = [aggregate(lat_p99[workload][t].get("proactive_ws", []), agg_mode) for t in ths]
        if all((x != x) for x in b) and all((x != x) for x in p):
            continue
        write_svg_simple_line_chart(
            f"{prefix}_lat_p99_vs_threads_{safe(workload)}.svg",
            f"Latency p99 vs threads: {workload} ({agg_mode})",
            "threads",
            f"p99 latency (us), {agg_mode}",
            ths,
            [("baseline_ws", b, "#4c78a8"), ("proactive_ws", p, "#f58518")],
        )
        print(f"saved {prefix}_lat_p99_vs_threads_{safe(workload)}.svg")

        speed = [(bb / pp) if (pp and pp > 0) else float("nan") for bb, pp in zip(b, p)]
        write_svg_simple_line_chart(
            f"{prefix}_lat_p99_speedup_vs_threads_{safe(workload)}.svg",
            f"Latency p99 speedup vs threads: {workload} ({agg_mode})",
            "threads",
            "speedup (baseline / proactive)",
            ths,
            [("p99 speedup", speed, "#e45756")],
            y_refs=[(1.0, "#777", "no speedup (1.0)")],
        )
        print(f"saved {prefix}_lat_p99_speedup_vs_threads_{safe(workload)}.svg")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
