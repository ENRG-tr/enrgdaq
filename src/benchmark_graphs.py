#!/usr/bin/env python3
"""
ENRGDAQ Benchmark Visualization Suite

Generates publication-quality graphs for JINST paper.
Run the benchmark first to collect data, then generate graphs.

Usage:
    python benchmark_graphs.py --run        # Run benchmarks and save data
    python benchmark_graphs.py --plot       # Generate graphs from saved data
    python benchmark_graphs.py --all        # Run both
"""

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt
import numpy as np

# Style settings for publication-quality figures
plt.style.use("seaborn-v0_8-whitegrid")
plt.rcParams.update(
    {
        "font.family": "serif",
        "font.size": 11,
        "axes.labelsize": 12,
        "axes.titlesize": 13,
        "legend.fontsize": 10,
        "xtick.labelsize": 10,
        "ytick.labelsize": 10,
        "figure.figsize": (8, 5),
        "figure.dpi": 150,
        "savefig.dpi": 300,
        "savefig.bbox": "tight",
    }
)

RESULTS_DIR = Path("benchmark_results")
GRAPHS_DIR = Path("benchmark_graphs")


@dataclass
class BenchmarkResult:
    """Single benchmark run result."""

    timestamp: str
    num_clients: int
    payload_size: int
    duration_seconds: float
    total_data_mb: float
    total_messages: int
    avg_throughput_mbps: float
    peak_throughput_mbps: float
    messages_per_second: float
    avg_queue_size: float


def run_single_benchmark(
    clients: int,
    payload_size: int,
    duration: int = 10,
) -> Optional[BenchmarkResult]:
    """Run a single benchmark configuration and parse results."""
    print(f"  Running: clients={clients}, payload_size={payload_size}...")

    cmd = [
        sys.executable,
        "src/benchmark.py",
        "--clients",
        str(clients),
        "--payload-size",
        str(payload_size),
        "--duration",
        str(duration),
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=duration + 60,  # Extra time for startup/shutdown
            cwd=os.getcwd(),
        )

        output = result.stdout + result.stderr

        # Parse results from output
        lines = output.split("\n")
        data = {}

        for line in lines:
            if "Duration:" in line and "seconds" in line:
                data["duration"] = float(line.split(":")[1].strip().split()[0])
            elif "Average Throughput:" in line:
                data["avg_throughput"] = float(line.split(":")[1].strip().split()[0])
            elif "Peak Throughput:" in line:
                data["peak_throughput"] = float(line.split(":")[1].strip().split()[0])
            elif "Total Data:" in line:
                data["total_data"] = float(line.split(":")[1].strip().split()[0])
            elif "Total Messages:" in line:
                msg_str = line.split(":")[1].strip().replace(",", "")
                data["total_messages"] = int(msg_str)
            elif "Messages/Second:" in line:
                msg_str = line.split(":")[1].strip().replace(",", "")
                data["messages_per_second"] = float(msg_str)
            elif "Average Queue Size:" in line:
                data["avg_queue"] = float(line.split(":")[1].strip())

        if len(data) < 5:
            print(f"    Warning: Could not parse all results. Got: {data}")
            return None

        return BenchmarkResult(
            timestamp=datetime.now().isoformat(),
            num_clients=clients,
            payload_size=payload_size,
            duration_seconds=data.get("duration", duration),
            total_data_mb=data.get("total_data", 0),
            total_messages=data.get("total_messages", 0),
            avg_throughput_mbps=data.get("avg_throughput", 0),
            peak_throughput_mbps=data.get("peak_throughput", 0),
            messages_per_second=data.get("messages_per_second", 0),
            avg_queue_size=data.get("avg_queue", 0),
        )

    except subprocess.TimeoutExpired:
        print("    Timeout!")
        return None
    except Exception as e:
        print(f"    Error: {e}")
        return None


def run_benchmarks():
    """Run comprehensive benchmark suite."""
    RESULTS_DIR.mkdir(exist_ok=True)

    results = []

    # Test 1: Throughput vs Payload Size (fixed clients)
    print("\n=== Test 1: Throughput vs Payload Size ===")
    payload_sizes = [1000, 5000, 10000, 50000, 100000, 200000]
    for payload in payload_sizes:
        result = run_single_benchmark(clients=3, payload_size=payload, duration=10)
        if result:
            results.append(asdict(result))
        time.sleep(2)  # Cool down between runs

    # Test 2: Throughput vs Number of Clients (fixed payload)
    print("\n=== Test 2: Throughput vs Number of Clients ===")
    client_counts = [1, 2, 3, 4, 5]
    for clients in client_counts:
        result = run_single_benchmark(clients=clients, payload_size=50000, duration=10)
        if result:
            results.append(asdict(result))
        time.sleep(2)

    # Test 3: Message Rate vs Payload Size
    print("\n=== Test 3: Small Payloads (High Message Rate) ===")
    small_payloads = [100, 500, 1000, 2000, 5000]
    for payload in small_payloads:
        result = run_single_benchmark(clients=3, payload_size=payload, duration=10)
        if result:
            results.append(asdict(result))
        time.sleep(2)

    # Save results
    results_file = (
        RESULTS_DIR
        / f"benchmark_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    )
    with open(results_file, "w") as f:
        json.dump(results, f, indent=2)

    # Also save as latest
    with open(RESULTS_DIR / "latest_results.json", "w") as f:
        json.dump(results, f, indent=2)

    print(f"\nResults saved to: {results_file}")
    return results


def load_results() -> list[dict]:
    """Load latest benchmark results."""
    results_file = RESULTS_DIR / "latest_results.json"
    if not results_file.exists():
        print(f"No results found at {results_file}")
        print("Run with --run first to generate benchmark data.")
        sys.exit(1)

    with open(results_file) as f:
        return json.load(f)


def plot_throughput_vs_payload(results: list[dict]):
    """Plot throughput vs payload size."""
    # Filter for payload size test (3 clients)
    data = [r for r in results if r["num_clients"] == 3]
    data.sort(key=lambda x: x["payload_size"])

    if len(data) < 2:
        print("Not enough data for throughput vs payload plot")
        return

    payloads = [r["payload_size"] for r in data]
    avg_throughput = [r["avg_throughput_mbps"] for r in data]
    peak_throughput = [r["peak_throughput_mbps"] for r in data]

    fig, ax = plt.subplots()

    ax.plot(
        payloads,
        avg_throughput,
        "o-",
        color="#2ecc71",
        linewidth=2,
        markersize=8,
        label="Average Throughput",
    )
    ax.plot(
        payloads,
        peak_throughput,
        "s--",
        color="#3498db",
        linewidth=2,
        markersize=8,
        label="Peak Throughput",
    )

    ax.fill_between(payloads, avg_throughput, alpha=0.3, color="#2ecc71")

    ax.set_xlabel("Payload Size (values per message)")
    ax.set_ylabel("Throughput (MB/s)")
    ax.set_title("ENRGDAQ Throughput vs Message Payload Size\n(3 concurrent clients)")
    ax.legend(loc="best")
    ax.set_xscale("log")
    ax.grid(True, alpha=0.3)

    # Add data rate annotation
    ax.axhline(
        y=20, color="red", linestyle=":", alpha=0.7, label="CAEN Digitizer (~20 MB/s)"
    )
    ax.text(payloads[0], 22, "CAEN Digitizer Rate", fontsize=9, color="red")

    plt.tight_layout()
    plt.savefig(GRAPHS_DIR / "throughput_vs_payload.png")
    plt.savefig(GRAPHS_DIR / "throughput_vs_payload.pdf")
    print("  Saved: throughput_vs_payload.png/pdf")
    plt.close()


def plot_peak_throughput(results: list[dict]):
    """Plot peak throughput vs payload size - shows maximum system capability."""
    # Filter for payload size test (3 clients)
    data = [r for r in results if r["num_clients"] == 3]
    data.sort(key=lambda x: x["payload_size"])

    if len(data) < 2:
        print("Not enough data for peak throughput plot")
        return

    payloads = [r["payload_size"] for r in data]
    peak_throughput = [r["peak_throughput_mbps"] for r in data]

    fig, ax = plt.subplots()

    # Create gradient-like bar chart
    colors = plt.cm.Blues(np.linspace(0.4, 0.9, len(payloads)))
    bars = ax.bar(
        range(len(payloads)),
        peak_throughput,
        color=colors,
        edgecolor="navy",
        linewidth=1.5,
    )

    ax.set_xlabel("Message Payload Size (values)")
    ax.set_ylabel("Peak Throughput (MB/s)")
    ax.set_title("ENRGDAQ Maximum Throughput Capability")
    ax.set_xticks(range(len(payloads)))
    ax.set_xticklabels([f"{p:,}" for p in payloads], rotation=45, ha="right")
    ax.grid(True, alpha=0.3, axis="y")

    # Add value labels on bars
    for bar, val in zip(bars, peak_throughput):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 5,
            f"{val:.0f}",
            ha="center",
            va="bottom",
            fontsize=10,
            fontweight="bold",
        )

    # Highlight maximum
    max_idx = peak_throughput.index(max(peak_throughput))
    ax.annotate(
        f"Max: {max(peak_throughput):.0f} MB/s",
        xy=(max_idx, max(peak_throughput)),
        xytext=(max_idx - 1.5, max(peak_throughput) + 40),
        fontsize=11,
        fontweight="bold",
        color="darkgreen",
        arrowprops=dict(arrowstyle="->", color="darkgreen", lw=2),
    )

    plt.tight_layout()
    plt.savefig(GRAPHS_DIR / "peak_throughput.png")
    plt.savefig(GRAPHS_DIR / "peak_throughput.pdf")
    print("  Saved: peak_throughput.png/pdf")
    plt.close()


def plot_requirements_comparison(results: list[dict]):
    """Compare ENRGDAQ performance vs typical DAQ requirements."""
    # Get best single-client result and typical multi-client result
    single_client = [r for r in results if r["num_clients"] == 1]
    multi_client = [
        r for r in results if r["num_clients"] == 3 and r["payload_size"] >= 50000
    ]

    if not single_client or not multi_client:
        print("Not enough data for requirements comparison")
        return

    # Get best results
    best_single = max(single_client, key=lambda x: x["avg_throughput_mbps"])
    best_multi = max(multi_client, key=lambda x: x["avg_throughput_mbps"])

    fig, ax = plt.subplots(figsize=(10, 6))

    categories = [
        "CAEN Digitizer\nRequirement",
        "ENRGDAQ\n(3 sources)",
        "ENRGDAQ\n(single source)",
        "ENRGDAQ\nPeak Capability",
    ]
    values = [
        20,  # CAEN requirement
        best_multi["avg_throughput_mbps"],
        best_single["avg_throughput_mbps"],
        best_single["peak_throughput_mbps"],
    ]
    colors = ["#e74c3c", "#3498db", "#2ecc71", "#9b59b6"]

    bars = ax.bar(categories, values, color=colors, edgecolor="black", linewidth=1.5)

    ax.set_ylabel("Throughput (MB/s)", fontsize=12)
    ax.set_title("ENRGDAQ Performance vs Data Acquisition Requirements", fontsize=14)
    ax.grid(True, alpha=0.3, axis="y")

    # Add value labels
    for bar, val in zip(bars, values):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 3,
            f"{val:.0f} MB/s",
            ha="center",
            va="bottom",
            fontsize=11,
            fontweight="bold",
        )

    # Add headroom annotations
    headroom_single = best_single["avg_throughput_mbps"] / 20
    ax.annotate(
        f"{headroom_single:.1f}x headroom",
        xy=(2, best_single["avg_throughput_mbps"]),
        xytext=(2.5, best_single["avg_throughput_mbps"] - 20),
        fontsize=10,
        color="darkgreen",
        arrowprops=dict(arrowstyle="->", color="darkgreen"),
    )

    plt.tight_layout()
    plt.savefig(GRAPHS_DIR / "requirements_comparison.png")
    plt.savefig(GRAPHS_DIR / "requirements_comparison.pdf")
    print("  Saved: requirements_comparison.png/pdf")
    plt.close()


def plot_single_client_performance(results: list[dict]):
    """Highlight single-client (optimal) performance across payload sizes."""
    # Get single-client results (or best per payload)
    payload_best = {}
    for r in results:
        ps = r["payload_size"]
        if (
            ps not in payload_best
            or r["avg_throughput_mbps"] > payload_best[ps]["avg_throughput_mbps"]
        ):
            payload_best[ps] = r

    data = sorted(payload_best.values(), key=lambda x: x["payload_size"])

    if len(data) < 2:
        print("Not enough data for single client plot")
        return

    payloads = [r["payload_size"] for r in data]
    avg_throughput = [r["avg_throughput_mbps"] for r in data]
    peak_throughput = [r["peak_throughput_mbps"] for r in data]

    fig, ax = plt.subplots()

    ax.fill_between(
        payloads,
        avg_throughput,
        peak_throughput,
        alpha=0.3,
        color="#3498db",
        label="Performance Range",
    )
    ax.plot(
        payloads,
        avg_throughput,
        "o-",
        color="#2ecc71",
        linewidth=2,
        markersize=8,
        label="Sustained Throughput",
    )
    ax.plot(
        payloads,
        peak_throughput,
        "s--",
        color="#3498db",
        linewidth=2,
        markersize=8,
        label="Peak Throughput",
    )

    # Add CAEN reference line
    ax.axhline(y=20, color="red", linestyle=":", alpha=0.8, linewidth=2)
    ax.text(
        payloads[-1],
        25,
        "CAEN Digitizer (20 MB/s)",
        fontsize=9,
        color="red",
        ha="right",
    )

    ax.set_xlabel("Payload Size (values per message)")
    ax.set_ylabel("Throughput (MB/s)")
    ax.set_title("ENRGDAQ Optimal Single-Source Performance")
    ax.legend(loc="upper left")
    ax.set_xscale("log")
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(GRAPHS_DIR / "single_client_performance.png")
    plt.savefig(GRAPHS_DIR / "single_client_performance.pdf")
    print("  Saved: single_client_performance.png/pdf")
    plt.close()


def plot_message_rate(results: list[dict]):
    """Plot message rate vs payload size."""
    # Use all data
    data = [r for r in results if r["num_clients"] == 3]
    data.sort(key=lambda x: x["payload_size"])

    if len(data) < 2:
        print("Not enough data for message rate plot")
        return

    payloads = [r["payload_size"] for r in data]
    msg_rates = [r["messages_per_second"] for r in data]

    fig, ax = plt.subplots()

    ax.plot(payloads, msg_rates, "o-", color="#f39c12", linewidth=2, markersize=8)
    ax.fill_between(payloads, msg_rates, alpha=0.3, color="#f39c12")

    ax.set_xlabel("Payload Size (values per message)")
    ax.set_ylabel("Message Rate (messages/second)")
    ax.set_title("ENRGDAQ Message Throughput Rate\n(3 concurrent clients)")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.grid(True, alpha=0.3)

    # Add annotations for key points
    if len(data) > 0:
        max_rate = max(msg_rates)
        max_idx = msg_rates.index(max_rate)
        ax.annotate(
            f"{max_rate:.0f} msg/s",
            xy=(payloads[max_idx], max_rate),
            xytext=(payloads[max_idx] * 2, max_rate * 1.2),
            fontsize=9,
            arrowprops=dict(arrowstyle="->", color="gray"),
        )

    plt.tight_layout()
    plt.savefig(GRAPHS_DIR / "message_rate.png")
    plt.savefig(GRAPHS_DIR / "message_rate.pdf")
    print("  Saved: message_rate.png/pdf")
    plt.close()


def plot_summary_comparison(results: list[dict]):
    """Create a summary comparison chart."""
    # Group by payload size, take 3-client results
    data = [r for r in results if r["num_clients"] == 3]
    data.sort(key=lambda x: x["payload_size"])

    if len(data) < 3:
        print("Not enough data for summary plot")
        return

    # Select representative payload sizes
    small = next((r for r in data if r["payload_size"] <= 1000), None)
    medium = next((r for r in data if 10000 <= r["payload_size"] <= 50000), None)
    large = next((r for r in data if r["payload_size"] >= 100000), None)

    selected = [r for r in [small, medium, large] if r is not None]
    if len(selected) < 2:
        print("Not enough varied data for summary")
        return

    categories = [f"{r['payload_size']:,}\nvalues/msg" for r in selected]
    throughputs = [r["avg_throughput_mbps"] for r in selected]
    msg_rates = [r["messages_per_second"] for r in selected]

    x = np.arange(len(categories))
    width = 0.35

    fig, ax1 = plt.subplots(figsize=(10, 6))

    color1 = "#2ecc71"
    bars1 = ax1.bar(
        x - width / 2,
        throughputs,
        width,
        label="Throughput (MB/s)",
        color=color1,
        alpha=0.8,
    )
    ax1.set_ylabel("Throughput (MB/s)", color=color1, fontsize=12)
    ax1.tick_params(axis="y", labelcolor=color1)

    ax2 = ax1.twinx()
    color2 = "#e74c3c"
    bars2 = ax2.bar(
        x + width / 2,
        msg_rates,
        width,
        label="Message Rate (msg/s)",
        color=color2,
        alpha=0.8,
    )
    ax2.set_ylabel("Message Rate (msg/s)", color=color2, fontsize=12)
    ax2.tick_params(axis="y", labelcolor=color2)

    ax1.set_xlabel("Payload Size Configuration", fontsize=12)
    ax1.set_title(
        "ENRGDAQ Performance Summary\n(3 concurrent clients, 10-second benchmark)",
        fontsize=14,
    )
    ax1.set_xticks(x)
    ax1.set_xticklabels(categories)

    # Add value labels on bars
    for bar, val in zip(bars1, throughputs):
        ax1.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 5,
            f"{val:.0f}",
            ha="center",
            va="bottom",
            fontsize=9,
            color=color1,
        )

    for bar, val in zip(bars2, msg_rates):
        ax2.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 2,
            f"{val:.0f}",
            ha="center",
            va="bottom",
            fontsize=9,
            color=color2,
        )

    # Combined legend
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper right")

    plt.tight_layout()
    plt.savefig(GRAPHS_DIR / "performance_summary.png")
    plt.savefig(GRAPHS_DIR / "performance_summary.pdf")
    print("  Saved: performance_summary.png/pdf")
    plt.close()


def generate_all_plots():
    """Generate all plots from saved results."""
    GRAPHS_DIR.mkdir(exist_ok=True)

    print("\nLoading results...")
    results = load_results()
    print(f"Loaded {len(results)} benchmark results")

    print("\nGenerating plots...")
    plot_throughput_vs_payload(results)
    plot_peak_throughput(results)
    plot_requirements_comparison(results)
    plot_single_client_performance(results)
    plot_message_rate(results)
    plot_summary_comparison(results)

    print(f"\nAll plots saved to: {GRAPHS_DIR}/")


def main():
    parser = argparse.ArgumentParser(
        description="ENRGDAQ Benchmark Visualization Suite"
    )
    parser.add_argument(
        "--run", action="store_true", help="Run benchmarks and save data"
    )
    parser.add_argument(
        "--plot", action="store_true", help="Generate graphs from saved data"
    )
    parser.add_argument(
        "--all", action="store_true", help="Run benchmarks and generate graphs"
    )

    args = parser.parse_args()

    if not any([args.run, args.plot, args.all]):
        parser.print_help()
        sys.exit(1)

    if args.run or args.all:
        print("=" * 60)
        print("ENRGDAQ Benchmark Suite")
        print("=" * 60)
        run_benchmarks()

    if args.plot or args.all:
        print("\n" + "=" * 60)
        print("Generating Publication Graphs")
        print("=" * 60)
        generate_all_plots()


if __name__ == "__main__":
    main()
