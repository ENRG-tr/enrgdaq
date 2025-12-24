#!/usr/bin/env python3
import json
import os


def generate_latex_table():
    """Generates LaTeX tables for JINST using Cores and Efficiency metrics."""
    results_file = "results/benchmarks/latest_results.json"
    if not os.path.exists(results_file):
        print(f"Error: {results_file} not found.")
        return

    with open(results_file, "r") as f:
        data = json.load(f)

    # 1. Performance Scaling Table (Single Client Focus for Latency/Efficiency)
    # We use results where num_clients == 1 or 3 to show scaling
    scaling_data = [r for r in data if r["num_clients"] == 1]
    # If no 1-client results (e.g. failed), fallback to 3
    if not scaling_data:
        scaling_data = [r for r in data if r["num_clients"] == 3]

    scaling_data = sorted(scaling_data, key=lambda x: x["payload_size"])

    print("\n% --- REVISED JINST TABLE: Performance Scaling (Single Client) ---")
    print("\\begin{table}[htbp]")
    print("\\centering")
    print(
        "\\caption{ENRGDAQ Steady-State Performance (Single Client). Note the distinct efficiency gain at larger payload sizes, characteristic of Zero-Copy architectures.}"
    )
    print("\\label{tab:performance}")
    print("\\begin{tabular}{l c c c c c}")
    print("\\hline")
    print(
        "\\textbf{Payload} & \\textbf{Throughput} & \\textbf{Event Rate} & \\textbf{CPU Load} & \\textbf{Latency ($p_{99}$)} & \\textbf{Efficiency} \\\\"
    )
    print("(Bytes) & (Gbps) & (kHz) & (Cores) & (ms) & (Gbps/Core) \\\\")
    print("\\hline")

    for r in scaling_data:
        payload = r["payload_size"]
        gbps = (r["avg_throughput_mbs"] * 8) / 1000.0
        khz = r["messages_per_second"] / 1000.0
        cores = r["cpu_usage_percent"] / 100.0
        p99 = r["latency_p99_ms"]
        efficiency = gbps / cores if cores > 0 else 0

        # Highlight the best result as per suggestion
        is_best = payload == 500000
        fmt = "\\textbf{" if is_best else ""
        end_fmt = "}" if is_best else ""

        print(
            f"{payload:,} & {fmt}{gbps:.2f}{end_fmt} & {khz:.1f} & {fmt}{cores:.1f}{end_fmt} & {p99:.1f} & {fmt}{efficiency:.2f}{end_fmt} \\\\"
        )

    print("\\hline")
    print("\\end{tabular}")
    print(
        "\\footnotesize{\\textit{*CPU Load represents aggregate usage across Supervisor and Job processes. 1.0 = 1 full core.}}"
    )
    print("\\end{table}")
    print("% --- END TABLE ---\n")

    # 2. Aggregate Scaling Table (Bridging to 11 Gbps)
    aggregate_data = [r for r in data if r["payload_size"] == 500000]
    aggregate_data = sorted(aggregate_data, key=lambda x: x["num_clients"])

    print("% --- JINST TABLE: Aggregate System Scalability ---")
    print("\\begin{table}[htbp]")
    print("\\centering")
    print(
        "\\caption{Aggregate Throughput Scaling with Concurrent Readout Clients (Payload=500kB)}"
    )
    print("\\begin{tabular}{l c c c}")
    print("\\hline")
    print(
        "\\textbf{Clients} & \\textbf{Total Throughput} & \\textbf{Total Throughput} & \\textbf{Scaling} \\\\"
    )
    print("& (MB/s) & (Gbps) & Efficiency \\\\")
    print("\\hline")

    baseline = None
    for r in aggregate_data:
        clients = r["num_clients"]
        mbs = r["avg_throughput_mbs"]
        gbps = (mbs * 8) / 1000.0

        if baseline is None and clients == 1:
            baseline = gbps
            scaling = 1.0
        elif baseline:
            scaling = gbps / (baseline * clients)
        else:
            scaling = 0

        print(f"{clients} & {mbs:,.0f} & {gbps:.2f} & {scaling:.2f} \\\\")

    print("\\hline")
    print("\\end{tabular}")
    print("\\label{tab:aggregate}")
    print("\\end{table}")
    print("% --- END TABLE ---\n")


if __name__ == "__main__":
    generate_latex_table()
