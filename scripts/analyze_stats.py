#!/usr/bin/env python3
"""
CAEN Digitizer Acquisition Stats Analyzer

Generates multi-panel visualizations for long-term acquisition statistics.
Usage: python analyze_stats.py <path_to_stats.csv>
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd


def load_stats(csv_path: str) -> pd.DataFrame:
    """Load and preprocess stats CSV."""
    df = pd.read_csv(csv_path)
    
    # Convert timestamp (unix ms) to datetime
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
    
    # Compute derived metrics
    # 1. Inter-arrival Times (dt)
    # Calculate time difference in seconds between consecutive events
    # We use 'timestamp' (ms). Diff gives ms. /1000 -> seconds.
    df['dt_s'] = df['timestamp'].diff() / 1000.0
    
    # 2. Dead Time Fraction
    # Approximated by (1 - RawRate / TheoreticalMax).
    # Theoretical Max for CAEN varies, but if we assume Raw Rate is the "Input Rate",
    # and we want to see how close we are to saturation.
    # Actually, a better proxy for "System Busy" in this context might be simpler:
    # Just show the Raw Rate itself as "Input Flux".
    df['raw_rate_hz'] = df['total_samples_raw']  # Since it's per second
    
    # 3. RMS (Energy/Stability)
    df['rms_mv'] = np.sqrt(df['sum_value_mv_squared'] / df['acq_samples'].replace(0, np.nan))
    
    # 4. Average Event Size (Samples per Event) - Pulse Width Proxy
    df['avg_event_size'] = df['acq_samples'] / df['acq_events'].replace(0, np.nan)
    
    # 5. Signal-to-Noise Ratio (SNR)
    noise = df['rms_mv'].replace(0, np.nan)
    signal_amp = (df['min_value_mv'] - df['mean_value_mv']).abs()
    df['snr'] = signal_amp / noise

    # 6. Bandwidth (KB/s) - Keep for summary but maybe not plot
    df['bandwidth_kbps'] = (df['acq_samples'] * 2) / 1024
    
    return df


def plot_time_series(ax, df: pd.DataFrame, col: str, label: str, color: str, 
                     ylabel: str = None, alpha: float = 0.7, legend_loc: str = 'upper right'):
    """Plot a time series with optional smoothing overlay."""
    ax.plot(df['datetime'], df[col], color=color, alpha=0.3, linewidth=0.5)
    # Rolling mean for trend
    if len(df) > 60:
        smooth = df[col].rolling(window=60, center=True).mean()
        ax.plot(df['datetime'], smooth, color=color, alpha=1.0, linewidth=1.5, label=f'{label} (1-min avg)')
    ax.set_ylabel(ylabel or label)
    ax.legend(loc=legend_loc, fontsize=8)
    ax.grid(True, alpha=0.3)


def create_analysis_figure(df: pd.DataFrame, output_path: str = None):
    """Create the WCD Physics Dashboard (3x2 grid)."""
    
    fig, axes = plt.subplots(3, 2, figsize=(16, 15))
    fig.suptitle(f'WCD Physics Dashboard ({len(df)} samples, {len(df)//3600:.1f} hours)', 
                 fontsize=16, fontweight='bold')
    
    # Plot 1: Event Rate & Input Flux (Trigger Stability)
    ax1 = axes[0, 0]
    plot_time_series(ax1, df, 'acq_events', 'Trigger Rate', '#2196F3', 'Hz', legend_loc='upper left')
    ax1_twin = ax1.twinx()
    # Normalize Raw Rate to MHz for readability
    df['raw_rate_mhz'] = df['raw_rate_hz'] / 1e6
    plot_time_series(ax1_twin, df, 'raw_rate_mhz', 'Input Rate', '#E91E63', 'MHz', legend_loc='upper right')
    ax1.set_title('Event Rate & Input Flux', fontweight='bold')
    
    # Plot 2: Baseline & Noise (Electronics Stability)
    ax2 = axes[0, 1]
    plot_time_series(ax2, df, 'mean_value_mv', 'Baseline', '#9C27B0', 'mV', legend_loc='upper left')
    ax2_twin = ax2.twinx()
    plot_time_series(ax2_twin, df, 'rms_mv', 'RMS Noise', '#FF9800', 'mV', legend_loc='upper right')
    ax2.set_title('Baseline Stability & Noise', fontweight='bold')

    # Plot 3: Amplitude Spectrum (The Physics Signal)
    ax3 = axes[1, 0]
    bins = np.linspace(-1000, 200, 121) 
    ax3.hist(df['min_value_mv'].clip(-1000, 200), bins=bins, alpha=0.7, color='#E91E63', label='Peak Min', density=True)
    ax3.set_xlabel('Amplitude (mV)')
    ax3.set_ylabel('Density')
    ax3.set_yscale('log')  # Log scale to see cosmic tail vs noise
    ax3.set_title('Amplitude Spectrum (Log Scale)', fontweight='bold')
    ax3.legend(loc='upper right', fontsize=8)
    ax3.grid(True, alpha=0.3)

    # Plot 4: Inter-arrival Times (Noise Hunt)
    ax4 = axes[1, 1]
    # Filter valid dt
    valid_dt = df['dt_s'][df['dt_s'] > 0]
    if len(valid_dt) > 0:
        log_dt = np.log10(valid_dt)
        ax4.hist(log_dt, bins=50, color='#607D8B', alpha=0.7, density=True)
        ax4.set_xlabel('log10(dt) [seconds]')
        ax4.set_title('Inter-arrival Time Distribution', fontweight='bold')
        # Add labels for 60Hz/50Hz roughly -1.2 to -1.3 log seconds
        # -1 = 0.1s, -2 = 0.01s (10ms). 16ms is ~ -1.8
        ax4.axvline(np.log10(0.016), color='red', linestyle=':', alpha=0.5, label='60Hz')
        ax4.axvline(np.log10(0.020), color='orange', linestyle=':', alpha=0.5, label='50Hz')
        ax4.legend(fontsize=8)
    
    # Plot 5: Amplitude vs Pulse Width (Particle ID Proxy)
    ax5 = axes[2, 0]
    # We use timestamps to align, but simpler to use scatter of values
    # Since we have time series data, we can just plot the correlation or time series of width
    # Time series of width vs Time series of Amp is hard to read.
    # Let's do a 2D Hist or Scatter. Since we have ~50k points, scatter might be heavy but manageable.
    # We'll use a hexbin for density.
    hb = ax5.hexbin(df['avg_event_size'], df['min_value_mv'], gridsize=30, cmap='inferno', mincnt=1)
    ax5.set_xlabel('Avg Event Width (Samples)')
    ax5.set_ylabel('Peak Amplitude (mV)')
    ax5.set_title('Pulse Shape: Amp vs Width', fontweight='bold')
    cb = fig.colorbar(hb, ax=ax5)
    cb.set_label('Count')
    
    # Plot 6: SNR Trend (Overall Quality)
    ax6 = axes[2, 1]
    plot_time_series(ax6, df, 'snr', 'SNR', '#3F51B5', 'Ratio', legend_loc='upper right')
    ax6.axhline(y=5, color='red', linestyle='--', alpha=0.5, label='Min Detectable')
    ax6.set_title('Signal-to-Noise Ratio', fontweight='bold')
    ax6.legend(loc='lower left', fontsize=8) # Move legend to not block data
    
    
    plt.tight_layout()
    plt.subplots_adjust(top=0.95)
    
    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        print(f"Saved figure to: {output_path}")
    
    return fig


def print_summary(df: pd.DataFrame):
    """Print summary statistics."""
    duration_hours = len(df) / 3600
    
    print("\n" + "="*60)
    print("ACQUISITION STATS SUMMARY")
    print("="*60)
    print(f"Duration: {duration_hours:.2f} hours ({len(df):,} samples)")
    print(f"Time range: {df['datetime'].min()} to {df['datetime'].max()}")
    print()
    
    print("📊 WCD Physics Stats:")
    print(f"  Trigger Rate: {df['acq_events'].mean():,.0f} ± {df['acq_events'].std():,.0f} Hz")
    print(f"  Input Flux:   {(df['raw_rate_hz'].mean()/1e6):.2f} MHz")
    print(f"  Dead Fraction: N/A (Needs Max Rate)")
    print()
    
    print("🔬 Signal & Stability:")
    print(f"  Baseline:     {df['mean_value_mv'].mean():.2f} ± {df['mean_value_mv'].std():.2f} mV")
    print(f"  RMS Noise:    {df['rms_mv'].mean():.2f} ± {df['rms_mv'].std():.2f} mV")
    print(f"  SNR:          {df['snr'].mean():.1f} ± {df['snr'].std():.1f}")
    print(f"  Avg Pulse:    {df['avg_event_size'].mean():.1f} samples ({df['avg_event_size'].mean()*2:.1f} ns)")
    print()
    
    print("📉 Spectrum:")
    print(f"  Peak Range:   {df['min_value_mv'].min()} mV to {df['max_value_mv'].max()} mV")
    print(f"  Data Volume:  {(df['bandwidth_kbps'].mean()/1024):.2f} MB/s")
    print("="*60)


def main():
    parser = argparse.ArgumentParser(description='Analyze CAEN Digitizer acquisition stats')
    parser.add_argument('csv_path', help='Path to stats CSV file')
    parser.add_argument('-o', '--output', help='Output PNG path (default: same as input with .png)')
    parser.add_argument('--no-show', action='store_true', help='Do not display the plot')
    
    args = parser.parse_args()
    
    csv_path = Path(args.csv_path)
    if not csv_path.exists():
        print(f"Error: File not found: {csv_path}")
        sys.exit(1)
    
    output_path = args.output or str(csv_path.with_suffix('.png'))
    
    print(f"Loading {csv_path}...")
    df = load_stats(str(csv_path))
    
    print_summary(df)
    
    print(f"\nGenerating plots...")
    fig = create_analysis_figure(df, output_path)
    
    if not args.no_show:
        plt.show()


if __name__ == '__main__':
    main()
