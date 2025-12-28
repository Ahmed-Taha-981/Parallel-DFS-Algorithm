"""
Performance Analysis Graph Generator
reads CSV logs from streaming_client.py -> Generates Throughput & Latency plots
"""

import pandas as pd
import matplotlib.pyplot as plt
import glob
import os
import argparse
import datetime

def generate_graphs(log_dir):
    # 1. Read CSV log(s)
    # Support both directory of CSVs (Spark) and single CSV (Simple Client)
    if os.path.isfile(os.path.join(log_dir, "streaming_events.csv")):
        df = pd.read_csv(os.path.join(log_dir, "streaming_events.csv"))
    else:
        all_files = glob.glob(os.path.join(log_dir, "*.csv"))
        if not all_files:
            print(f"Error: No CSV files found in {log_dir}")
            return
        print(f"Reading {len(all_files)} log files...")
        df = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)
    
    # Ensure numeric types
    df['total_latency_ms'] = pd.to_numeric(df['total_latency_ms'], errors='coerce').fillna(0)

    
    # Convert timestamps
    df['processing_timestamp'] = pd.to_datetime(df['processing_timestamp'])
    
    # Sort by time
    df = df.sort_values('processing_timestamp')
    
    # Determine experiment start time (relative time 0)
    start_time = df['processing_timestamp'].min()
    df['seconds_elapsed'] = (df['processing_timestamp'] - start_time).dt.total_seconds()

    # --- METRIC 1: THROUGHPUT (Requests per Second) ---
    # Bin data by 1-second intervals
    df['time_bin'] = df['seconds_elapsed'].astype(int)
    throughput = df.groupby('time_bin').size()
    
    # --- METRIC 2: LATENCY (p95 per Second) ---
    latency_p95 = df.groupby('time_bin')['total_latency_ms'].quantile(0.95)
    
    # --- AUTOMATIC METRIC DETECTION ---
    # 1. Baseline Latency (first 5 seconds)
    baseline_window = df[df['seconds_elapsed'] < 5]
    if not baseline_window.empty:
        baseline_latency = baseline_window['total_latency_ms'].mean()
    else:
        baseline_latency = 10.0 # Fallback
        
    print(f"  - Baseline Latency: {baseline_latency:.2f} ms")

    # 2. Detect Crash Time (First Failure or Huge Latency Spike)
    # We look for status=FAILED or latency > 10x baseline
    crash_candidates = df[
        (df['status'] == 'FAILED') | 
        (df['total_latency_ms'] > baseline_latency * 10)
    ]
    
    crash_time = None
    if not crash_candidates.empty:
        crash_time = crash_candidates['seconds_elapsed'].min()
        print(f"  - Detected Crash Time: T+{crash_time:.2f}s")
    else:
        print("  - No crash detected (no failures or high latency spikes)")

    # 3. Detect Recovery Time
    # First SUCCESS after crash_time where latency returns to near baseline (< 2x baseline)
    recovery_time = None
    recovery_duration = 0.0
    
    if crash_time is not None:
        post_crash = df[
            (df['seconds_elapsed'] > crash_time) & 
            (df['status'] == 'SUCCESS') & 
            (df['total_latency_ms'] < baseline_latency * 2.0)
        ]
        if not post_crash.empty:
            recovery_time = post_crash['seconds_elapsed'].min()
            recovery_duration = recovery_time - crash_time
            print(f"  - Detected Recovery Time: T+{recovery_time:.2f}s")
            print(f"  - Calculated Recovery Duration: {recovery_duration:.2f}s")
        else:
            print("  - System did not recover within the experiment duration")

    # --- PLOTTING ---
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 10), sharex=True)
    
    # Plot Throughput
    ax1.plot(throughput.index, throughput.values, marker='o', linestyle='-', color='b', label='Throughput')
    ax1.set_title('System Throughput (Req/sec)', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Requests/sec')
    ax1.grid(True, alpha=0.3)
    
    # Plot P95 Latency
    ax2.plot(latency_p95.index, latency_p95.values, marker='x', linestyle='-', color='r', label='P95 Latency')
    ax2.set_title('P95 Latency (ms)', fontsize=12, fontweight='bold')
    ax2.set_xlabel('Time (seconds)')
    ax2.set_ylabel('Latency (ms)')
    ax2.grid(True, alpha=0.3)
    
    # --- ANNOTATIONS ---
    if crash_time:
        # Vertical line for Crash
        ax1.axvline(x=crash_time, color='red', linestyle='--', alpha=0.7, label='Crash Injection')
        ax2.axvline(x=crash_time, color='red', linestyle='--', alpha=0.7, label='Crash Injection')
        ax2.text(crash_time, ax2.get_ylim()[1]*0.9, ' CRASH', color='red', fontweight='bold')
        
        if recovery_time:
            # Vertical line for Recovery
            ax1.axvline(x=recovery_time, color='green', linestyle='--', alpha=0.7, label='Recovery Point')
            ax2.axvline(x=recovery_time, color='green', linestyle='--', alpha=0.7, label='Recovery Point')
            
            # Region shading
            ax2.axvspan(crash_time, recovery_time, color='yellow', alpha=0.1)
            
            # Annotation text
            ax2.annotate(
                f'Recovery Time: {recovery_duration:.2f}s', 
                xy=(recovery_time, baseline_latency), 
                xytext=(recovery_time + 5, baseline_latency * 5),
                arrowprops=dict(facecolor='black', shrink=0.05)
            )

    ax1.legend(loc='upper right')
    ax2.legend(loc='upper right')
    
    plt.tight_layout()
    output_file = 'performance_analysis.png'
    plt.savefig(output_file, dpi=300)
    print(f"Graphs generated: {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--log-dir', type=str, default='streaming_logs', help='Directory containing CSV logs')
    args = parser.parse_args()
    
    generate_graphs(args.log_dir)
