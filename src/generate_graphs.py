#!/usr/bin/env python3
"""
Performance Profiling Visualization Script
Generates graphs for DFS traversal performance analysis
"""

import matplotlib.pyplot as plt
import numpy as np

# Performance data from profiling results
threads = [2, 4, 8, 16]
T_P = [0.003119, 0.004229, 0.008202, 0.009997]  # seconds
speedup = [0.1829, 0.1349, 0.0696, 0.0571]
efficiency = [0.0915, 0.0337, 0.0087, 0.0036]
T_S = 0.000571  # serial time in seconds

# Convert to milliseconds for better readability
T_P_ms = [t * 1000 for t in T_P]
T_S_ms = T_S * 1000

# Create figure with subplots
fig = plt.figure(figsize=(16, 12))

# Set style
plt.style.use('seaborn-v0_8-darkgrid' if 'seaborn-v0_8-darkgrid' in plt.style.available else 'default')

# 1. Execution Time Comparison (Serial vs Parallel)
ax1 = plt.subplot(2, 2, 1)
ax1.bar(['Serial'], [T_S_ms], color='green', alpha=0.7, label='Serial (T_S)')
ax1.bar(threads, T_P_ms, color='red', alpha=0.7, label='Parallel (T_P)')
ax1.axhline(y=T_S_ms, color='green', linestyle='--', linewidth=2, label='Serial Baseline')
ax1.set_xlabel('Number of Threads', fontsize=12, fontweight='bold')
ax1.set_ylabel('Execution Time (milliseconds)', fontsize=12, fontweight='bold')
ax1.set_title('Execution Time: Serial vs Parallel', fontsize=14, fontweight='bold')
ax1.legend(fontsize=10)
ax1.grid(True, alpha=0.3)
ax1.set_xticks([0] + threads)
ax1.set_xticklabels(['Serial'] + [f'{t}' for t in threads])

# Add value labels on bars
for i, (t, val) in enumerate(zip(threads, T_P_ms)):
    ax1.text(t, val, f'{val:.3f}ms', ha='center', va='bottom', fontsize=9)
ax1.text(0, T_S_ms, f'{T_S_ms:.3f}ms', ha='center', va='bottom', fontsize=9, color='green')

# 2. Speedup Analysis
ax2 = plt.subplot(2, 2, 2)
ax2.plot(threads, speedup, 'o-', linewidth=2.5, markersize=10, color='blue', label='Measured Speedup')
ax2.axhline(y=1.0, color='red', linestyle='--', linewidth=2, label='No Speedup (Baseline)')
# Ideal linear speedup
ideal_speedup = threads
ax2.plot(threads, ideal_speedup, '--', linewidth=2, color='gray', alpha=0.5, label='Ideal Linear Speedup')
ax2.set_xlabel('Number of Threads (p)', fontsize=12, fontweight='bold')
ax2.set_ylabel('Speedup (S = T_S / T_P)', fontsize=12, fontweight='bold')
ax2.set_title('Speedup vs Number of Threads', fontsize=14, fontweight='bold')
ax2.legend(fontsize=10)
ax2.grid(True, alpha=0.3)
ax2.set_xticks(threads)

# Add value labels
for t, s in zip(threads, speedup):
    ax2.text(t, s, f'{s:.3f}', ha='center', va='bottom', fontsize=9)

# Add annotation about negative speedup
ax2.text(0.5, 0.95, 'Speedup < 1.0 indicates\nparallel version is slower', 
         transform=ax2.transAxes, fontsize=10, 
         bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.3),
         verticalalignment='top', ha='center')

# 3. Efficiency Analysis
ax3 = plt.subplot(2, 2, 3)
efficiency_percent = [e * 100 for e in efficiency]
ax3.plot(threads, efficiency_percent, 's-', linewidth=2.5, markersize=10, color='purple', label='Measured Efficiency')
ax3.axhline(y=100, color='green', linestyle='--', linewidth=2, label='Ideal Efficiency (100%)')
ax3.fill_between(threads, efficiency_percent, 0, alpha=0.3, color='purple')
ax3.set_xlabel('Number of Threads (p)', fontsize=12, fontweight='bold')
ax3.set_ylabel('Efficiency (%)', fontsize=12, fontweight='bold')
ax3.set_title('Efficiency vs Number of Threads', fontsize=14, fontweight='bold')
ax3.legend(fontsize=10)
ax3.grid(True, alpha=0.3)
ax3.set_xticks(threads)
ax3.set_ylim([0, max(efficiency_percent) * 1.2])

# Add value labels
for t, e in zip(threads, efficiency_percent):
    ax3.text(t, e, f'{e:.2f}%', ha='center', va='bottom', fontsize=9)

# 4. Performance Degradation Factor (how many times slower than serial)
ax4 = plt.subplot(2, 2, 4)
degradation_factor = [1.0 / s for s in speedup]  # How many times slower than serial
ax4.bar(threads, degradation_factor, color='orange', alpha=0.7, edgecolor='darkorange', linewidth=2)
ax4.axhline(y=1.0, color='red', linestyle='--', linewidth=2, label='Serial Performance')
ax4.set_xlabel('Number of Threads (p)', fontsize=12, fontweight='bold')
ax4.set_ylabel('Performance Degradation Factor', fontsize=12, fontweight='bold')
ax4.set_title('How Many Times Slower Than Serial', fontsize=14, fontweight='bold')
ax4.legend(fontsize=10)
ax4.grid(True, alpha=0.3, axis='y')
ax4.set_xticks(threads)

# Add value labels
for t, d in zip(threads, degradation_factor):
    ax4.text(t, d, f'{d:.1f}x', ha='center', va='bottom', fontsize=10, fontweight='bold')

plt.tight_layout(pad=3.0)

# Save figure
output_file = 'docs/performance_graphs.png'
plt.savefig(output_file, dpi=300, bbox_inches='tight')
print(f"Graphs saved to {output_file}")

# Also create individual graphs for better clarity
fig2, axes = plt.subplots(1, 3, figsize=(18, 5))

# Individual graph 1: Execution Time
axes[0].bar(['Serial'], [T_S_ms], color='green', alpha=0.7, width=0.5)
axes[0].bar(threads, T_P_ms, color='red', alpha=0.7, width=0.5)
axes[0].axhline(y=T_S_ms, color='green', linestyle='--', linewidth=2)
axes[0].set_xlabel('Number of Threads', fontsize=12, fontweight='bold')
axes[0].set_ylabel('Execution Time (ms)', fontsize=12, fontweight='bold')
axes[0].set_title('Execution Time Comparison', fontsize=14, fontweight='bold')
axes[0].grid(True, alpha=0.3, axis='y')
axes[0].set_xticks([0] + threads)
axes[0].set_xticklabels(['Serial'] + [f'{t}' for t in threads])
for i, (t, val) in enumerate(zip(threads, T_P_ms)):
    axes[0].text(t, val, f'{val:.3f}', ha='center', va='bottom', fontsize=9)
axes[0].text(0, T_S_ms, f'{T_S_ms:.3f}', ha='center', va='bottom', fontsize=9, color='green')

# Individual graph 2: Speedup
axes[1].plot(threads, speedup, 'o-', linewidth=3, markersize=12, color='blue', label='Measured')
axes[1].plot(threads, ideal_speedup, '--', linewidth=2, color='gray', alpha=0.6, label='Ideal (Linear)')
axes[1].axhline(y=1.0, color='red', linestyle=':', linewidth=2, label='Baseline')
axes[1].set_xlabel('Number of Threads (p)', fontsize=12, fontweight='bold')
axes[1].set_ylabel('Speedup (S)', fontsize=12, fontweight='bold')
axes[1].set_title('Speedup Analysis', fontsize=14, fontweight='bold')
axes[1].legend(fontsize=10)
axes[1].grid(True, alpha=0.3)
axes[1].set_xticks(threads)
for t, s in zip(threads, speedup):
    axes[1].text(t, s, f'{s:.3f}', ha='center', va='bottom', fontsize=9)

# Individual graph 3: Efficiency
axes[2].plot(threads, efficiency_percent, 's-', linewidth=3, markersize=12, color='purple', label='Measured')
axes[2].axhline(y=100, color='green', linestyle='--', linewidth=2, label='Ideal (100%)')
axes[2].fill_between(threads, efficiency_percent, 0, alpha=0.3, color='purple')
axes[2].set_xlabel('Number of Threads (p)', fontsize=12, fontweight='bold')
axes[2].set_ylabel('Efficiency (%)', fontsize=12, fontweight='bold')
axes[2].set_title('Efficiency Analysis', fontsize=14, fontweight='bold')
axes[2].legend(fontsize=10)
axes[2].grid(True, alpha=0.3)
axes[2].set_xticks(threads)
axes[2].set_ylim([0, max(efficiency_percent) * 1.3])
for t, e in zip(threads, efficiency_percent):
    axes[2].text(t, e, f'{e:.2f}%', ha='center', va='bottom', fontsize=9)

plt.tight_layout()
plt.savefig('docs/performance_graphs_individual.png', dpi=300, bbox_inches='tight')
print(f"Individual graphs saved to docs/performance_graphs_individual.png")

# Create a comprehensive comparison chart
fig3, ax = plt.subplots(figsize=(12, 8))

x = np.arange(len(threads))
width = 0.35

# Normalize values for comparison (show relative performance)
bars1 = ax.bar(x - width/2, speedup, width, label='Speedup (S)', color='blue', alpha=0.7)
bars2 = ax.bar(x + width/2, efficiency, width, label='Efficiency (E)', color='purple', alpha=0.7)

ax.set_xlabel('Number of Threads (p)', fontsize=12, fontweight='bold')
ax.set_ylabel('Normalized Performance Metric', fontsize=12, fontweight='bold')
ax.set_title('Speedup and Efficiency Comparison', fontsize=14, fontweight='bold')
ax.set_xticks(x)
ax.set_xticklabels(threads)
ax.axhline(y=1.0, color='red', linestyle='--', linewidth=2, label='Baseline (Serial)')
ax.legend(fontsize=11)
ax.grid(True, alpha=0.3, axis='y')

# Add value labels
for bars in [bars1, bars2]:
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.3f}',
                ha='center', va='bottom', fontsize=9)

plt.tight_layout()
plt.savefig('docs/performance_graphs_comparison.png', dpi=300, bbox_inches='tight')
print(f"Comparison chart saved to docs/performance_graphs_comparison.png")

plt.show()

print("\nAll graphs generated successfully!")
print("\nGenerated files:")
print("  - docs/performance_graphs.png (4-panel overview)")
print("  - docs/performance_graphs_individual.png (3 individual charts)")
print("  - docs/performance_graphs_comparison.png (speedup vs efficiency)")

