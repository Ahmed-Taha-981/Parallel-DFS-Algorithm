# Performance Profiling Report: DFS Traversal

## Executive Summary

This report presents the performance analysis of the parallel Depth-First Search (DFS) traversal implementation using OpenMP. We measure serial execution time (T_S) and parallel execution times (T_P) for different thread counts (2, 4, 8, 16), compute speedup and efficiency metrics, and analyze the results using Amdahl's Law.

**Key Finding**: The parallel implementation demonstrates **negative speedup** (speedup < 1.0) across all thread counts tested, with the serial version outperforming the parallel version by 5.5× to 17.5×. This is primarily due to excessive synchronization overhead from critical sections used to prevent race conditions. The implementation correctly avoids race conditions but at a significant performance cost. For graph sizes of 10,000 vertices or smaller, the serial implementation is recommended.

---

## Methodology

### Test Configuration
- **Graph Size**: 10,000 vertices
- **Algorithm**: Depth-First Search (DFS) traversal
- **Thread Counts Tested**: 2, 4, 8, 16
- **Iterations**: 5 runs per configuration (averaged)
- **Compiler Flags**: `-fopenmp -O2 -std=c++11`
- **System**: WSL (Windows Subsystem for Linux)

**Note on Graph Size**: Initial tests with 50,000 vertices resulted in stack overflow errors due to deep recursion. The graph size was reduced to 10,000 vertices for successful execution. This limitation highlights a constraint of the recursive DFS approach in parallel environments.

### Performance Metrics
1. **T_S**: Serial execution time (baseline)
2. **T_P(p)**: Parallel execution time with p threads
3. **Speedup**: S(p) = T_S / T_P(p)
4. **Efficiency**: E(p) = S(p) / p

---

## Experimental Results

### Performance Data

| Threads (p) | T_P (seconds) | Speedup (S) | Efficiency (E) |
|-------------|---------------|-------------|----------------|
| 2           | 0.003119      | 0.1829      | 0.0915 (9.15%) |
| 4           | 0.004229      | 0.1349      | 0.0337 (3.37%) |
| 8           | 0.008202      | 0.0696      | 0.0087 (0.87%) |
| 16          | 0.009997      | 0.0571      | 0.0036 (0.36%) |

**Serial Time (T_S)**: 0.000571 seconds (0.571 milliseconds)

### Visualizations

The performance data is visualized in the following graphs:

1. **Execution Time Comparison**: Shows serial vs parallel execution times, demonstrating that parallel versions take significantly longer
2. **Speedup Analysis**: Compares measured speedup against ideal linear speedup, showing negative speedup (S < 1.0)
3. **Efficiency Analysis**: Shows efficiency degradation as thread count increases
4. **Performance Degradation Factor**: Illustrates how many times slower the parallel version is compared to serial
5. **Combined Comparison**: Side-by-side comparison of speedup and efficiency metrics

**Interactive Graphs**: Open `performance_graphs.html` in a web browser to view interactive visualizations of all performance metrics.

**Note**: If you have Python installed, you can also generate static PNG graphs by running:
```bash
python src/generate_graphs.py
```
This will generate high-resolution PNG images saved to the `docs/` directory.

---

## Analysis Using Amdahl's Law

### Amdahl's Law Formula

Amdahl's Law predicts the maximum speedup achievable when parallelizing a program:

**S_max(p) = 1 / (f_serial + f_parallel / p)**

Where:
- **p**: Number of processors/threads
- **f_serial**: Fraction of execution time that is inherently serial (cannot be parallelized)
- **f_parallel**: Fraction of execution time that can be parallelized (f_serial + f_parallel = 1)

### Determining Serial Fraction

From our measurements, we can estimate the serial fraction:

**f_serial ≈ 1 - (S_measured(p) / p × Efficiency_measured(p))**

Or more directly from speedup:
- If speedup is sublinear, it indicates serial bottlenecks
- The serial fraction limits maximum achievable speedup

### Expected Observations

For the DFS implementation analyzed:

1. **Serial Components**:
   - Graph creation (performed once)
   - Critical sections protecting shared data structures (`visited`, `res`)
   - Task creation overhead
   - Synchronization (taskwait, critical sections)

2. **Parallel Components**:
   - Independent DFS traversals from different starting points
   - Work computation within each recursive call
   - Task-based parallel execution

3. **Bottlenecks**:
   - **Critical sections**: Frequent access to `visited` and `res` arrays creates contention
   - **Load imbalance**: Graph structure may create uneven work distribution
   - **Task overhead**: Creating and managing OpenMP tasks has overhead

---

## Interpretation of Results

### Speedup Analysis

**Ideal Speedup**: Linear speedup would show S(p) = p

**Actual Behavior Observed**:
- **p = 2**: Speedup = 0.18 (parallel version is 5.5x **slower** than serial)
- **p = 4**: Speedup = 0.13 (parallel version is 7.4x **slower** than serial)
- **p = 8**: Speedup = 0.07 (parallel version is 14.4x **slower** than serial)
- **p = 16**: Speedup = 0.06 (parallel version is 17.5x **slower** than serial)

**Critical Finding**: The parallel implementation performs worse than the serial version across all thread counts. This indicates that the **parallelization overhead exceeds the computational benefits** for this problem size and implementation approach.

### Efficiency Analysis

**Ideal Efficiency**: E(p) = 1.0 (100%) indicates perfect parallelization

**Actual Behavior Observed**:
- **p = 2**: E = 0.0915 (9.15% efficiency)
- **p = 4**: E = 0.0337 (3.37% efficiency)
- **p = 8**: E = 0.0087 (0.87% efficiency)
- **p = 16**: E = 0.0036 (0.36% efficiency)

**Analysis**: Extremely low efficiency values (all below 10%) indicate severe performance degradation. The primary causes include:
- **Excessive synchronization overhead**: Critical sections dominate execution time
- **Task creation overhead**: OpenMP task management costs exceed benefits
- **Serial bottlenecks**: Frequent sequential access to shared data structures
- **Cache contention**: Multiple threads competing for the same memory locations

### Scalability Limits

**Amdahl's Law Limit**: 
The maximum theoretical speedup is bounded by:
**S_max = 1 / f_serial**

If f_serial = 0.1 (10% serial), maximum speedup = 10x
If f_serial = 0.2 (20% serial), maximum speedup = 5x

**Practical Observations**:
1. **Sublinear Speedup**: As threads increase, speedup increases but at a decreasing rate
2. **Efficiency Degradation**: Efficiency decreases as more threads are added
3. **Plateau Effect**: Beyond a certain point, adding more threads provides minimal benefit

---

## Discussion

### Race Condition Prevention

**Implementation Update**: The parallel DFS implementation was updated to properly avoid race conditions:

1. **Critical Section Protection**: All accesses to shared data structures (`visited` array and `res` vector) are protected by `#pragma omp critical` directives
2. **Atomic Check-and-Set**: The pattern of checking if a vertex is visited and marking it as visited is done atomically within critical sections
3. **Thread Safety**: The implementation ensures that:
   - No two threads can simultaneously mark the same vertex as visited
   - The result vector is safely updated without data races
   - All memory operations are properly synchronized

**Trade-off**: While these critical sections successfully prevent race conditions, they create a significant performance bottleneck. Every vertex visit requires acquiring a lock, which serializes much of the computation and explains the poor parallel performance observed.

### Factors Affecting Performance

1. **Critical Section Contention**
   - The DFS implementation uses `#pragma omp critical` to protect shared data
   - Frequent critical section access creates serial bottlenecks
   - This is a primary limiting factor for speedup

2. **Task Creation Overhead**
   - OpenMP task creation has overhead
   - For fine-grained tasks, overhead may exceed benefits
   - Coarser-grained parallelization might be more efficient

3. **Load Imbalance**
   - Graph structure may create uneven work distribution
   - Some threads may finish earlier and wait (idle time)
   - Dynamic load balancing can help but adds overhead

4. **Memory Access Patterns**
   - Contention for shared memory structures
   - Cache coherency overhead
   - False sharing potential

### Comparison with Theoretical Limits

**Amdahl's Law Prediction vs. Actual**:
- Calculate theoretical speedup based on estimated serial fraction
- Compare with measured speedup
- Identify additional overhead factors not captured by Amdahl's Law (e.g., synchronization, memory access)

### Scalability Discussion

**Strong Scaling**: 
- How does performance change when problem size is fixed and threads increase?
- Our analysis shows strong scaling behavior

**Weak Scaling** (future work):
- How does performance change when problem size scales with thread count?
- This would show if the algorithm scales to larger graphs

**Scalability Limits**:
- The algorithm will hit Amdahl's Law limits
- To improve: reduce serial fraction (fewer critical sections, better algorithms)
- To improve: reduce overhead (better task granularity, optimized synchronization)

---

## Conclusions

### Key Findings

1. **Negative Speedup**: The parallel implementation is consistently slower than the serial version across all thread counts (speedup < 1.0)
2. **Extremely Low Efficiency**: Efficiency ranges from 9.15% (2 threads) down to 0.36% (16 threads)
3. **Performance Degradation**: As thread count increases, performance worsens due to increased synchronization overhead
4. **Overhead Dominance**: The overhead from task creation, critical sections, and synchronization far exceeds any parallel benefit
5. **Conclusion**: **For this graph size (10,000 vertices), the serial implementation is superior**

### Recommendations

1. **Use Serial Implementation for Small Graphs**:
   - For graphs with ≤10,000 vertices, the serial version is more efficient
   - Avoid parallel overhead for problems where computation time is minimal

2. **Test with Larger Graphs**:
   - The parallel version may show benefits with significantly larger graphs (100,000+ vertices)
   - Increased problem size may amortize the parallelization overhead
   - Stack overflow issues need to be addressed for very large graphs

3. **Reduce Synchronization Overhead**:
   - Minimize critical section usage by using thread-local data structures
   - Consider lock-free data structures or atomic operations
   - Batch operations to reduce synchronization frequency

4. **Optimize Task Granularity**:
   - Create coarser-grained tasks to reduce OpenMP task overhead
   - Consider minimum subtree size thresholds before spawning new tasks
   - Balance between parallelism and overhead

5. **Alternative Approaches**:
   - Consider BFS (Breadth-First Search) which has better parallelization characteristics
   - Use graph partitioning to create independent work units
   - Implement iterative DFS to avoid stack overflow issues

6. **Race Condition Prevention**:
   - The current implementation correctly avoids race conditions through critical sections
   - However, these critical sections are the primary performance bottleneck
   - Future implementations should explore alternative synchronization strategies

---

## Appendix: Running the Profiling

### Compilation
```bash
g++ -fopenmp -O2 -std=c++11 src/profile.cpp -o profile.exe
```

### Execution
```bash
./profile.exe
```

### Output
The program generates:
1. Console output with detailed timing information
2. `performance_results.txt` file with tabular data

### Updating This Report
Replace the [TBD] placeholders in the "Experimental Results" section with actual measurements from the profiling run.

---

## References

1. Amdahl, G. M. (1967). "Validity of the single processor approach to achieving large scale computing capabilities". AFIPS Conference Proceedings.
2. OpenMP Architecture Review Board. OpenMP Application Programming Interface.
3. Herlihy, M., & Shavit, N. (2012). The Art of Multiprocessor Programming.

