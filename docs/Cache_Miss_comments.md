## Cache Miss and Locality Analysis (from gprof)

**Serial Version:**

- 249,995 recursive calls cause deep stack with poor cache locality
- 133,417 vector reallocations trigger frequent cache invalidations
- Random graph traversal has poor spatial locality

**Parallel Version:**

- 133,414 vector reallocations (similar overhead)
- False sharing: critical sections on shared `visited` array cause cache line invalidation between threads
- Cache coherency overhead from multiple threads accessing same memory locations

**Main Issues:** Deep recursion (stack cache misses), random memory access patterns, and false sharing in parallel version limit cache efficiency.
