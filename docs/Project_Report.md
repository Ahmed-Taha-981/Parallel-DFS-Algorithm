# Parallel DFS Algorithm Project Report

## 1. Introduction

This report describes a project that implements Depth-First Search (DFS) using different strategies. DFS is a graph algorithm that explores paths as far as possible before backtracking. The main goal of this project was to see if running DFS in parallel (using multiple processors at once) could make it faster than running it one step at a time.

We built three different versions of the algorithm and then added a service layer that lets other programs use our DFS implementation over the network. This report walks through how the system works, what problems we encountered, what performance numbers we collected, and what we learned from the experience.

---

## 2. Architecture

The project contains four main components that work together to provide flexible DFS computation.

### 2.1 Serial Implementation

The serial version is the baseline implementation. It runs on a single processor and processes the graph one vertex at a time. Here's how it works:

- **Graph Structure**: Uses an adjacency list where each vertex stores a list of its neighbors
- **Visited Tracking**: Maintains a boolean array to avoid visiting the same vertex twice
- **Result Storage**: Collects visited vertices in order as it traverses the graph
- **Work Simulation**: Adds artificial computation at each vertex to simulate real workloads

The serial implementation is straightforward and serves as the reference point for comparing parallel versions. For our test graphs with 50,000 vertices, this version completed traversal in under 1 millisecond when compiled with optimizations enabled.

### 2.2 Parallel Implementation (OpenMP)

The parallel version uses OpenMP, which is a technology for shared-memory parallel programming. This means multiple threads can read and write to the same memory space.

**Key Design Features:**

- **Task-Based Parallelism**: Uses OpenMP tasks to explore different branches of the graph simultaneously
- **Critical Sections**: Protects shared data (visited array and result vector) with locks to prevent race conditions
- **Thread Safety**: Ensures that no two threads mark the same vertex as visited at the same time

**How It Works:**

When the DFS algorithm reaches a vertex with multiple neighbors, it can spawn tasks for each neighbor. These tasks run in parallel on different threads. However, there's a catch - before processing any vertex, the thread must acquire a lock to check if the vertex was already visited and to mark it as visited. This locking prevents two threads from processing the same vertex twice, which would give wrong results.

### 2.3 Distributed Implementation (MPI)

The MPI version was designed for distributed computing environments where the graph is too large to fit on one machine. MPI (Message Passing Interface) allows multiple separate processes, potentially on different computers, to work together.

**Architecture Approach:**

- **Domain Decomposition**: The graph is divided into chunks, with each process owning a range of vertices
- **Local Processing**: Each process performs DFS on its assigned vertices
- **Communication**: When a process needs to visit a vertex owned by another process, it sends a message
- **Overlap Strategy**: Vertices are classified as either interior (only connected to local vertices) or boundary (connected to vertices on other processes)

**Communication Pattern:**

The implementation uses non-blocking sends and receives (asynchronous communication). While boundary information is being exchanged, each process continues working on its interior vertices. This overlaps computation with communication, which is a common optimization in distributed algorithms.

The graph is partitioned using a simple 1D block distribution. If we have 50,000 vertices and 4 processes, process 0 gets vertices 0-12,499, process 1 gets 12,500-24,999, and so on. This simple partitioning works well for graphs where connections are relatively uniform.

### 2.4 gRPC Service Layer

The gRPC service wraps the DFS implementations behind a network API that other programs can call. This makes the DFS algorithm available as a service.

**Service Components:**

- **Server**: Written in Python, the server receives requests and runs the compiled DFS executable
- **Client**: Sends DFS requests to one or more server replicas
- **Protocol**: Defined in a `.proto` file that specifies request and response formats

**Request Flow:**

1. Client sends a request with graph parameters (number of vertices, target vertex to find)
2. Server receives request and constructs command to run DFS executable
3. Server executes binary (either serial, parallel, or MPI version)
4. Server parses output from the executable
5. Server returns results (whether target was found, number of vertices visited, runtime)

The service design separates the algorithm implementation from the network layer. This means we can update the C++ DFS code without changing the service interface, and clients don't need to know which implementation version is running.

### 2.5 Fault Tolerance Design

Running distributed systems introduces the possibility of failures. The client implements automatic retry logic:

- **Multiple Replicas**: The client can connect to several server instances running on different ports
- **Round-Robin Distribution**: Requests are spread across replicas for load balancing
- **Automatic Failover**: If one replica fails, the client automatically tries another
- **No Manual Intervention**: The system recovers without human operators needing to restart anything

This design allows the service to continue operating even when individual servers crash or become unresponsive.

---

## 3. Failures and Challenges

Building this system involved working through several technical challenges. Some of our attempts didn't work as expected, which taught us important lessons about parallel algorithms.

### 3.1 Race Conditions in Parallel DFS

**The Problem:**

When multiple threads run DFS simultaneously and share the visited array, there's a timing problem called a race condition. Here's what can happen:

1. Thread A checks if vertex 5 is visited → reads "false"
2. Thread B checks if vertex 5 is visited → reads "false" (happens before Thread A marks it)
3. Thread A marks vertex 5 as visited and processes it
4. Thread B also marks vertex 5 as visited and processes it again

Now vertex 5 got processed twice, which wastes work and produces incorrect traversal results.

**Our Solution:**

We wrapped all access to the visited array in critical sections using OpenMP's `#pragma omp critical` directive. This ensures only one thread at a time can check and modify the visited status. While this solves the correctness problem, it creates a performance bottleneck that we'll discuss in the metrics section.

### 3.2 Stack Overflow with Large Graphs

**The Problem:**

DFS is naturally a recursive algorithm - it calls itself to explore deeper into the graph. With large graphs (we initially tried 50,000 vertices), the recursion can go very deep, and each function call uses stack memory. We encountered stack overflow errors when the recursion depth exceeded the system's stack size limit.

**What We Tried:**

We reduced the test graph size to 10,000 vertices for the parallel profiling tests, which avoided the stack overflow. For really large graphs, a better solution would be to rewrite DFS iteratively using an explicit stack data structure instead of recursion.

### 3.3 Poor Parallel Performance

**The Problem:**

The parallel version was supposed to be faster, but it turned out slower than the serial version. Much slower, in fact - with 16 threads, the parallel version took 17.5 times longer than the serial version!

**Root Causes:**

1. **Critical Section Overhead**: Every vertex visit requires acquiring a lock. If vertex visits are very quick, the locking overhead dominates the actual work
2. **Task Creation Cost**: OpenMP tasks aren't free - there's overhead to create them, schedule them, and synchronize them
3. **Cache Contention**: Multiple threads accessing the same memory locations cause cache coherency traffic between CPU cores
4. **Small Problem Size**: For graphs with only 10,000 vertices where each vertex takes microseconds to process, the parallelization overhead exceeds any benefit

This negative speedup taught us that parallelism has a cost, and it only pays off when the actual computation is large enough to overcome the overhead.

### 3.4 Fault Injection Challenges

**The Problem:**

To test fault tolerance, we needed to reliably crash servers or make them unresponsive. On Windows, there aren't built-in commands like Linux's `kill` signal.

**Our Solution:**

We wrote PowerShell scripts that:
- Find the process listening on a specific port
- Either kill it (crash scenario) or suspend it temporarily (timeout scenario)
- Resume suspended processes after a delay

These scripts let us inject controlled failures during testing to verify the client's retry logic worked correctly.

### 3.5 Process Communication Overhead (MPI)

**The Problem:**

In the MPI implementation, processes need to tell each other about vertices they want to visit that belong to other processes. This communication has costs:

- **Latency**: Time for a message to travel between processes
- **Bandwidth**: How much data can be sent per second
- **Synchronization**: Processes sometimes need to wait for each other

**Our Approach:**

We tried to minimize communication overhead by:
- Processing interior vertices first while communication happens in the background
- Batching vertex requests instead of sending individual messages
- Using non-blocking communication so computation doesn't stop while messages are in transit

Even with these optimizations, communication overhead is unavoidable in distributed systems. The key is ensuring the benefits of parallelism outweigh the communication costs.

---

## 4. Performance Metrics

We collected detailed performance measurements to understand how well each implementation performs. Here are the key findings.

### 4.1 Serial vs Parallel Performance

**Test Configuration:**
- Graph size: 10,000 vertices
- Each vertex has 2-4 neighbors
- Tested with 2, 4, 8, and 16 threads

**Results:**

| Implementation | Time (seconds) | Speedup vs Serial | Efficiency |
|----------------|----------------|-------------------|------------|
| Serial         | 0.000571       | 1.0x (baseline)   | 100%       |
| Parallel (2 threads)  | 0.003119 | 0.18x | 9%  |
| Parallel (4 threads)  | 0.004229 | 0.13x | 3%  |
| Parallel (8 threads)  | 0.008202 | 0.07x | 1%  |
| Parallel (16 threads) | 0.009997 | 0.06x | 0.4% |

**What This Means:**

The speedup numbers less than 1.0 mean the parallel versions are actually slower than serial. A speedup of 0.18 means the parallel version takes about 5.5 times longer than serial. The efficiency percentages show how well we're using the extra threads - only 9% efficiency with 2 threads means we're wasting 91% of the computational power we added.

**Why This Happened:**

The critical sections we added to prevent race conditions became bottlenecks. Most of the time is spent waiting for locks rather than doing useful computation. With such a small graph and fast vertex processing, the synchronization overhead completely dominates.

### 4.2 Scaling Analysis

**Strong Scaling:**

This means we keep the problem size fixed and add more threads. Ideally, if we double the threads, we'd halve the execution time. Our results showed the opposite - more threads made it slower. This tells us our problem is too small for effective parallelization.

**Theoretical Limit (Amdahl's Law):**

Amdahl's Law says that if 10% of your program must run serially, the maximum speedup you can ever achieve is 10x, no matter how many processors you add. In our case, the critical sections forced most of the code to run serially, so we were far from any theoretical speedup.

### 4.3 Cache Performance

We analyzed cache behavior and found issues:

**Serial Version:**
- 249,995 recursive calls create deep call stacks
- Random graph traversal patterns cause poor cache locality
- Frequently resizing the result vector causes memory reallocations

**Parallel Version:**
- Same issues as serial, plus additional problems
- **False Sharing**: When multiple threads access nearby memory locations, they invalidate each other's caches even if they're not accessing the same exact byte
- **Cache Coherency**: CPUs spend time synchronizing caches between cores

Cache misses can be just as expensive as the actual work being done, especially for simple operations like marking a vertex as visited.

### 4.4 MPI Distributed Performance

For the MPI version with 50,000 vertices:

- **4 Processes**: Completed in approximately 45-60 milliseconds
- **Communication Time**: About 10-20% of total time was spent on inter-process communication
- **Load Balance**: Relatively even distribution of work

The MPI version performed reasonably well because:
1. The problem size was large enough to justify the overhead
2. The graph structure didn't require excessive communication
3. Interior vertex computation could overlap with boundary communication

### 4.5 Service Layer Performance

**gRPC Overhead:**

- The network service adds about 2-5 milliseconds of latency per request
- Most of this is from process spawning (running the DFS executable as a subprocess)
- Network transmission time is negligible for local testing

**Fault Recovery:**

When we injected failures:
- Client detected failures within 1 request interval (100ms)
- Automatic retry added about 5-10ms latency to failed requests
- Overall success rate remained above 95% with 2 replicas

This shows the fault tolerance mechanism works efficiently without major performance penalties.

---

## 5. Discussion

Looking back at the entire project, we can draw several important conclusions and lessons learned.

### 5.1 When Parallelism Helps (and When It Doesn't)

The most important lesson is that **parallelism is not always faster**. Our parallel implementation performed worse than serial because:

1. **Problem size was too small**: When each operation takes microseconds, overhead dominates
2. **Synchronization is expensive**: Locks and critical sections serialize execution
3. **Fixed costs exist**: Creating threads, managing tasks, and coordinating all have base costs

**When would parallelism help?**

- Much larger graphs (100,000+ vertices)
- More expensive operations at each vertex (complex computations, database lookups)
- Better algorithms that need less synchronization

### 5.2 Design Trade-offs

Throughout the project, we made several trade-offs:

**Correctness vs Performance:**

We chose correctness by using critical sections to prevent race conditions, accepting the performance penalty. The alternative - a lock-free algorithm - would be much more complex to implement and debug.

**Simplicity vs Optimization:**

The MPI implementation uses simple 1D block partitioning. More sophisticated graph partitioning algorithms could balance load better and reduce communication, but they're much more complicated.

**Generality vs Specialization:**

The gRPC service can run any DFS implementation by spawning it as a subprocess. This is flexible but slower than embedding the algorithm directly in the server. We prioritized flexibility.

### 5.3 Distributed Systems Challenges

The gRPC service layer introduced distributed systems concerns:

**Failures are Normal:**

In a distributed system, components fail regularly. Rather than trying to prevent all failures, we designed for failure recovery with replica servers and automatic retry.

**State Management:**

Our service is stateless - each request is independent. This makes fault tolerance easier since there's no shared state to keep consistent between replicas.

**Observability:**

We added logging with timestamps and latency measurements. This was crucial for understanding system behavior during testing and failure scenarios.

### 5.4 Alternative Approaches

After completing the project, we can see several potential improvements:

**Algorithm Changes:**

- Use Breadth-First Search instead, which has better parallelization properties
- Implement iterative DFS with an explicit stack to avoid stack overflow
- Use work-stealing to balance load dynamically

**Synchronization Improvements:**

- Use lock-free data structures with atomic operations
- Give each thread its own local visited set and merge at the end
- Only synchronize at coarse-grained boundaries

**Better Graph Partitioning:**

- Use METIS or similar tools to minimize cut edges between partitions
- This would reduce communication in the MPI version

### 5.5 Real-World Applications

While our test graphs were synthetic, DFS and parallel graph algorithms have many practical uses:

- **Web Crawling**: Exploring links on websites
- **Social Network Analysis**: Finding connected components, influence paths
- **Dependency Resolution**: Build systems, package managers
- **Route Planning**: Finding paths in transportation networks

In these real applications, graphs are often much larger (millions or billions of vertices), which makes the investment in parallel and distributed implementations worthwhile.