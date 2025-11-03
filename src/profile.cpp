#include <iostream>
#include <vector>
#include <omp.h>
#include <chrono>
#include <iomanip>
#include <fstream>
#include <cmath>
using namespace std;

// Serial DFS implementation
void dfsRecSerial(vector<vector<int>> &adj, vector<bool> &visited, int s, vector<int> &res) {
    visited[s] = true;
    res.push_back(s);

    double work = 0;
    for (int i = 0; i < 1000; i++)
    {
        work += (s * i) % 100;
    }

    for (int i : adj[s])
        if (visited[i] == false)
            dfsRecSerial(adj, visited, i, res);
}

vector<int> dfsSerial(vector<vector<int>> &adj) {
    vector<bool> visited(adj.size(), false);
    vector<int> res;

    for (int i = 0; i < adj.size(); i++)
    {
        if (visited[i] == false)
        {
            dfsRecSerial(adj, visited, i, res);
        }
    }
    return res;
}

// Parallel DFS implementation
void dfsRecParallel(vector<vector<int>> &adj, vector<bool> &visited, int s, vector<int> &res) {
    #pragma omp critical
    {
        if (!visited[s])
        {
            visited[s] = true;
            res.push_back(s);
        }
    }

    double work = 0;
    for (int i = 0; i < 1000; i++)
    {
        work += (s * i) % 100;
    }

    for (int i : adj[s])
    {
        bool needsVisit = false;

        #pragma omp critical
        {
            needsVisit = !visited[i];
        }

        if (needsVisit) {
            #pragma omp task shared(adj, visited, res)
            {
                dfsRecParallel(adj, visited, i, res);
            }
        }
    }
    #pragma omp taskwait
}

vector<int> dfsParallel(vector<vector<int>> &adj)
{
    vector<bool> visited(adj.size(), false);
    vector<int> res;

    #pragma omp parallel
    {
        #pragma omp single
        {
            for (int i = 0; i < adj.size(); i++)
            {
                if (visited[i] == false)
                {
                    dfsRecParallel(adj, visited, i, res);
                }
            }
        }
    }
    return res;
}

// Create test graph
vector<vector<int>> createGraph(int numVertices) {
    vector<vector<int>> adj(numVertices);

    for (int i = 0; i < numVertices; i++)
    {
        int connections = 2 + (i % 3);
        for (int j = 1; j <= connections; j++)
        {
            int neighbor = (i * 7 + j * 13) % numVertices;
            if (neighbor != i)
            {
                adj[i].push_back(neighbor);
            }
        }
    }
    return adj;
}

// Measure execution time for serial version
double measureSerialTime(vector<vector<int>> &adj, int iterations = 5) {
    vector<double> times;
    
    for (int iter = 0; iter < iterations; iter++) {
        auto start = chrono::high_resolution_clock::now();
        vector<int> result = dfsSerial(adj);
        auto end = chrono::high_resolution_clock::now();
        
        chrono::duration<double> duration = end - start;
        times.push_back(duration.count());
    }
    
    // Calculate average (excluding first warmup run if needed)
    double sum = 0;
    for (double t : times) {
        sum += t;
    }
    return sum / times.size();
}

// Measure execution time for parallel version with specified threads
double measureParallelTime(vector<vector<int>> &adj, int numThreads, int iterations = 5) {
    omp_set_num_threads(numThreads);
    vector<double> times;
    
    for (int iter = 0; iter < iterations; iter++) {
        auto start = chrono::high_resolution_clock::now();
        vector<int> result = dfsParallel(adj);
        auto end = chrono::high_resolution_clock::now();
        
        chrono::duration<double> duration = end - start;
        times.push_back(duration.count());
    }
    
    // Calculate average
    double sum = 0;
    for (double t : times) {
        sum += t;
    }
    return sum / times.size();
}

int main()
{
    const int numVertices = 50000;
    const int iterations = 5; // Number of runs for averaging
    
    cout << "===========================================" << endl;
    cout << "Performance Profiling: DFS Traversal" << endl;
    cout << "===========================================" << endl;
    cout << "Graph size: " << numVertices << " vertices" << endl;
    cout << "Averaging over " << iterations << " iterations" << endl;
    cout << "===========================================" << endl << endl;
    
    // Create graph once
    cout << "Creating graph..." << endl;
    vector<vector<int>> adj = createGraph(numVertices);
    cout << "Graph created successfully!" << endl << endl;
    
    // Measure serial time (T_S)
    cout << "Measuring Serial Execution Time (T_S)..." << endl;
    double T_S = measureSerialTime(adj, iterations);
    cout << "T_S = " << fixed << setprecision(6) << T_S << " seconds" << endl;
    cout << "T_S = " << fixed << setprecision(3) << (T_S * 1000.0) << " milliseconds" << endl << endl;
    
    // Measure parallel times for different thread counts
    vector<int> threadCounts = {1, 2, 4, 8};
    vector<double> T_P_values;
    vector<double> speedups;
    vector<double> efficiencies;
    
    cout << "Measuring Parallel Execution Times (T_P)..." << endl;
    cout << "-------------------------------------------" << endl;
    
    for (int threads : threadCounts) {
        cout << "\nTesting with " << threads << " thread(s)..." << endl;
        double T_P = measureParallelTime(adj, threads, iterations);
        T_P_values.push_back(T_P);
        
        double speedup = T_S / T_P;
        double efficiency = speedup / threads;
        
        speedups.push_back(speedup);
        efficiencies.push_back(efficiency);
        
        cout << "T_P(" << threads << ") = " << fixed << setprecision(6) << T_P << " seconds" << endl;
        cout << "T_P(" << threads << ") = " << fixed << setprecision(3) << (T_P * 1000.0) << " milliseconds" << endl;
        cout << "Speedup (S) = " << fixed << setprecision(4) << speedup << endl;
        cout << "Efficiency (E) = " << fixed << setprecision(4) << efficiency << " (" << (efficiency * 100) << "%)" << endl;
    }
    
    // Output summary table
    cout << "\n\n===========================================" << endl;
    cout << "PERFORMANCE SUMMARY" << endl;
    cout << "===========================================" << endl;
    cout << left << setw(10) << "Threads" 
         << setw(15) << "T_P (seconds)" 
         << setw(15) << "Speedup (S)" 
         << setw(15) << "Efficiency (E)" << endl;
    cout << "-------------------------------------------" << endl;
    
    for (size_t i = 0; i < threadCounts.size(); i++) {
        cout << left << setw(10) << threadCounts[i]
             << setw(15) << fixed << setprecision(6) << T_P_values[i]
             << setw(15) << setprecision(4) << speedups[i]
             << setw(15) << setprecision(4) << efficiencies[i] << endl;
    }
    
    cout << "\nSerial Time (T_S): " << fixed << setprecision(6) << T_S << " seconds" << endl;
    
    // Save results to file
    ofstream resultsFile("performance_results.txt");
    if (resultsFile.is_open()) {
        resultsFile << "Performance Profiling Results\n";
        resultsFile << "============================\n\n";
        resultsFile << "Graph size: " << numVertices << " vertices\n";
        resultsFile << "Iterations per measurement: " << iterations << "\n\n";
        resultsFile << "Serial Time (T_S): " << fixed << setprecision(6) << T_S << " seconds\n\n";
        resultsFile << left << setw(10) << "Threads" 
                    << setw(15) << "T_P (seconds)" 
                    << setw(15) << "Speedup (S)" 
                    << setw(15) << "Efficiency (E)" << "\n";
        resultsFile << "-------------------------------------------\n";
        
        for (size_t i = 0; i < threadCounts.size(); i++) {
            resultsFile << left << setw(10) << threadCounts[i]
                       << setw(15) << fixed << setprecision(6) << T_P_values[i]
                       << setw(15) << setprecision(4) << speedups[i]
                       << setw(15) << setprecision(4) << efficiencies[i] << "\n";
        }
        resultsFile.close();
        cout << "\nResults saved to performance_results.txt" << endl;
    }
    
    return 0;
}

