#include <iostream>
#include <vector>
#include <omp.h>
using namespace std;

void dfsRec(vector<vector<int>> &adj, vector<bool> &visited, int s, vector<int> &res) {
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
                dfsRec(adj, visited, i, res);
            }
        }
    }
    #pragma omp taskwait
}

vector<int> dfs(vector<vector<int>> &adj)
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
                    dfsRec(adj, visited, i, res);
                }
                }
        }
    }
    return res;
}

int main()
{
    int numVertices = 50000;
    vector<vector<int>> adj(numVertices);

    cout << "Creating large graph with " << numVertices << " vertices..." << endl;

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

    cout << "Graph created successfully!" << endl;

    cout << "DFS Traversal of the graph (Parallel):" << endl;

    double start = omp_get_wtime();

    vector<int> result = dfs(adj);

    double end = omp_get_wtime();

    double time_seconds = end - start;
    double time_ms = time_seconds * 1000.0;

    cout << "Total vertices visited: " << result.size() << endl;
    cout << "First 10 vertices: ";
    for (int i = 0; i < 10 && i < result.size(); i++)
    {
        cout << result[i] << " ";
    }
    
    cout << "..." << endl << "Execution time: " << time_ms << " milliseconds (ms)" << endl;  
    cout << "Number of threads used: " << omp_get_max_threads() << endl;

    return 0;
}