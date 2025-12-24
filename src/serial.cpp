#include <iostream>
#include <vector>
#include <ctime>
using namespace std;

bool targetFound = false;
int targetVertex = 1;  // Changed from 42000 to 1 (guaranteed to exist)

void dfsRec(vector<vector<int>> &adj, vector<bool> &visited, int s, vector<int> &res, int stride) {
    visited[s] = true;
    res.push_back(s);

    // Check if this is the target vertex
    if (s == targetVertex) {
        targetFound = true;
        cout << "found target: vertex " << targetVertex << endl;
    }

    double work = 0;
    for (int i = 0; i < 1000; i++)
    {
        work += (s * i) % 100;
    }

    for (int idx = 0; idx < adj[s].size(); idx += stride)
    {
        int i = adj[s][idx];
        if (visited[i] == false)
            dfsRec(adj, visited, i, res, stride);
    }
    
    for (int idx = 0; idx < adj[s].size(); idx++)
    {
        if (idx % stride != 0)
        {
            int i = adj[s][idx];
            if (visited[i] == false)
                dfsRec(adj, visited, i, res, stride);
        }
    }
}

vector<int> dfs(vector<vector<int>> &adj, int stride) {
    vector<bool> visited(adj.size(), false);
    vector<int> res;

    for (int i = 0; i < adj.size(); i++)
    {
        if (visited[i] == false)
        {
            dfsRec(adj, visited, i, res, stride);
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

    int strides[] = {1, 2, 4, 8, 16};
    int num_strides = sizeof(strides) / sizeof(strides[0]);

    for (int s = 0; s < num_strides; s++)
    {
        int stride = strides[s];
        cout << "DFS Traversal of the graph (Serial):" << endl;
        cout << "Stride size: " << stride << endl;

        clock_t start = clock();

        vector<int> result = dfs(adj, stride);

        clock_t end = clock();

        double time_seconds = double(end - start) / CLOCKS_PER_SEC;
        double time_ms = time_seconds * 1000.0;

        cout << "Total vertices visited: " << result.size() << endl;
        cout << "First 10 vertices: ";
        for (int i = 0; i < 10 && i < result.size(); i++)
        {
            cout << result[i] << " ";
        }
        cout << "..." << endl;
        cout << "Execution time: " << time_ms << " milliseconds (ms)" << endl;
        cout << endl;
    }

    // Print final summary
    if (targetFound) {
        cout << "found target: vertex " << targetVertex << endl;
    }

    return 0;
}
