#include <iostream>
#include <vector>
#include <mpi.h>
#include <algorithm>
#include <set>
using namespace std;

struct DomainInfo {
    int rank;
    int numRanks;
    int startVertex;
    int endVertex;
    int localSize;
};

DomainInfo setupDomain(int totalVertices, int rank, int numRanks) {
    DomainInfo domain;
    domain.rank = rank;
    domain.numRanks = numRanks;
    
    int baseSize = totalVertices / numRanks;
    int remainder = totalVertices % numRanks;
    
    if (rank < remainder) {
        domain.localSize = baseSize + 1;
        domain.startVertex = rank * domain.localSize;
    } else {
        domain.localSize = baseSize;
        domain.startVertex = remainder * (baseSize + 1) + (rank - remainder) * baseSize;
    }
    
    domain.endVertex = domain.startVertex + domain.localSize;
    
    return domain;
}

bool isLocalVertex(int vertex, const DomainInfo& domain) {
    return vertex >= domain.startVertex && vertex < domain.endVertex;
}

int findOwnerRank(int vertex, int totalVertices, int numRanks) {
    int baseSize = totalVertices / numRanks;
    int remainder = totalVertices % numRanks;
    
    int threshold = remainder * (baseSize + 1);
    if (vertex < threshold) {
        return vertex / (baseSize + 1);
    } else {
        return remainder + (vertex - threshold) / baseSize;
    }
}

bool localDFS(const vector<vector<int>>& adj, vector<bool>& visited, 
              int vertex, vector<int>& localResult, 
              set<int>& boundaryVertices, const DomainInfo& domain,
              int target, bool& found) {
    
    if (visited[vertex]) return false;
    
    visited[vertex] = true;
    localResult.push_back(vertex);
    
    if (vertex == target) {
        found = true;
        return true;
    }
    
    double work = 0;
    for (int i = 0; i < 1000; i++) {
        work += (vertex * i) % 100;
    }
    
    for (int neighbor : adj[vertex]) {
        if (isLocalVertex(neighbor, domain)) {
            if (!visited[neighbor]) {
                if (localDFS(adj, visited, neighbor, localResult, boundaryVertices, domain, target, found)) {
                    return true;
                }
            }
        } else {
            boundaryVertices.insert(neighbor);
        }
    }
    
    return false;
}

bool isBoundaryVertex(int vertex, const vector<vector<int>>& adj, const DomainInfo& domain) {
    if (!isLocalVertex(vertex, domain)) return false;
    
    for (int neighbor : adj[vertex]) {
        if (!isLocalVertex(neighbor, domain)) {
            return true;
        }
    }
    return false;
}

pair<vector<int>, bool> dfs_mpi_with_overlap(const vector<vector<int>>& adj, 
                                              const DomainInfo& domain, 
                                              int target) {
    int totalVertices = adj.size();
    vector<bool> visited(totalVertices, false);
    vector<int> localResult;
    set<int> boundaryVertices;
    bool targetFound = false;
    
    vector<int> interiorVertices;
    vector<int> localBoundaryVertices;
    
    for (int v = domain.startVertex; v < domain.endVertex; v++) {
        if (isBoundaryVertex(v, adj, domain)) {
            localBoundaryVertices.push_back(v);
        } else {
            interiorVertices.push_back(v);
        }
    }
    
    vector<MPI_Request> recvRequests;
    vector<vector<int>> recvBuffers(domain.numRanks);
    vector<int> recvSizes(domain.numRanks, 0);
    
    for (int srcRank = 0; srcRank < domain.numRanks; srcRank++) {
        if (srcRank != domain.rank) {
            MPI_Request req;
            MPI_Irecv(&recvSizes[srcRank], 1, MPI_INT, srcRank, 0, 
                     MPI_COMM_WORLD, &req);
            recvRequests.push_back(req);
        }
    }
    
    set<int> externalVerticesSet;
    for (int v : localBoundaryVertices) {
        for (int neighbor : adj[v]) {
            if (!isLocalVertex(neighbor, domain)) {
                externalVerticesSet.insert(neighbor);
            }
        }
    }
    
    vector<MPI_Request> sendRequests;
    vector<vector<int>> sendBuffers(domain.numRanks);
    
    for (int extV : externalVerticesSet) {
        int ownerRank = findOwnerRank(extV, totalVertices, domain.numRanks);
        if (ownerRank != domain.rank) {
            sendBuffers[ownerRank].push_back(extV);
        }
    }
    
    for (int destRank = 0; destRank < domain.numRanks; destRank++) {
        if (destRank != domain.rank) {
            int size = sendBuffers[destRank].size();
            
            MPI_Request sizeReq;
            MPI_Isend(&size, 1, MPI_INT, destRank, 0, MPI_COMM_WORLD, &sizeReq);
            sendRequests.push_back(sizeReq);
            
            if (size > 0) {
                MPI_Request dataReq;
                MPI_Isend(sendBuffers[destRank].data(), size, MPI_INT, 
                         destRank, 1, MPI_COMM_WORLD, &dataReq);
                sendRequests.push_back(dataReq);
            }
        }
    }
    
    for (int v : interiorVertices) {
        if (!visited[v] && !targetFound) {
            set<int> dummy;
            if (localDFS(adj, visited, v, localResult, dummy, domain, target, targetFound)) {
                break;
            }
        }
    }
    
    if (!recvRequests.empty()) {
        MPI_Waitall(recvRequests.size(), recvRequests.data(), MPI_STATUSES_IGNORE);
    }
    
    recvRequests.clear();
    for (int srcRank = 0; srcRank < domain.numRanks; srcRank++) {
        if (srcRank != domain.rank && recvSizes[srcRank] > 0) {
            recvBuffers[srcRank].resize(recvSizes[srcRank]);
            MPI_Request req;
            MPI_Irecv(recvBuffers[srcRank].data(), recvSizes[srcRank], MPI_INT, 
                     srcRank, 1, MPI_COMM_WORLD, &req);
            recvRequests.push_back(req);
        }
    }
    
    if (!recvRequests.empty()) {
        MPI_Waitall(recvRequests.size(), recvRequests.data(), MPI_STATUSES_IGNORE);
    }
    
    for (int v : localBoundaryVertices) {
        if (!visited[v] && !targetFound) {
            localDFS(adj, visited, v, localResult, boundaryVertices, domain, target, targetFound);
        }
    }
    
    for (int srcRank = 0; srcRank < domain.numRanks; srcRank++) {
        if (srcRank != domain.rank && !targetFound) {
            for (int v : recvBuffers[srcRank]) {
                if (isLocalVertex(v, domain) && !visited[v]) {
                    set<int> dummy;
                    if (localDFS(adj, visited, v, localResult, dummy, domain, target, targetFound)) {
                        break;
                    }
                }
            }
        }
    }
    
    if (!sendRequests.empty()) {
        MPI_Waitall(sendRequests.size(), sendRequests.data(), MPI_STATUSES_IGNORE);
    }
    
    return {localResult, targetFound};
}

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    
    int rank, numRanks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &numRanks);
    
    int numVertices = 50000;
    int targetVertex = 42000;
    
    MPI_Bcast(&numVertices, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&targetVertex, 1, MPI_INT, 0, MPI_COMM_WORLD);
    
    vector<vector<int>> adj(numVertices);
    
    if (rank == 0) {
        cout << "running distributed DFS..." << endl;
        cout << "graph size: " << numVertices << " vertices" << endl;
        cout << "searching for vertex: " << targetVertex << endl;
        cout << "using " << numRanks << " processes" << endl << endl;
    }
    
    for (int i = 0; i < numVertices; i++) {
        for (int j = 1; j <= 3; j++) {
            int neighbor = (i + j * 7) % numVertices;
            adj[i].push_back(neighbor);
        }
    }
    
    DomainInfo domain = setupDomain(numVertices, rank, numRanks);
    
    if (rank == 0) {
        cout << "domain decomposition (1D block):" << endl;
    }
    for (int r = 0; r < numRanks; r++) {
        if (rank == r) {
            cout << "rank " << rank << " owns vertices " << domain.startVertex 
                 << " to " << (domain.endVertex-1) << endl;
        }
        MPI_Barrier(MPI_COMM_WORLD);
    }
    
    MPI_Barrier(MPI_COMM_WORLD);
    double startTime = MPI_Wtime();
    
    auto [localResult, localFound] = dfs_mpi_with_overlap(adj, domain, targetVertex);
    
    MPI_Barrier(MPI_COMM_WORLD);
    double endTime = MPI_Wtime();
    
    int localCount = localResult.size();
    int totalCount = 0;
    MPI_Reduce(&localCount, &totalCount, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    
    int foundFlag = localFound ? 1 : 0;
    int globalFound = 0;
    MPI_Reduce(&foundFlag, &globalFound, 1, MPI_INT, MPI_MAX, 0, MPI_COMM_WORLD);
    
    double localTime = endTime - startTime;
    double maxTime = 0;
    MPI_Reduce(&localTime, &maxTime, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    
    if (rank == 0) {
        cout << endl;
        cout << "time taken: " << (maxTime * 1000.0) << " ms" << endl;
        cout << "vertices visited: " << totalCount << endl;
        if (globalFound) {
            cout << "found target: vertex " << targetVertex << endl;
        } else {
            cout << "target not found: vertex " << targetVertex << endl;
        }
    }
    
    MPI_Finalize();
    return 0;
}