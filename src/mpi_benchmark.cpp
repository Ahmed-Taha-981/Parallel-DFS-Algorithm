#include <iostream>
#include <vector>
#include <mpi.h>
#include <cmath>
#include <iomanip>
#include <fstream>

// Latency measurement: ping-pong test
double measureLatency(int rank, int numRanks) {
    const int iterations = 1000;
    const int warmup = 100;
    char data = 'X';
    
    if (numRanks < 2) {
        return -1.0;
    }
    
    int partner = (rank == 0) ? 1 : 0;
    
    // Warmup
    for (int i = 0; i < warmup; i++) {
        if (rank == 0) {
            MPI_Send(&data, 1, MPI_CHAR, partner, 0, MPI_COMM_WORLD);
            MPI_Recv(&data, 1, MPI_CHAR, partner, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        } else {
            MPI_Recv(&data, 1, MPI_CHAR, partner, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Send(&data, 1, MPI_CHAR, partner, 0, MPI_COMM_WORLD);
        }
    }
    
    // Actual measurement
    MPI_Barrier(MPI_COMM_WORLD);
    double startTime = MPI_Wtime();
    
    for (int i = 0; i < iterations; i++) {
        if (rank == 0) {
            MPI_Send(&data, 1, MPI_CHAR, partner, 0, MPI_COMM_WORLD);
            MPI_Recv(&data, 1, MPI_CHAR, partner, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        } else {
            MPI_Recv(&data, 1, MPI_CHAR, partner, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Send(&data, 1, MPI_CHAR, partner, 0, MPI_COMM_WORLD);
        }
    }
    
    double endTime = MPI_Wtime();
    double roundTripTime = (endTime - startTime) / iterations;
    double latency = roundTripTime / 2.0; // One-way latency
    
    return latency;
}

// Bandwidth measurement: send large messages
double measureBandwidth(int rank, int numRanks, int messageSize) {
    const int iterations = 10;
    const int warmup = 3;
    
    if (numRanks < 2) {
        return -1.0;
    }
    
    int partner = (rank == 0) ? 1 : 0;
    std::vector<char> sendBuffer(messageSize, 'X');
    std::vector<char> recvBuffer(messageSize);
    
    // Warmup
    for (int i = 0; i < warmup; i++) {
        if (rank == 0) {
            MPI_Send(sendBuffer.data(), messageSize, MPI_CHAR, partner, 0, MPI_COMM_WORLD);
            MPI_Recv(recvBuffer.data(), messageSize, MPI_CHAR, partner, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        } else {
            MPI_Recv(recvBuffer.data(), messageSize, MPI_CHAR, partner, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Send(sendBuffer.data(), messageSize, MPI_CHAR, partner, 0, MPI_COMM_WORLD);
        }
    }
    
    // Actual measurement
    MPI_Barrier(MPI_COMM_WORLD);
    double startTime = MPI_Wtime();
    
    for (int i = 0; i < iterations; i++) {
        if (rank == 0) {
            MPI_Send(sendBuffer.data(), messageSize, MPI_CHAR, partner, 0, MPI_COMM_WORLD);
            MPI_Recv(recvBuffer.data(), messageSize, MPI_CHAR, partner, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        } else {
            MPI_Recv(recvBuffer.data(), messageSize, MPI_CHAR, partner, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Send(sendBuffer.data(), messageSize, MPI_CHAR, partner, 0, MPI_COMM_WORLD);
        }
    }
    
    double endTime = MPI_Wtime();
    double totalTime = (endTime - startTime) / iterations;
    double bandwidth = (messageSize * 2.0) / totalTime; // bytes per second (round trip)
    
    return bandwidth;
}

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    
    int rank, numRanks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &numRanks);
    
    if (numRanks < 2) {
        if (rank == 0) {
            std::cerr << "Error: This benchmark requires at least 2 MPI processes" << std::endl;
        }
        MPI_Finalize();
        return 1;
    }
    
    // Latency measurement
    double latency = measureLatency(rank, numRanks);
    double maxLatency = 0.0;
    MPI_Reduce(&latency, &maxLatency, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    
    // Bandwidth measurements for different message sizes
    std::vector<int> messageSizes = {
        1024,           // 1 KB
        10240,          // 10 KB
        102400,         // 100 KB
        1048576,        // 1 MB
        10485760,       // 10 MB
        104857600       // 100 MB
    };
    
    std::vector<double> bandwidths(messageSizes.size());
    
    for (size_t i = 0; i < messageSizes.size(); i++) {
        double bw = measureBandwidth(rank, numRanks, messageSizes[i]);
        double maxBw = 0.0;
        MPI_Reduce(&bw, &maxBw, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        bandwidths[i] = maxBw;
    }
    
    // Output results
    if (rank == 0) {
        std::cout << "===========================================" << std::endl;
        std::cout << "MPI Communication Benchmark Results" << std::endl;
        std::cout << "===========================================" << std::endl;
        std::cout << "Number of processes: " << numRanks << std::endl;
        std::cout << std::endl;
        
        std::cout << "Latency (one-way): " << std::fixed << std::setprecision(6) 
                  << (maxLatency * 1e6) << " microseconds" << std::endl;
        std::cout << "Latency = (Round-trip time) / 2" << std::endl;
        std::cout << std::endl;
        
        std::cout << "Bandwidth Results:" << std::endl;
        std::cout << "Bandwidth = message_size_bytes / (one-way time)" << std::endl;
        std::cout << std::left << std::setw(15) << "Message Size" 
                  << std::setw(20) << "Bandwidth (MB/s)" << std::endl;
        std::cout << "-------------------------------------------" << std::endl;
        
        for (size_t i = 0; i < messageSizes.size(); i++) {
            double bwMBps = bandwidths[i] / (1024.0 * 1024.0);
            std::cout << std::left << std::setw(15) << messageSizes[i] << " bytes"
                      << std::setw(20) << std::fixed << std::setprecision(2) << bwMBps << std::endl;
        }
    }
    
    MPI_Finalize();
    return 0;
}


