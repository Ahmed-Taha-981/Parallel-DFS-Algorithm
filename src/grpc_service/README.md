# DFS gRPC Service — Fault-Tolerant Distributed System

## Overview

This directory provides a fault-tolerant, replicated gRPC wrapper around the existing DFS algorithm. The core algorithm remains **unmodified**; the wrapper launches the DFS binary as a subprocess on each request, logs per-request timestamp and latency, and provides automatic failover when a replica goes down.

## Key Files

| File               | Purpose                                                                            |
| ------------------ | ---------------------------------------------------------------------------------- |
| `dfs.proto`        | gRPC API definition (RunDFS RPC)                                                   |
| `server.py`        | Python gRPC server; executes DFS binary on demand, logs timestamp + latency        |
| `client.py`        | Client that sends continuous requests for ≥60s and auto-retries on replica failure |
| `run_replicas.ps1` | PowerShell helper to start 2 replicas on ports 50051 & 50052                       |
| `requirements.txt` | Python dependencies (grpcio, grpcio-tools)                                         |
| `TEST_RESULTS.md`  | Full test report showing normal operation + failover behavior                      |

## Quick Start (Windows, PowerShell)

### 1. Install dependencies

```powershell
cd src\grpc_service
python -m pip install -r requirements.txt
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. dfs.proto
```

### 2. Start Replica 1

```powershell
cd src\grpc_service
python server.py --port 50051 --logfile server_50051.log
# Expected output: "DFS gRPC server listening on [::]:50051"
```

### 3. Start Replica 2 (new terminal)

```powershell
cd src\grpc_service
python server.py --port 50052 --logfile server_50052.log
```

### 4. Run the client for 60 seconds (new terminal)

```powershell
cd src\grpc_service
python client.py --replicas localhost:50051,localhost:50052 --duration 60
# Expected output: "Finished: total attempts=<N>, successes=<N>"
```

## How It Works

1. **Server** (`server.py`):

   - Listens on a gRPC port for `RunDFS` requests
   - Launches the existing DFS binary (`src/mpi_dfs`) via subprocess
   - Parses output (found, visited_count, runtime_ms)
   - **Logs each request**: timestamp (ISO 8601 UTC) + latency_ms
   - Returns response to client

2. **Client** (`client.py`):

   - Sends one request every 0.1 seconds for 60 seconds
   - **Round-robin** load balances across replicas
   - On RPC error → **automatically retries** the next replica
   - **Logs each request**: endpoint, latency, success/failure

3. **Failover**:
   - If replica 1 crashes → client detects gRPC error
   - Client immediately retries replica 2 (or next in list)
   - No manual intervention; requests continue uninterrupted

## Deliverables Met ✅

| Requirement                    | How                                                                             |
| ------------------------------ | ------------------------------------------------------------------------------- |
| **Replication (2+ instances)** | Server runs on 2+ ports; start via manual commands                              |
| **Auto-Retry / Failover**      | Client catches gRPC errors, moves to next replica in round-robin                |
| **Logging**                    | `server_<port>.log` has `request_ts=<timestamp> latency_ms=<value>` per request |
| **Continuous 60+ seconds**     | Client parameter `--duration 60`                                                |
| **Core algorithm unmodified**  | Wrapper calls DFS binary as subprocess; source code untouched                   |

## Testing Failover

1. Start both replicas (steps 2–3 above)
2. Start the client (step 4)
3. While client is running, kill one replica in a separate terminal:
   ```powershell
   Stop-Process -Id <PID_OF_REPLICA> -Force
   ```
4. Observe that the client **continues** serving requests from the remaining replica(s)

See [TEST_RESULTS.md](TEST_RESULTS.md) for detailed test output showing 100% success with both replicas, and continued operation when one fails.

## Command-Line Options

### Server

```powershell
python server.py --port 50051 --exec ../mpi_dfs --mpi-procs 4 --logfile server.log --timeout 120
```

- `--port` — TCP port (default 50051)
- `--exec` — Path to DFS binary (default `../mpi_dfs`)
- `--mpi-procs` — Number of MPI processes (default 4)
- `--logfile` — Log file path (default `server.log`)
- `--timeout` — Subprocess timeout in seconds (default 120)

### Client

```powershell
python client.py --replicas localhost:50051,localhost:50052 --duration 60 --logfile client.log
```

- `--replicas` — Replica endpoints (default `localhost:50051,localhost:50052`)
- `--duration` — Run time in seconds (default 60)
- `--target` — Target vertex (default 42000)
- `--num-vertices` — Graph size (default 50000)
- `--per-call-timeout` — RPC timeout (default 30)
- `--logfile` — Log file path (default `client.log`)
