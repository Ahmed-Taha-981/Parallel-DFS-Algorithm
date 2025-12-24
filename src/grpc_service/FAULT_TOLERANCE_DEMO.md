# Task 2: Fault Tolerance Demonstration Guide

## Overview

This guide demonstrates how to inject failures into the distributed DFS gRPC service and verify that the system automatically recovers without manual intervention. The system must continue operating and serving requests even when one or more replicas fail.

## Requirements Met

| Requirement | Implementation |
|------------|----------------|
| **At least 2 different failure types** | 1. Service crash (kill process)<br>2. Network/service disruption (timeout via process suspension) |
| **No manual restart** | Client automatically retries all replicas |
| **No permanent data loss** | Requests are retried on healthy replicas |
| **System continues streaming outputs** | Client continues sending requests for full duration |

## Prerequisites

1. **Install dependencies:**
   ```powershell
   cd src\grpc_service
   python -m pip install -r requirements.txt
   python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. dfs.proto
   ```



## Step-by-Step Demonstration

### Step 1: Start Multiple Replicas

**Option A: Using the helper script (Recommended)**
```powershell
cd src\grpc_service
.\run_replicas.ps1 -NumReplicas 2 -StartPort 50051
```

**Option B: Manual start (2 separate terminals)**
```powershell
# Terminal 1
cd src\grpc_service
python server.py --port 50051 --logfile server_50051.log

# Terminal 2
cd src\grpc_service
python server.py --port 50052 --logfile server_50052.log
```

**Verify replicas are running:**
```powershell
Get-NetTCPConnection | Where-Object { $_.LocalPort -ge 50051 -and $_.LocalPort -le 50052 }
```

### Step 2: Start Continuous Client Load

**In a new terminal:**
```powershell
cd src\grpc_service
python client.py --replicas localhost:50051,localhost:50052 --duration 60 --logfile client.log
```

The client will:
- Send requests every 0.1 seconds for 60 seconds
- Round-robin across replicas
- Automatically retry all replicas if one fails
- Log all requests with timestamps and latencies

### Step 3: Inject Failures

While the client is running, inject failures in separate terminals. The client should automatically recover.

#### Failure Type 1: Service Crash

**In a new terminal:**
```powershell
cd src\grpc_service
.\inject_crash.ps1 -Port 50051 -Delay 5
```

**What happens:**
- After 5 seconds, the script kills the process on port 50051
- Client detects the failure (gRPC error)
- Client automatically retries on port 50052
- Requests continue without interruption
- **No manual intervention required**

**Expected client behavior:**
- Logs show warnings for port 50051: `endpoint=localhost:50051 error=...`
- Logs show successful requests on port 50052: `endpoint=localhost:50052 latency_ms=...`
- Total success rate remains high (only brief failures during retry)

#### Failure Type 2: Network/Service Disruption (Process Suspension)

**In a new terminal:**
```powershell
cd src\grpc_service
.\inject_timeout.ps1 -Port 50052 -Duration 10
```

**What happens:**
- Process on port 50052 is suspended for 10 seconds
- Client requests to port 50052 timeout
- Client automatically retries on port 50051
- After 10 seconds, process resumes automatically
- Client can use port 50052 again once it recovers

**Expected client behavior:**
- Console shows: `✗ Request #X → localhost:50052 ERROR (DEADLINE_EXCEEDED) - Retrying...`
- Console shows: `✓ Request #X → RETRY SUCCESS on localhost:50051 after 1 failure(s)`
- After recovery, client may use port 50052 again (round-robin)

### Step 4: Verify Recovery

**Watch the client console output in real-time:**

**Normal operation:**
```
[21:35:00] ✓ Request #1 → localhost:50051 (latency: 4.28ms)
[21:35:00] ✓ Request #2 → localhost:50052 (latency: 4.56ms)
```

**During failure (Service Crash):**
```
[21:35:15] ✗ Request #50 → localhost:50051 FAILED (StatusCode.UNAVAILABLE) - Retrying...
[21:35:15] ✓ Request #50 → RETRY SUCCESS on localhost:50052 after 1 failure(s) (latency: 4.67ms)
[21:35:15] ✓ Request #51 → localhost:50052 (latency: 4.33ms)
```

**During failure (Network Disruption):**
```
[21:35:20] ✗ Request #75 → localhost:50052 ERROR (DEADLINE_EXCEEDED) - Retrying...
[21:35:20] ✓ Request #75 → RETRY SUCCESS on localhost:50051 after 1 failure(s) (latency: 4.25ms)
```

**Success criteria:**
- ✅ Client continues sending requests for full 60 seconds
- ✅ Failures are immediately retried on other replicas
- ✅ Success rate remains high (>95% expected)
- ✅ No manual intervention required

### Extract Metrics for Analysis

**Throughput (requests per second):**
```powershell
# Count successful requests per second from client log
Get-Content client.log | Select-String "latency_ms=" | 
    Group-Object { ($_ -split ',')[0] } | 
    Select-Object Count, Name
```

**Latency percentiles:**
```powershell
# Extract latencies and calculate p95
$latencies = Get-Content client.log | Select-String "latency_ms=(\d+\.\d+)" | 
    ForEach-Object { [double]($_.Matches.Groups[1].Value) }
$sorted = $latencies | Sort-Object
$p95 = $sorted[[math]::Floor($sorted.Count * 0.95)]
Write-Host "P95 Latency: $p95 ms"
```

**Recovery time:**
- Measure time between first failure log and first success log after failure
- Should be < 1 second (immediate retry to next replica)

