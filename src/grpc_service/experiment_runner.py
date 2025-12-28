"""
DFS Experiment Runner
Orchestrates the performance analysis experiment:
1. Starts 3 Replicas
2. Starts Client (Normal Load)
3. Injects Crash
4. Collects Data (via Client Logs)
"""

import os
import time
import subprocess
import signal
import sys
import shutil

# Configuration
REPLICAS_COUNT = 3
START_PORT = 50051
CLIENT_DURATION = 60
CRASH_DELAY = 20  # Crash 20s into the experiment
CRASH_TARGET_PORT = 50052 # Middle replica
LOG_DIR = "streaming_logs"

def cleanup():
    print("\n[CLEANUP] Killing all python/python3 processes (brute force cleanup)...")
    # This is rough but effective for this lab environment to ensure no zombie servers
    if os.name == 'nt':
        os.system("taskkill /F /IM python.exe /T >nul 2>&1")
    else:
        os.system("pkill -9 python")

def run_experiment():
    print("="*60)
    print("🧪 STARTING FAULT TOLERANCE EXPERIMENT")
    print("="*60)

    # 0. Cleanup from previous runs
    print("[0/5] Cleaning up previous runs...")
    cleanup()
    if os.path.exists(LOG_DIR):
        shutil.rmtree(LOG_DIR)
    os.makedirs(LOG_DIR, exist_ok=True)
    time.sleep(2)

    # 1. Start Replicas
    print(f"\n[1/5] Starting {REPLICAS_COUNT} Replicas...")
    processes = []
    
    # We use the powershell script but we can also just spawn python processes directly
    # Spawning directly gives us better control here
    for i in range(REPLICAS_COUNT):
        port = START_PORT + i
        log_file = os.path.join(LOG_DIR, f"server_{port}.log")
        
        # Use Mock DFS for consistent testing
        exec_path = os.path.abspath("mock_dfs.bat")
        
        cmd = [sys.executable, "server.py", "--port", str(port), "--logfile", log_file, "--exec", exec_path]
        
        p = subprocess.Popen(cmd, cwd=os.getcwd())
        processes.append(p)
        print(f" Started Replica {i+1} on port {port} (PID: {p.pid})")
    
    time.sleep(5) # Warmup

    # 2. Start Streaming Client
    print(f"\n[2/5] Starting Streaming Client for {CLIENT_DURATION}s...")
    
    replicas = ",".join([f"localhost:{START_PORT+i}" for i in range(REPLICAS_COUNT)])
    
    log_file = os.path.join(LOG_DIR, "streaming_events.csv")
    
    client_cmd = [
        sys.executable, "-u", "simple_streaming_client.py", # -u for unbuffered output
        "--replicas", replicas,
        "--requests-per-second", "10",
        "--duration", str(CLIENT_DURATION),
        "--log-file", log_file
    ]
    
    # Capture output to debug
    client_log = open("client_stdout.log", "w")
    client_process = subprocess.Popen(client_cmd, cwd=os.getcwd(), stdout=client_log, stderr=subprocess.STDOUT)
    print(f"  Client started (PID: {client_process.pid}) - logging to client_stdout.log")

    # 3. Wait for pre-crash period
    print(f"\n[3/5] Running normal operation for {CRASH_DELAY}s...")
    time.sleep(CRASH_DELAY)

    # 4. Inject Failure
    print(f"\n[4/5] INJECTING FAILURE: Crashing Replica on Port {CRASH_TARGET_PORT}...")
    
    # Find the process for the target port
    # In our list 'processes', index 1 corresponds to 50052
    target_proc = processes[1] 
    target_proc.terminate() # or kill()
    print(f"  Killed process {target_proc.pid} on port {CRASH_TARGET_PORT}")
    
    # Optional: You can use inject_crash.ps1 via subprocess if you prefer that
    # subprocess.run(["powershell", "-File", "inject_crash.ps1", "-Port", str(CRASH_TARGET_PORT)])

    # 5. Wait for remainder
    remaining_time = CLIENT_DURATION - CRASH_DELAY
    print(f"\n[5/5] Observing recovery for remaining {remaining_time}s...")
    
    client_process.wait()
    print("\nExperiment Complete.")
    
    cleanup()

if __name__ == "__main__":
    run_experiment()
