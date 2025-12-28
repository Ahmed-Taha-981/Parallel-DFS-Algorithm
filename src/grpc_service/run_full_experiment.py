"""
Run Full Experiment (End-to-End)
Fully automated script to:
1. Setup environment (cleanup)
2. Start Replicas
3. Start Client (Simple Streaming)
4. Inject Crash
5. Wait for Completion
6. Generate Analysis Graphs
"""

import os
import time
import subprocess
import sys
import shutil

# Configuration
REPLICAS_COUNT = 3
START_PORT = 50051
CLIENT_DURATION = 60
CRASH_DELAY = 15  # Crash @ 15s
CRASH_TARGET_PORT = 50052
LOG_DIR = "streaming_logs"

def cleanup():
    print("[Cleanup] Improving hygiene...")
    os.system("taskkill /F /IM python.exe /T >nul 2>&1")
    time.sleep(2) # Wait for processes to release locks
    
    if os.path.exists(LOG_DIR):
        try:
            shutil.rmtree(LOG_DIR)
        except Exception as e:
            print(f"Warning: Could not remove {LOG_DIR}: {e}")
            # Try to just clear contents if folder is locked
            for filename in os.listdir(LOG_DIR):
                file_path = os.path.join(LOG_DIR, filename)
                try:
                    if os.path.isfile(file_path):
                        os.unlink(file_path)
                except Exception:
                    pass
                    
    os.makedirs(LOG_DIR, exist_ok=True)
    time.sleep(1)

def main():
    print("="*60)
    print("STARTING FULL PERFORMANCE EXPERIMENT")
    print("="*60)
    
    # 1. Cleanup
    cleanup()
    
    # 2. Start Replicas
    print(f"\n[1/6] Starting {REPLICAS_COUNT} Replicas...")
    processes = []
    
    # Mock DFS wrapper path
    exec_path = os.path.abspath("mock_dfs.bat")
    
    for i in range(REPLICAS_COUNT):
        port = START_PORT + i
        log_file = os.path.join(LOG_DIR, f"server_{port}.log")
        
        # Start server.py with unbuffered output (-u)
        cmd = [sys.executable, "-u", "server.py", "--port", str(port), "--logfile", log_file, "--exec", exec_path]
        
        # We assume server.py is in current dir
        p = subprocess.Popen(cmd)
        processes.append(p)
        print(f"  Replica {i+1} (Port {port}) started (PID: {p.pid})")
        
    time.sleep(3) # Warmup

    # 3. Start Client
    print(f"\n[2/6] Starting Streaming Client ({CLIENT_DURATION}s)...")
    replicas = ",".join([f"localhost:{START_PORT+i}" for i in range(REPLICAS_COUNT)])
    log_file = os.path.join(LOG_DIR, "streaming_events.csv")
    
    # Start simple_streaming_client.py
    client_cmd = [
        sys.executable, "-u", "simple_streaming_client.py",
        "--replicas", replicas,
        "--requests-per-second", "10",
        "--duration", str(CLIENT_DURATION),
        "--log-file", log_file
    ]
    
    # Capture client output to file for debugging if needed
    client_out = open("client_run.log", "w")
    client_proc = subprocess.Popen(client_cmd, stdout=client_out, stderr=subprocess.STDOUT)
    print(f"  ✓ Client started (PID: {client_proc.pid})")

    # 4. Normal Operation
    print(f"\n[3/6] running normal load for {CRASH_DELAY}s...")
    time.sleep(CRASH_DELAY)

    # 5. Inject Crash
    print(f"\n[4/6] INJECTING CRASH on Port {CRASH_TARGET_PORT}...")
    # Target is index 1 (50051 + 1 = 50052)
    target_proc = processes[1]
    target_proc.terminate()
    print(f"  ✓ Terminated Replica on Port {CRASH_TARGET_PORT}")
    
    # 6. Wait for Recovery observation
    remaining = CLIENT_DURATION - CRASH_DELAY
    print(f"\n[5/6] Observing recovery ({remaining}s remaining)...")
    client_proc.wait()
    client_out.close()
    
    print("\nExperiment Run Complete.")
    
    # Stop remaining replicas
    for p in processes:
        if p.poll() is None:
            p.terminate()

    # 7. Generate Graphs
    print(f"\n[6/6] Generating Analysis Graphs...")
    if os.path.exists(log_file):
        subprocess.run([sys.executable, "generate_analysis_graphs.py", "--log-dir", LOG_DIR])
    else:
        print("Error: Log file not found. Experiment failed.")
        
    print("\nDONE! Check 'performance_analysis.png'")

if __name__ == "__main__":
    main()
