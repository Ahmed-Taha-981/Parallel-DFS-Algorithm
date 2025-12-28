import time
import sys
import random
import os

def main():
    # Simulate some work
    # Sleep between 10ms and 100ms
    sleep_time = random.uniform(0.01, 0.1)
    
    # Check environment variables for configuration
    target = os.environ.get('TARGET_VERTEX', '0')
    
    # 5% chance of finding target
    found = random.random() < 0.05
    
    time.sleep(sleep_time)
    
    # Output expected by server.py
    # "Execution time: 8.727 milliseconds (ms)"
    print(f"Execution time: {sleep_time*1000:.3f} milliseconds (ms)")
    
    # "Total vertices visited: 50000"
    visited = int(random.uniform(1000, 5000))
    print(f"Total vertices visited: {visited}")
    
    if found:
        print(f"found target: {target}")
    
    sys.exit(0)

if __name__ == '__main__':
    main()
