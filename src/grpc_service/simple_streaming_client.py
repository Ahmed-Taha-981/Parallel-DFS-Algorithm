"""
Simple Streaming DFS Client (No Spark Required)

WHAT IS BEING STREAMED:
- A continuous stream of DFS computation requests generated at a configurable rate
- Each request contains: request_id, timestamp, target_vertex, num_vertices
- Requests are sent to gRPC servers with round-robin load balancing and fault tolerance

HOW IT WORKS:
1. Generator produces requests at a configurable rate (e.g., 10 requests/second)
2. Each request is immediately processed and sent to gRPC servers
3. Load balancing: Requests alternate between replicas in round-robin fashion
4. Fault tolerance: If a replica fails, automatically retries on other replicas
5. All events (request sent, response received, failures) are logged with timestamps
6. Runs continuously for a specified duration (min 60 seconds)

STREAMING ARCHITECTURE:
- Event Generator: Time-based request generation at configurable rate
- Stream Processor: Processes each request immediately with gRPC call
- Event Logger: Logs all events with precise timestamps to CSV
"""

import argparse
import time
import datetime
import csv
import os
import grpc
import dfs_pb2
import dfs_pb2_grpc
from threading import Thread, Lock
from queue import Queue


class StreamingDFSClient:
    """Handles streaming DFS requests with fault tolerance and detailed logging"""
    
    def __init__(self, replicas, target, num_vertices, per_call_timeout=3, log_file='streaming_events.csv'):
        self.replicas = replicas
        self.target = target
        self.num_vertices = num_vertices
        self.per_call_timeout = per_call_timeout
        self.log_file = log_file
        
        # Statistics
        self.request_counter = 0
        self.success_counter = 0
        self.failure_counter = 0
        self.stats_lock = Lock()
        
        # Event queue for logging
        self.event_queue = Queue()
        
        # Initialize CSV log file
        self._init_log_file()
        
    def _init_log_file(self):
        """Initialize CSV log file with headers"""
        with open(self.log_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                'request_id',
                'event_timestamp',
                'processing_timestamp', 
                'endpoint',
                'attempt_number',
                'status',
                'latency_ms',
                'total_latency_ms',
                'found',
                'visited_count',
                'runtime_ms',
                'error_message'
            ])
    
    def _log_event(self, event_data):
        """Log event to CSV file"""
        with open(self.log_file, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                event_data['request_id'],
                event_data['event_timestamp'],
                event_data['processing_timestamp'],
                event_data['endpoint'],
                event_data['attempt_number'],
                event_data['status'],
                event_data['latency_ms'],
                event_data['total_latency_ms'],
                event_data['found'],
                event_data['visited_count'],
                event_data['runtime_ms'],
                event_data.get('error_message', '')
            ])
    
    def call_grpc_with_retry(self, request_id, event_timestamp):
        """
        Send a single gRPC request with automatic failover across replicas
        Returns: dict with detailed metrics
        """
        with self.stats_lock:
            self.request_counter += 1
        
        start_time = time.time()
        
        # Try all replicas in round-robin order
        start_idx = request_id % len(self.replicas)
        
        for attempt in range(len(self.replicas)):
            replica_idx = (start_idx + attempt) % len(self.replicas)
            endpoint = self.replicas[replica_idx]
            attempt_start = time.time()
            
            try:
                # Create gRPC channel and stub
                channel = grpc.insecure_channel(endpoint)
                stub = dfs_pb2_grpc.DFSServiceStub(channel)
                
                # Create request
                req = dfs_pb2.RunRequest(
                    target=self.target,
                    num_vertices=self.num_vertices,
                    use_mpi=True
                )
                
                # Make RPC call
                resp = stub.RunDFS(req, timeout=self.per_call_timeout)
                
                # Success!
                end_time = time.time()
                latency_ms = (end_time - attempt_start) * 1000.0
                total_latency_ms = (end_time - start_time) * 1000.0
                
                with self.stats_lock:
                    self.success_counter += 1
                
                result = {
                    'request_id': request_id,
                    'event_timestamp': event_timestamp,
                    'processing_timestamp': datetime.datetime.now().isoformat(),
                    'endpoint': endpoint,
                    'attempt_number': attempt + 1,
                    'status': 'SUCCESS',
                    'latency_ms': f'{latency_ms:.2f}',
                    'total_latency_ms': f'{total_latency_ms:.2f}',
                    'found': resp.found,
                    'visited_count': resp.visited_count,
                    'runtime_ms': f'{resp.runtime_ms:.2f}',
                    'error_message': ''
                }
                
                # Log the event
                self._log_event(result)
                
                return result
                
            except grpc.RpcError as e:
                # gRPC error - try next replica
                error_code = e.code().name if hasattr(e, 'code') and hasattr(e.code(), 'name') else 'UNKNOWN'
                error_msg = f"gRPC Error: {error_code}"
                
                if attempt < len(self.replicas) - 1:
                    # Not the last attempt, continue to next replica
                    print(f"  ⚠ Request #{request_id} → {endpoint} FAILED ({error_code}) - Retrying...")
                    continue
                else:
                    # Last attempt failed
                    end_time = time.time()
                    total_latency_ms = (end_time - start_time) * 1000.0
                    
                    with self.stats_lock:
                        self.failure_counter += 1
                    
                    result = {
                        'request_id': request_id,
                        'event_timestamp': event_timestamp,
                        'processing_timestamp': datetime.datetime.now().isoformat(),
                        'endpoint': 'ALL_REPLICAS',
                        'attempt_number': len(self.replicas),
                        'status': 'FAILED',
                        'latency_ms': '0.00',
                        'total_latency_ms': f'{total_latency_ms:.2f}',
                        'found': False,
                        'visited_count': 0,
                        'runtime_ms': '0.00',
                        'error_message': error_msg
                    }
                    
                    self._log_event(result)
                    return result
                    
            except Exception as e:
                # Other error
                error_msg = str(e)[:100]
                
                if attempt < len(self.replicas) - 1:
                    print(f"  ⚠ Request #{request_id} → {endpoint} ERROR ({error_msg[:50]}) - Retrying...")
                    continue
                else:
                    end_time = time.time()
                    total_latency_ms = (end_time - start_time) * 1000.0
                    
                    with self.stats_lock:
                        self.failure_counter += 1
                    
                    result = {
                        'request_id': request_id,
                        'event_timestamp': event_timestamp,
                        'processing_timestamp': datetime.datetime.now().isoformat(),
                        'endpoint': 'ALL_REPLICAS',
                        'attempt_number': len(self.replicas),
                        'status': 'FAILED',
                        'latency_ms': '0.00',
                        'total_latency_ms': f'{total_latency_ms:.2f}',
                        'found': False,
                        'visited_count': 0,
                        'runtime_ms': '0.00',
                        'error_message': error_msg
                    }
                    
                    self._log_event(result)
                    return result


def stream_requests(client, requests_per_second, duration):
    """
    Generate and process streaming requests at a configurable rate
    
    This simulates a real streaming scenario where events arrive continuously
    """
    interval = 1.0 / requests_per_second  # Time between requests
    request_id = 0
    start_time = time.time()
    
    print(f"\n🌊 Starting request stream at {requests_per_second} requests/second")
    print(f"   (1 request every {interval*1000:.0f}ms)\n")
    
    batch_counter = 0
    batch_size = requests_per_second * 2  # Show stats every 2 seconds
    
    while time.time() - start_time < duration:
        # Generate timestamp for this event
        event_timestamp = datetime.datetime.now().isoformat()
        
        # Process the request
        req_start = time.time()
        
        # Send request (with fault tolerance)
        result = client.call_grpc_with_retry(request_id, event_timestamp)
        
        # Print progress
        if result['status'] == 'SUCCESS':
            if result['attempt_number'] == 1:
                print(f"[{datetime.datetime.now().strftime('%H:%M:%S.%f')[:-3]}] "
                      f"✓ Stream Event #{request_id} → {result['endpoint']} "
                      f"(latency: {result['latency_ms']}ms)")
            else:
                print(f"[{datetime.datetime.now().strftime('%H:%M:%S.%f')[:-3]}] "
                      f"✓ Stream Event #{request_id} → {result['endpoint']} "
                      f"(RETRY SUCCESS after {result['attempt_number']-1} failure(s), "
                      f"latency: {result['latency_ms']}ms)")
        else:
            print(f"[{datetime.datetime.now().strftime('%H:%M:%S.%f')[:-3]}] "
                  f"✗ Stream Event #{request_id} → ALL REPLICAS FAILED")
        
        request_id += 1
        batch_counter += 1
        
        # Show statistics every batch_size requests
        if batch_counter >= batch_size:
            elapsed = time.time() - start_time
            print(f"\n  📊 Stream Statistics (after {elapsed:.1f}s):")
            print(f"     Events Processed: {client.request_counter}")
            print(f"     Successes: {client.success_counter} ({client.success_counter/max(client.request_counter,1)*100:.1f}%)")
            print(f"     Failures: {client.failure_counter}")
            print(f"     Actual Rate: {client.request_counter/elapsed:.2f} events/sec\n")
            batch_counter = 0
        
        # Sleep to maintain the desired request rate
        req_duration = time.time() - req_start
        sleep_time = max(0, interval - req_duration)
        if sleep_time > 0:
            time.sleep(sleep_time)


def main():
    parser = argparse.ArgumentParser(description='Simple Streaming DFS Client (No Spark Required)')
    parser.add_argument('--replicas', type=str, default='localhost:50051,localhost:50052',
                        help='Comma-separated list of replica endpoints')
    parser.add_argument('--requests-per-second', type=int, default=10,
                        help='Number of requests to generate per second (ADJUSTABLE RATE)')
    parser.add_argument('--duration', type=int, default=60,
                        help='Duration to run streaming in seconds (min: 60)')
    parser.add_argument('--target', type=int, default=42000,
                        help='Target vertex for DFS')
    parser.add_argument('--num-vertices', type=int, default=50000,
                        help='Number of vertices in graph')
    parser.add_argument('--per-call-timeout', type=int, default=3,
                        help='Timeout for each gRPC call in seconds')
    parser.add_argument('--log-file', type=str, default='streaming_events.csv',
                        help='Output CSV file for detailed event logs')
    
    args = parser.parse_args()
    
    # Parse replicas
    replicas = [r.strip() for r in args.replicas.split(',') if r.strip()]
    
    # Validation
    if args.duration < 60:
        print(f"⚠ Warning: Duration should be at least 60 seconds. Setting to 60.")
        args.duration = 60
    
    print("\n" + "="*80)
    print("🌊 STREAMING DFS CLIENT (Simple Implementation)")
    print("="*80)
    print(f"\n📋 Configuration:")
    print(f"   Replicas: {', '.join(replicas)}")
    print(f"   ⚡ Request Rate: {args.requests_per_second} requests/second (ADJUSTABLE)")
    print(f"   ⏱  Duration: {args.duration} seconds")
    print(f"   📊 Expected Total Events: ~{args.requests_per_second * args.duration}")
    print(f"   🎯 Target Vertex: {args.target}")
    print(f"   🔢 Number of Vertices: {args.num_vertices}")
    print(f"   ⏰ Per-call Timeout: {args.per_call_timeout}s")
    print(f"   📁 Event Log File: {args.log_file}")
    print(f"\n📡 STREAMING EXPLAINED:")
    print(f"   • Continuous stream of DFS requests generated at {args.requests_per_second} req/sec")
    print(f"   • Each request is an 'event' with a timestamp")
    print(f"   • Events are immediately processed and sent to gRPC servers")
    print(f"   • Round-robin load balancing across {len(replicas)} replica(s)")
    print(f"   • Automatic failover if a replica fails")
    print(f"   • All events logged with timestamps: event_time + processing_time")
    print(f"   • Stream runs for {args.duration} seconds")
    print("="*80 + "\n")
    
    # Create client
    client = StreamingDFSClient(
        replicas, 
        args.target, 
        args.num_vertices, 
        args.per_call_timeout,
        args.log_file
    )
    
    print(f"✓ Client initialized")
    print(f"✓ Event log file created: {args.log_file}\n")
    
    # Start streaming
    start_time = time.time()
    
    try:
        stream_requests(client, args.requests_per_second, args.duration)
    except KeyboardInterrupt:
        print("\n\n⚠ Stream interrupted by user")
    
    end_time = time.time()
    actual_duration = end_time - start_time
    
    # Final statistics
    print("\n" + "="*80)
    print("📊 FINAL STREAMING STATISTICS")
    print("="*80)
    print(f"Total Stream Events: {client.request_counter}")
    print(f"Successful Events: {client.success_counter} ({client.success_counter/max(client.request_counter,1)*100:.1f}%)")
    print(f"Failed Events: {client.failure_counter} ({client.failure_counter/max(client.request_counter,1)*100:.1f}%)")
    print(f"Actual Duration: {actual_duration:.2f} seconds")
    print(f"Target Rate: {args.requests_per_second} events/second")
    print(f"Actual Rate: {client.request_counter / actual_duration:.2f} events/second")
    print(f"Rate Accuracy: {(client.request_counter / actual_duration) / args.requests_per_second * 100:.1f}%")
    print(f"\n📁 Detailed event logs with timestamps saved to: {args.log_file}")
    print(f"   Open this CSV file to see:")
    print(f"   - event_timestamp: When each event was generated")
    print(f"   - processing_timestamp: When it was processed")
    print(f"   - endpoint: Which server handled it")
    print(f"   - latency_ms: Response time")
    print(f"   - status: SUCCESS or FAILED")
    print("="*80 + "\n")


if __name__ == '__main__':
    main()
