"""
Streaming DFS Client with PySpark Structured Streaming

WHAT IS BEING STREAMED:
- A continuous stream of DFS computation requests generated at a configurable rate
- Each request contains: request_id, timestamp, target_vertex, num_vertices
- Requests are processed in micro-batches and sent to gRPC servers with fault tolerance

HOW IT WORKS:
1. PySpark generates a stream using the 'rate' source (generates rows with timestamp + value)
2. Each row represents a DFS request to be sent to the server
3. Requests are load-balanced across replicas in round-robin fashion
4. If a replica fails, automatically retries on other replicas (fault tolerance)
5. All events (request sent, response received, failures) are logged with timestamps

STREAMING ARCHITECTURE:
- Source: Spark Rate Stream (generates N requests/second)
- Processing: foreach batch -> send gRPC requests with retry logic
- Sink: Console output + detailed CSV logs with timestamps
"""

import argparse
import time
import datetime
import grpc
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType, IntegerType
import dfs_pb2
import dfs_pb2_grpc


class StreamingDFSClient:
    """Handles streaming DFS requests with fault tolerance and detailed logging"""
    
    def __init__(self, replicas, target, num_vertices, per_call_timeout=3):
        self.replicas = replicas
        self.target = target
        self.num_vertices = num_vertices
        self.per_call_timeout = per_call_timeout
        self.request_counter = 0
        self.success_counter = 0
        self.failure_counter = 0
        
    def call_grpc_with_retry(self, request_id, timestamp):
        """
        Send a single gRPC request with automatic failover across replicas
        Returns: dict with detailed metrics
        """
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
                
                self.success_counter += 1
                
                return {
                    'request_id': request_id,
                    'event_timestamp': timestamp,
                    'processing_timestamp': datetime.datetime.now().isoformat(),
                    'endpoint': endpoint,
                    'attempt_number': attempt + 1,
                    'status': 'SUCCESS',
                    'latency_ms': latency_ms,
                    'total_latency_ms': total_latency_ms,
                    'found': resp.found,
                    'visited_count': resp.visited_count,
                    'runtime_ms': resp.runtime_ms,
                    'error_message': None
                }
                
            except grpc.RpcError as e:
                # gRPC error - try next replica
                error_code = e.code().name if hasattr(e, 'code') and hasattr(e.code(), 'name') else 'UNKNOWN'
                error_msg = f"gRPC Error: {error_code}"
                
                if attempt < len(self.replicas) - 1:
                    # Not the last attempt, continue to next replica
                    print(f"  ⚠ Request #{request_id} → {endpoint} FAILED ({error_code}) - Retrying on next replica...")
                    continue
                else:
                    # Last attempt failed
                    end_time = time.time()
                    total_latency_ms = (end_time - start_time) * 1000.0
                    self.failure_counter += 1
                    
                    return {
                        'request_id': request_id,
                        'event_timestamp': timestamp,
                        'processing_timestamp': datetime.datetime.now().isoformat(),
                        'endpoint': 'ALL_REPLICAS',
                        'attempt_number': len(self.replicas),
                        'status': 'FAILED',
                        'latency_ms': 0.0,
                        'total_latency_ms': total_latency_ms,
                        'found': False,
                        'visited_count': 0,
                        'runtime_ms': 0.0,
                        'error_message': error_msg
                    }
                    
            except Exception as e:
                # Other error
                error_msg = str(e)[:100]
                
                if attempt < len(self.replicas) - 1:
                    print(f"  ⚠ Request #{request_id} → {endpoint} ERROR ({error_msg}) - Retrying on next replica...")
                    continue
                else:
                    end_time = time.time()
                    total_latency_ms = (end_time - start_time) * 1000.0
                    self.failure_counter += 1
                    
                    return {
                        'request_id': request_id,
                        'event_timestamp': timestamp,
                        'processing_timestamp': datetime.datetime.now().isoformat(),
                        'endpoint': 'ALL_REPLICAS',
                        'attempt_number': len(self.replicas),
                        'status': 'FAILED',
                        'latency_ms': 0.0,
                        'total_latency_ms': total_latency_ms,
                        'found': False,
                        'visited_count': 0,
                        'runtime_ms': 0.0,
                        'error_message': error_msg
                    }
        
        # Should never reach here, but just in case
        return None


def process_batch(batch_df, batch_id, client, output_path):
    """
    Process each micro-batch of streaming requests
    
    This is where the actual streaming happens:
    - Each micro-batch contains multiple requests
    - Each request is sent to gRPC servers with fault tolerance
    - Results are collected and logged
    """
    print(f"\n{'='*80}")
    print(f"📦 PROCESSING MICRO-BATCH #{batch_id} at {datetime.datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
    print(f"{'='*80}")
    
    # Collect batch data
    requests = batch_df.collect()
    
    if not requests:
        print("  (Empty batch - no requests)")
        return
    
    print(f"  Batch contains {len(requests)} request(s)")
    
    # Process each request
    results = []
    for row in requests:
        request_id = row['value']
        timestamp = row['timestamp']
        
        print(f"\n  🚀 Sending Request #{request_id} at {datetime.datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
        
        # Call gRPC with retry logic
        result = client.call_grpc_with_retry(request_id, timestamp)
        
        if result:
            results.append(result)
            
            # Print result
            if result['status'] == 'SUCCESS':
                print(f"  ✓ Request #{result['request_id']} → {result['endpoint']} "
                      f"(latency: {result['latency_ms']:.2f}ms, visited: {result['visited_count']})")
            else:
                print(f"  ✗ Request #{result['request_id']} → FAILED after {result['attempt_number']} attempts "
                      f"(error: {result['error_message']})")
    
    # Create DataFrame from results and write to CSV
    if results:
        spark = SparkSession.builder.getOrCreate()
        
        # Define schema for results
        schema = StructType([
            StructField("request_id", LongType(), True),
            StructField("event_timestamp", StringType(), True),
            StructField("processing_timestamp", StringType(), True),
            StructField("endpoint", StringType(), True),
            StructField("attempt_number", IntegerType(), True),
            StructField("status", StringType(), True),
            StructField("latency_ms", DoubleType(), True),
            StructField("total_latency_ms", DoubleType(), True),
            StructField("found", BooleanType(), True),
            StructField("visited_count", IntegerType(), True),
            StructField("runtime_ms", DoubleType(), True),
            StructField("error_message", StringType(), True),
        ])
        
        results_df = spark.createDataFrame(results, schema)
        
        # Append to CSV log file
        results_df.write.mode('append').csv(output_path, header=True)
        
        print(f"\n  📝 Logged {len(results)} result(s) to {output_path}")
    
    # Print statistics
    print(f"\n  📊 Current Statistics:")
    print(f"     Total Requests: {client.request_counter}")
    print(f"     Successes: {client.success_counter} ({client.success_counter/max(client.request_counter,1)*100:.1f}%)")
    print(f"     Failures: {client.failure_counter} ({client.failure_counter/max(client.request_counter,1)*100:.1f}%)")
    print(f"{'='*80}\n")


def main():
    parser = argparse.ArgumentParser(description='Streaming DFS Client with PySpark')
    parser.add_argument('--replicas', type=str, default='localhost:50051,localhost:50052',
                        help='Comma-separated list of replica endpoints')
    parser.add_argument('--requests-per-second', type=int, default=10,
                        help='Number of requests to generate per second (adjustable rate)')
    parser.add_argument('--duration', type=int, default=60,
                        help='Duration to run streaming in seconds')
    parser.add_argument('--target', type=int, default=42000,
                        help='Target vertex for DFS')
    parser.add_argument('--num-vertices', type=int, default=50000,
                        help='Number of vertices in graph')
    parser.add_argument('--per-call-timeout', type=int, default=3,
                        help='Timeout for each gRPC call in seconds')
    parser.add_argument('--output-log', type=str, default='streaming_logs',
                        help='Output directory for detailed CSV logs')
    
    args = parser.parse_args()
    
    # Parse replicas
    replicas = [r.strip() for r in args.replicas.split(',') if r.strip()]
    
    print("\n" + "="*80)
    print("🌊 SPARK STREAMING DFS CLIENT")
    print("="*80)
    print(f"\n📋 Configuration:")
    print(f"   Replicas: {', '.join(replicas)}")
    print(f"   Request Rate: {args.requests_per_second} requests/second")
    print(f"   Duration: {args.duration} seconds")
    print(f"   Expected Total Requests: ~{args.requests_per_second * args.duration}")
    print(f"   Target Vertex: {args.target}")
    print(f"   Number of Vertices: {args.num_vertices}")
    print(f"   Per-call Timeout: {args.per_call_timeout}s")
    print(f"   Output Log Directory: {args.output_log}/")
    print(f"\n📡 STREAMING EXPLAINED:")
    print(f"   • Spark will generate a continuous stream of DFS requests")
    print(f"   • Rate source generates {args.requests_per_second} rows/second (each row = 1 request)")
    print(f"   • Requests are processed in micro-batches (every 2 seconds)")
    print(f"   • Each request is sent to gRPC servers with automatic failover")
    print(f"   • All events are timestamped and logged to CSV files")
    print(f"   • Stream runs for {args.duration} seconds then stops gracefully")
    print("="*80 + "\n")
    
    # Create Spark session
    print("🔧 Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("StreamingDFSClient") \
        .master("local[*]") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")  # Reduce Spark logging noise
    
    print("✓ Spark Session created successfully\n")
    
    # Create client
    client = StreamingDFSClient(replicas, args.target, args.num_vertices, args.per_call_timeout)
    
    # Create streaming DataFrame using rate source
    # The 'rate' source generates rows with: timestamp (current time) and value (incrementing number)
    print(f"🌊 Starting rate stream generator ({args.requests_per_second} requests/second)...")
    
    stream_df = spark.readStream \
        .format("rate") \
        .option("rowsPerSecond", args.requests_per_second) \
        .option("numPartitions", 1) \
        .load()
    
    print("✓ Stream source created\n")
    print("🚀 Starting stream processing...\n")
    
    # Start streaming query with foreachBatch
    query = stream_df.writeStream \
        .foreachBatch(lambda batch_df, batch_id: process_batch(batch_df, batch_id, client, args.output_log)) \
        .trigger(processingTime='2 seconds') \
        .start()
    
    # Run for specified duration
    start_time = time.time()
    try:
        while time.time() - start_time < args.duration and query.isActive:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n⚠ Interrupted by user")
    
    # Stop the stream
    print("\n\n🛑 Stopping stream...")
    query.stop()
    query.awaitTermination(timeout=10)
    
    # Final statistics
    print("\n" + "="*80)
    print("📊 FINAL STATISTICS")
    print("="*80)
    print(f"Total Requests Sent: {client.request_counter}")
    print(f"Successful Requests: {client.success_counter} ({client.success_counter/max(client.request_counter,1)*100:.1f}%)")
    print(f"Failed Requests: {client.failure_counter} ({client.failure_counter/max(client.request_counter,1)*100:.1f}%)")
    print(f"Actual Duration: {time.time() - start_time:.1f} seconds")
    print(f"Average Request Rate: {client.request_counter / (time.time() - start_time):.2f} requests/second")
    print(f"\n📁 Detailed logs saved to: {args.output_log}/")
    print("="*80 + "\n")
    
    # Stop Spark
    spark.stop()


if __name__ == '__main__':
    main()
