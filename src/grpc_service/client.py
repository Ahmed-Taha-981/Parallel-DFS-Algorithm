import argparse
import time
import datetime
import logging
import grpc

# Try import generated stubs
import dfs_pb2
import dfs_pb2_grpc


def call_one(stub, target, num_vertices, timeout_s=10):
    req = dfs_pb2.RunRequest(target=target or 0, num_vertices=num_vertices or 0, use_mpi=True)
    start = time.time()
    try:
        resp = stub.RunDFS(req, timeout=timeout_s)
        latency_ms = (time.time() - start) * 1000.0
        return True, resp, latency_ms
    except grpc.RpcError as e:
        return False, e, None


def main(replicas, duration, target, num_vertices, per_call_timeout, logfile):
    logging.basicConfig(filename=logfile, level=logging.INFO,
                        format='%(asctime)s %(message)s')
    start_ts = time.time()
    idx = 0
    total = 0
    successes = 0

    while time.time() - start_ts < duration:
        endpoint = replicas[idx % len(replicas)]
        idx += 1
        total += 1
        try:
            channel = grpc.insecure_channel(endpoint)
            stub = dfs_pb2_grpc.DFSServiceStub(channel)
            ok, resp, latency_ms = call_one(stub, target, num_vertices, timeout_s=per_call_timeout)
            if ok:
                successes += 1
                logging.info('endpoint=%s latency_ms=%.2f found=%s visited=%d runtime_ms=%.2f', endpoint, latency_ms, resp.found, resp.visited_count, resp.runtime_ms)
            else:
                err = resp
                logging.warning('endpoint=%s error=%s', endpoint, err)
                # try next replica immediately
                continue
        except Exception as e:
            logging.warning('endpoint=%s exception=%s', endpoint, e)
            continue

        # small pause between requests to avoid busy looping
        time.sleep(0.1)

    print(f"Finished: total attempts={total}, successes={successes}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--replicas', type=str, default='localhost:50051,localhost:50052')
    parser.add_argument('--duration', type=int, default=60)
    parser.add_argument('--target', type=int, default=42000)
    parser.add_argument('--num-vertices', type=int, default=50000)
    parser.add_argument('--per-call-timeout', type=int, default=30)
    parser.add_argument('--logfile', type=str, default='client.log')
    args = parser.parse_args()

    replicas = [r.strip() for r in args.replicas.split(',') if r.strip()]
    main(replicas, args.duration, args.target, args.num_vertices, args.per_call_timeout, args.logfile)
