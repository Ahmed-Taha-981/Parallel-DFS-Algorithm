import argparse
import os
import subprocess
import time
import datetime
import threading
import logging

import grpc
from concurrent.futures import ThreadPoolExecutor

# Try to compile proto if generated files don't exist
try:
    import dfs_pb2
    import dfs_pb2_grpc
except Exception:
    from grpc_tools import protoc
    protoc.main((
        '',
        '-I.',
        '--python_out=.',
        '--grpc_python_out=.',
        'dfs.proto',
    ))
    import dfs_pb2
    import dfs_pb2_grpc


class DFSServiceServicer(dfs_pb2_grpc.DFSServiceServicer):
    def __init__(self, exec_cmd, use_mpi=True, mpi_procs=4, logfile=None, timeout=120):
        self.exec_cmd = exec_cmd
        self.use_mpi = use_mpi
        self.mpi_procs = mpi_procs
        self.timeout = timeout
        self.logfile = logfile or 'server.log'
        logging.basicConfig(filename=self.logfile, level=logging.INFO,
                            format='%(asctime)s %(message)s')

    def RunDFS(self, request, context):
        req_ts = datetime.datetime.utcnow().isoformat() + 'Z'
        start = time.time()

        # Build command without modifying DFS source
        cmd = []
        if self.use_mpi and request.use_mpi:
            # Use mpirun to launch the MPI binary
            cmd = ["mpirun", "-np", str(self.mpi_procs), self.exec_cmd]
        else:
            cmd = [self.exec_cmd]

        # Optionally pass target/numVertices as env vars
        env = os.environ.copy()
        if request.num_vertices:
            env['NUM_VERTICES'] = str(request.num_vertices)
        if request.target:
            env['TARGET_VERTEX'] = str(request.target)

        try:
            proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, timeout=self.timeout, universal_newlines=True)
            stdout = proc.stdout
            stderr = proc.stderr
            rc = proc.returncode
        except subprocess.TimeoutExpired as e:
            stdout = getattr(e, 'stdout', '') or ''
            stderr = 'TimeoutExpired'
            rc = -1
        except Exception as e:
            stdout = ''
            stderr = str(e)
            rc = -1

        end = time.time()
        latency_ms = (end - start) * 1000.0

        # Parse output from DFS binary (supports both serial and MPI formats)
        found = False
        visited_count = 0
        runtime_ms = 0.0
        for line in stdout.splitlines():
            line = line.strip()
            # Support both formats:
            # MPI_DFS: "time taken: 45 ms" or "vertices visited: 1024"
            # serial/parallel: "Execution time: 8.727 milliseconds (ms)" or "Total vertices visited: 50000"
            if line.startswith('Execution time:'):
                try:
                    # "Execution time: 8.727 milliseconds (ms)"
                    parts = line.split(':', 1)[1].strip().split()
                    runtime_ms = float(parts[0])
                except Exception:
                    pass
            elif line.startswith('time taken:'):
                try:
                    # "time taken: x ms"
                    parts = line.split(':', 1)[1].strip().split()
                    runtime_ms = float(parts[0])
                except Exception:
                    pass
            if line.startswith('Total vertices visited:'):
                try:
                    visited_count = int(line.split(':', 1)[1].strip())
                except Exception:
                    pass
            elif line.startswith('vertices visited:'):
                try:
                    visited_count = int(line.split(':', 1)[1].strip())
                except Exception:
                    pass
            if line.startswith('found target:'):
                found = True

        # Log timestamp and latency
        logging.info('request_ts=%s latency_ms=%.2f exit_code=%d found=%s visited=%d', req_ts, latency_ms, rc, found, visited_count)

        return dfs_pb2.RunResponse(found=found, visited_count=visited_count, runtime_ms=runtime_ms, stdout=stdout, stderr=stderr)


def serve(port, exec_cmd, use_mpi=True, mpi_procs=4, logfile=None, timeout=120):
    server = grpc.server(ThreadPoolExecutor(max_workers=10))
    dfs_pb2_grpc.add_DFSServiceServicer_to_server(DFSServiceServicer(exec_cmd, use_mpi, mpi_procs, logfile, timeout), server)
    listen_addr = f'[::]:{port}'
    server.add_insecure_port(listen_addr)
    server.start()
    print(f"DFS gRPC server listening on {listen_addr} (exec={exec_cmd}, use_mpi={use_mpi}, mpi_procs={mpi_procs})")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=50051)
    parser.add_argument('--exec', dest='exec_cmd', type=str, default=os.path.join('..', 'MPI_DFS.exe'))
    parser.add_argument('--use-mpi', action='store_true')
    parser.add_argument('--mpi-procs', type=int, default=4)
    parser.add_argument('--logfile', type=str, default=None)
    parser.add_argument('--timeout', type=int, default=120)
    args = parser.parse_args()

    serve(args.port, args.exec_cmd, use_mpi=args.use_mpi, mpi_procs=args.mpi_procs, logfile=args.logfile, timeout=args.timeout)
