# Run Instructions

## Terminal 1 (Replica 1)
```powershell
cd src\grpc_service
python server.py --port 50051 --logfile server_50051.log
```

## Terminal 2 (Replica 2)
```powershell
cd src\grpc_service
python server.py --port 50052 --logfile server_50052.log
```

## Terminal 3 (Client)
```powershell
cd src\grpc_service
python simple_streaming_client.py --replicas localhost:50051,localhost:50052 --requests-per-second 10 --duration 60
```
