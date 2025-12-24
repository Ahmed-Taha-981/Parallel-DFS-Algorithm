# PowerShell script to start multiple gRPC server replicas
# Usage: .\run_replicas.ps1 [num_replicas] [start_port]

param(
    [int]$NumReplicas = 2,
    [int]$StartPort = 50051,
    [string]$ExecCmd = "..\MPI_DFS.exe",
    [int]$MpiProcs = 4,
    [string]$LogDir = "."
)

Write-Host "Starting $NumReplicas replica(s) on ports $StartPort to $($StartPort + $NumReplicas - 1)..." -ForegroundColor Green

$jobs = @()

for ($i = 0; $i -lt $NumReplicas; $i++) {
    $port = $StartPort + $i
    $logfile = Join-Path $LogDir "server_$port.log"
    
    Write-Host "Starting replica on port $port (log: $logfile)..." -ForegroundColor Yellow
    
    $currentDir = Get-Location
    $job = Start-Job -ScriptBlock {
        param($port, $exec, $mpiProcs, $logfile, $workDir)
        Set-Location $workDir
        python server.py --port $port --exec $exec --mpi-procs $mpiProcs --logfile $logfile
    } -ArgumentList $port, $ExecCmd, $MpiProcs, $logfile, $currentDir
    
    $jobs += $job
    Start-Sleep -Seconds 1  # Small delay between starts
}

Write-Host "`nAll replicas started. Job IDs:" -ForegroundColor Green
$jobs | ForEach-Object { Write-Host "  Port $($StartPort + $jobs.IndexOf($_)): Job ID $($_.Id)" }

Write-Host "`nTo stop all replicas, run: Get-Job | Stop-Job; Get-Job | Remove-Job" -ForegroundColor Cyan
Write-Host "To view logs, check: $LogDir\server_*.log" -ForegroundColor Cyan
Write-Host "`nPress Ctrl+C to stop this script (replicas will continue running in background)" -ForegroundColor Yellow

# Keep script running to show status
try {
    while ($true) {
        Start-Sleep -Seconds 5
        $running = (Get-Job | Where-Object { $_.State -eq 'Running' }).Count
        Write-Host "`r[$((Get-Date).ToString('HH:mm:ss'))] Running replicas: $running/$NumReplicas" -NoNewline
    }
} catch {
    Write-Host "`nStopping..." -ForegroundColor Yellow
}
