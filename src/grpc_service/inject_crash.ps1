# Fault Injection Script: Service Crash
# Kills a gRPC server replica to simulate service crash failure
# Usage: .\inject_crash.ps1 -Port 50051 [-Delay 5]

param(
    [Parameter(Mandatory=$true)]
    [int]$Port,
    
    [int]$Delay = 5
)

Write-Host "Fault Injection: Service Crash" -ForegroundColor Red
Write-Host "Target: Port $Port" -ForegroundColor Yellow
Write-Host "Delay before crash: $Delay seconds" -ForegroundColor Yellow

Start-Sleep -Seconds $Delay

# Find process listening on the port
$process = Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue | 
    Select-Object -ExpandProperty OwningProcess -Unique

if ($process) {
    $procInfo = Get-Process -Id $process -ErrorAction SilentlyContinue
    if ($procInfo) {
        Write-Host "`n[$(Get-Date -Format 'HH:mm:ss')] CRASH INJECTED: Killing process $($procInfo.ProcessName) (PID: $process) on port $Port" -ForegroundColor Red
        Stop-Process -Id $process -Force
        Write-Host "Process terminated successfully." -ForegroundColor Green
    } else {
        Write-Host "Process not found (may have already terminated)." -ForegroundColor Yellow
    }
} else {
    Write-Host "No process found listening on port $Port" -ForegroundColor Yellow
    Write-Host "Available connections:" -ForegroundColor Cyan
    Get-NetTCPConnection | Where-Object { $_.LocalPort -ge 50051 -and $_.LocalPort -le 50060 } | 
        Format-Table LocalPort, State, OwningProcess -AutoSize
}
