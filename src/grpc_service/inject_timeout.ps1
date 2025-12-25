# Fault Injection Script: Network/Service Disruption via Timeout
# Temporarily suspends a gRPC server process to simulate network timeout/disruption
# Usage: .\inject_timeout.ps1 -Port 50051 -Duration 10

param(
    [Parameter(Mandatory=$true)]
    [int]$Port,
    
    [Parameter(Mandatory=$true)]
    [int]$Duration = 10
)

# Define suspend/resume functions using Windows API (works across PowerShell versions)
Add-Type -TypeDefinition @"
using System;
using System.Runtime.InteropServices;
public class ProcessSuspend {
    [DllImport("kernel32.dll")]
    static extern IntPtr OpenThread(uint dwDesiredAccess, bool bInheritHandle, uint dwThreadId);
    [DllImport("kernel32.dll")]
    static extern uint SuspendThread(IntPtr hThread);
    [DllImport("kernel32.dll")]
    static extern int ResumeThread(IntPtr hThread);
    [DllImport("kernel32", CharSet = CharSet.Auto, SetLastError = true)]
    static extern bool CloseHandle(IntPtr handle);
    const uint THREAD_SUSPEND_RESUME = 0x0002;
    
    public static void SuspendProcess(int pid) {
        foreach (System.Diagnostics.ProcessThread thread in System.Diagnostics.Process.GetProcessById(pid).Threads) {
            IntPtr hThread = OpenThread(THREAD_SUSPEND_RESUME, false, (uint)thread.Id);
            if (hThread != IntPtr.Zero) {
                SuspendThread(hThread);
                CloseHandle(hThread);
            }
        }
    }
    
    public static void ResumeProcess(int pid) {
        foreach (System.Diagnostics.ProcessThread thread in System.Diagnostics.Process.GetProcessById(pid).Threads) {
            IntPtr hThread = OpenThread(THREAD_SUSPEND_RESUME, false, (uint)thread.Id);
            if (hThread != IntPtr.Zero) {
                ResumeThread(hThread);
                CloseHandle(hThread);
            }
        }
    }
}
"@

Write-Host "Fault Injection: Network/Service Disruption (Timeout)" -ForegroundColor Red
Write-Host "Target: Port $Port" -ForegroundColor Yellow
Write-Host "Disruption duration: $Duration seconds" -ForegroundColor Yellow

# Find process listening on the port
$process = Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue | 
    Select-Object -ExpandProperty OwningProcess -Unique

if ($process) {
    $procInfo = Get-Process -Id $process -ErrorAction SilentlyContinue
    if ($procInfo) {
        Write-Host "`n[$(Get-Date -Format 'HH:mm:ss')] DISRUPTION STARTED: Suspending process $($procInfo.ProcessName) (PID: $process) on port $Port" -ForegroundColor Red
        
        try {
            # Suspend the process (simulates network timeout/unresponsiveness)
            [ProcessSuspend]::SuspendProcess($process)
            Write-Host "Process suspended. Waiting $Duration seconds..." -ForegroundColor Yellow
            Start-Sleep -Seconds $Duration
            
            Write-Host "`n[$(Get-Date -Format 'HH:mm:ss')] RECOVERY: Resuming process..." -ForegroundColor Green
            [ProcessSuspend]::ResumeProcess($process)
            Write-Host "Process resumed. Service should be operational again." -ForegroundColor Green
        } catch {
            Write-Host "Failed to suspend/resume process: $_" -ForegroundColor Yellow
            Write-Host "Error: Process may have terminated or insufficient privileges." -ForegroundColor Yellow
        }
    } else {
        Write-Host "Process not found (may have already terminated)." -ForegroundColor Yellow
    }
} else {
    Write-Host "No process found listening on port $Port" -ForegroundColor Yellow
}
