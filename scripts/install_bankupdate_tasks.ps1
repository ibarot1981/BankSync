param(
    [string]$TaskUser = "$env:USERDOMAIN\$env:USERNAME",
    [string]$RepoRoot = "D:\Irshad\Dev\Python\BankUpdate"
)

$ErrorActionPreference = "Stop"

function New-BankUpdateTask {
    param(
        [string]$TaskName,
        [Microsoft.Management.Infrastructure.CimInstance[]]$Triggers,
        [string]$Arguments
    )

    $action = New-ScheduledTaskAction -Execute "$RepoRoot\run_sync.bat" -Argument $Arguments -WorkingDirectory $RepoRoot
    $settings = New-ScheduledTaskSettingsSet `
        -StartWhenAvailable `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -MultipleInstances IgnoreNew
    $principal = New-ScheduledTaskPrincipal -UserId $TaskUser -LogonType S4U -RunLevel Highest

    Register-ScheduledTask `
        -TaskName $TaskName `
        -Action $action `
        -Trigger $Triggers `
        -Settings $settings `
        -Principal $principal `
        -Force | Out-Null
}

$dailyMainTrigger = New-ScheduledTaskTrigger -Daily -At 11:30AM
$dailyFallbackTrigger = New-ScheduledTaskTrigger -Daily -At 1:00PM
$retryStart = (Get-Date).AddMinutes(5)
$retryTrigger = New-ScheduledTaskTrigger -Once -At $retryStart
$retryTrigger.Repetition = (New-ScheduledTaskRepetitionSettings -Interval (New-TimeSpan -Hours 1))

New-BankUpdateTask -TaskName "BankUpdate Daily Main" -Triggers @($dailyMainTrigger) -Arguments "daily"
New-BankUpdateTask -TaskName "BankUpdate Daily Fallback" -Triggers @($dailyFallbackTrigger) -Arguments "daily"
New-BankUpdateTask -TaskName "BankUpdate Retry Hourly" -Triggers @($retryTrigger) -Arguments "retry"

Write-Host "Installed BankUpdate scheduled tasks:"
Write-Host "- BankUpdate Daily Main"
Write-Host "- BankUpdate Daily Fallback"
Write-Host "- BankUpdate Retry Hourly"
