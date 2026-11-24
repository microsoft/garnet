[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$Peer,
    [ValidateRange(20, 1000)][int]$IntervalMs = 100,
    [string]$OutputPath
)
$ErrorActionPreference = 'Stop'
$route = @(& ip -j route get $Peer | ConvertFrom-Json)[0]
if ($LASTEXITCODE -ne 0 -or -not $route.dev) { throw 'Cannot resolve the data-plane interface.' }
$rxPath = "/sys/class/net/$($route.dev)/statistics/rx_bytes"
$txPath = "/sys/class/net/$($route.dev)/statistics/tx_bytes"
$rx = [long][IO.File]::ReadAllText($rxPath)
$tx = [long][IO.File]::ReadAllText($txPath)
$time = [datetime]::UtcNow
$writer = if ($OutputPath) { [IO.StreamWriter]::new($OutputPath, $false) }
if ($writer) { $writer.AutoFlush = $true }
function Write-SampleLine([string]$Line) {
    if ($writer) { $writer.WriteLine($Line) } else { Write-Output $Line }
}
try {
    Write-SampleLine "INTERFACE=$($route.dev)"
    Write-SampleLine 'StartUtc,EndUtc,RxBytes,TxBytes'
    while ($true) {
        Start-Sleep -Milliseconds $IntervalMs
        $nextRx = [long][IO.File]::ReadAllText($rxPath)
        $nextTx = [long][IO.File]::ReadAllText($txPath)
        $nextTime = [datetime]::UtcNow
        Write-SampleLine ("{0},{1},{2},{3}" -f $time.ToString('o'), $nextTime.ToString('o'), ($nextRx - $rx), ($nextTx - $tx))
        $rx = $nextRx
        $tx = $nextTx
        $time = $nextTime
    }
} finally {
    if ($writer) { $writer.Dispose() }
}
