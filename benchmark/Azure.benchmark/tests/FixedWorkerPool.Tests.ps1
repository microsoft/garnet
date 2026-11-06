$benchmarkDir = Split-Path -Parent $PSScriptRoot

Describe 'Fixed worker pool command generation' {
    $tokens = $null
    $parseErrors = $null
    $ast = [System.Management.Automation.Language.Parser]::ParseFile(
        (Join-Path $benchmarkDir '03-workload\bench\resp-bench.ps1'),
        [ref]$tokens, [ref]$parseErrors)
    if ($parseErrors.Count) { throw ($parseErrors | Out-String) }
    $assignment = $ast.EndBlock.Statements | Where-Object {
        $_ -is [System.Management.Automation.Language.AssignmentStatementAst] -and
        $_.Left.Extent.Text -eq '$useFixedWorkerPool'
    }

    $append = $ast.EndBlock.Statements | Where-Object {
        $_ -is [System.Management.Automation.Language.IfStatementAst] -and
        $_.Clauses[0].Item1.Extent.Text -eq '$useFixedWorkerPool -eq "true"'
    }
    if (-not $assignment -or -not $append) {
        throw 'Cannot locate fixed worker pool command generation.'
    }
    $resolveOption = [scriptblock]::Create($assignment.Extent.Text)
    $appendOption = [scriptblock]::Create($append.Extent.Text)

    It 'maps <Setting> to the benchmark flag without launching SSH' -TestCases @(
        @{ Setting = 'true'; Expected = 'Resp.benchmark --use-fixed-worker-pool' }
        @{ Setting = 'false'; Expected = 'Resp.benchmark' }
        @{ Setting = $null; Expected = 'Resp.benchmark --use-fixed-worker-pool' }
    ) {
        param($Setting, $Expected)
        $config = @{}
        if ($null -ne $Setting) { $config.UseFixedWorkerPool = $Setting }
        $benchCmd = 'Resp.benchmark'

        . $resolveOption
        . $appendOption

        $benchCmd | Should Be $Expected
    }

}

Describe 'Parallel fan-out command generation' {
    $tokens = $null
    $parseErrors = $null
    $ast = [System.Management.Automation.Language.Parser]::ParseFile(
        (Join-Path $benchmarkDir '03-workload\bench\resp-bench.ps1'),
        [ref]$tokens, [ref]$parseErrors)
    if ($parseErrors.Count) { throw ($parseErrors | Out-String) }
    $assignment = $ast.EndBlock.Statements | Where-Object {
        $_ -is [System.Management.Automation.Language.AssignmentStatementAst] -and
        $_.Left.Extent.Text -eq '$useParallelFanOut'
    }
    $append = $ast.EndBlock.Statements | Where-Object {
        $_ -is [System.Management.Automation.Language.IfStatementAst] -and
        $_.Clauses[0].Item1.Extent.Text -eq '$useParallelFanOut -eq "true"'
    }
    if (-not $assignment -or -not $append) {
        throw 'Cannot locate parallel fan-out command generation.'
    }
    $resolveOption = [scriptblock]::Create($assignment.Extent.Text)
    $appendOption = [scriptblock]::Create($append.Extent.Text)

    It 'maps <Setting> to the fan-out flag without launching SSH' -TestCases @(
        @{ Setting = 'true'; Expected = 'Resp.benchmark --use-parallel-fan-out' }
        @{ Setting = 'false'; Expected = 'Resp.benchmark' }
        @{ Setting = $null; Expected = 'Resp.benchmark' }
    ) {
        param($Setting, $Expected)
        $config = @{}
        if ($null -ne $Setting) { $config.UseParallelFanOut = $Setting }
        $benchCmd = 'Resp.benchmark'

        . $resolveOption
        . $appendOption

        $benchCmd | Should Be $Expected
    }

}
