#!/usr/bin/env pwsh
param (
	[string]$addr,
    [int]$port=7000,
	[int]$count=3,
    [int]$shards=3,
	[int]$replicas=0,
	[switch]$redis,
	[switch]$tls,
	[string]$password="empty",
	[switch]$Help
)

if ($Help -or (-not $addr -and -not $redis)) {
    Write-Host "Usage: simple-setup.ps1 [-addr <ip>] [-port <n>] [-count <n>] [-shards <n>] [-replicas <n>] [-redis] [-tls] [-password <pass>]"
    Write-Host ""
    Write-Host "Set up a redis/valkey/garnet cluster by assigning slots and issuing CLUSTER MEET."
    Write-Host ""
    Write-Host "Options:"
    Write-Host "  -addr       Target address (auto-detected from listening port if omitted)"
    Write-Host "  -port       Base port (default: 7000)"
    Write-Host "  -count      Number of nodes (default: 3)"
    Write-Host "  -shards     Number of shards for slot assignment (default: 3)"
    Write-Host "  -replicas   Number of replicas (default: 0)"
    Write-Host "  -redis      Use redis-cli --cluster create instead of manual setup"
    Write-Host "  -tls        Enable TLS connections"
    Write-Host "  -password   Password for AUTH (default: none)"
    Write-Host "  -Help       Show this help message"
    return
}

if ($addr) {	
	Write-Host "Using input address!"
	$address=$addr	
}
else{
	Write-Host "Trying to find address from process port!"
	$address = "127.0.0.1"
	# Used to find collection
	$connection = Get-NetTCPConnection -State Listen | Where-Object { $_.LocalPort -eq $port }
	$process = Get-Process -Id $connection.OwningProcess
	$address = $connection.LocalAddress

	# Check if the IP address is unspecified (:: or 0.0.0.0), and use the loopback address instead
	if ($address -eq "::") {
		$address = "[::1]"
	}ElseIf($address -eq "0.0.0.0"){
		$address = "127.0.0.1"
	}
}

Write-Host "Address:${address}, Port:$port, Count:$count, Shards:$shards"

function getCommand($port, $password, $tls, $redisCommand) {
	$cli = "redis-cli -h ${address} -p ${port}"
	if($tls){
		$cli += " --tls --insecure"
	}

	if($password -ne "empty"){
		$cli+=" -a ${password}"
	}
	
	$cli += " $redisCommand"
	
	return $cli
}

function invokeCommand($cli){
	$invoke = @($cli) -join " "
	$result = Invoke-Expression $invoke
	return $result
}

function issueCommand($port, $password, $tls, $redisCommand){
	$cli = getCommand $port $password $tls $redisCommand
	$result = invokeCommand $cli
	$isError = $result -like "ERR*"
	
	Write-Host $cli -ForegroundColor Cyan -NoNewline
	Write-Host " [$result]" -ForegroundColor $(if ($isError) { "Red" } else { "Green" })
	return $result
}

function getEndpoint($port, $password, $tls){
	Write-Host "[Get Source Endpoint]" -ForegroundColor Yellow
	$result = issueCommand $port $password $tls "cluster myid"	
	$endpoint = issueCommand $port $password $tls "cluster endpoint ${result}"	
	return $endpoint
}

if($redis){
	$create=""
	for($p=$port;$p -lt $port + $count;$p++){
		$create = $create + $address+":"+[String]$p + " "
		
	}
	echo $create
	
	$command=@("redis-cli --cluster create ${create} --cluster-yes") -join " "
	invoke-expression $command	
	
}else{
	Write-Host "[Set config epoch]" -ForegroundColor Yellow
	for($p=$port;$p -lt $port + $count;$p++)
	{	
		$epoch = $p - $port + 1	
		$result = issueCommand $p $password $tls "cluster set-config-epoch $epoch"
	}

	######################
	#### Assign Slots ####
	######################
	Write-Host "[Assigning Slots]" -ForegroundColor Yellow
	$slots=16384
	$range = [math]::Ceiling($slots / $shards) # Calculate the range for each shard
	$p=$port
	for ($i = 0; $i -lt $slots; $i += $range) {
		$s = $i
		$e = $s + $range - 1 # Subtract 1 to get the correct end value

		# Ensure that the end value does not exceed the total number of slots
		if ($e -ge $slots) {
			$e = $slots - 1
		}	

		$result = issueCommand $p $password $tls "cluster addslotsrange $s $e"		
		$p++
	}

	###################
	#### Find Port ####
	###################
	$result=getEndpoint $port $password $tls

	######################
	#### Cluster Meet ####
	######################
	Write-Host "[Issue Meet]" -ForegroundColor Yellow
	$sourceAddr=$result.Substring(0,$result.lastIndexOf(':'))
	$sourcePort = $result.Split(":")[-1]	
	Write-Host $sourceAddr $sourcePort
	for($p=$port + 1;$p -lt $port + $count;$p++)
	{	
		# Get target endpoint
		$result=getEndpoint $p $password $tls
		
		# Execute Meet
		$targetAddr=$result.Substring(0,$result.lastIndexOf(':'))
		$targetPort = $result.Split(":")[-1]
		$result = issueCommand $port $password $tls "cluster meet ${targetAddr} ${targetPort}"
	}
	
	Start-Sleep -Seconds 2
	Write-Host "<<<< Cluster Config >>>>" -ForegroundColor Yellow
	for($p=$port;$p -lt $port + $count;$p++)
	{
		$cli = getCommand $p $password $tls "cluster nodes"
		$result = ($cli) -join " "
		Write-Host "[" $cli "]" -ForegroundColor Cyan
		Invoke-Expression $result
		Write-Host $output -ForegroundColor Yellow
	}
}