REM Usage: \runGarnetTests.cmd 100 Debug ClusterUseTLSReplicationTests

echo off
setlocal enabledelayedexpansion

SET numCount=%1
SET config=%2
SET filter=%3

echo [Test Configuration]
echo Iteration: %numCount%
echo Configuration: %config%
dotnet build -c !config!&& cd ../../../

echo [==================]
FOR /L %%A IN (1,1,%numCount%) DO (
  echo [============================= Running iteration number %%A out of %numCount%, started on %time% =============================]
  cd .\Garnet\test\Garnet.test\ && dotnet test -c !config! --logger:"console;verbosity=detailed" --filter !filter! --no-build && cd ../../../
  echo [============================= Ended iteration number %%A out of %numCount%, completed on %time% ===============================]
)