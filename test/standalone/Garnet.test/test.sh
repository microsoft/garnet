#!/bin/bash

testname=ClusterTLSRPrimaryCheckpointRetrieve
config=Release
framework=net8.0

# First run does build
dotnet test -c $config --logger:"console;verbosity=detailed" --framework:$framework --filter $testname

while dotnet test --no-build -c $config --logger:"console;verbosity=detailed" --framework:$framework --filter $testname; do
   echo "Tests passed. Running again..."
done

echo "Tests failed."