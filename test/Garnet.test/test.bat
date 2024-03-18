:loop
dotnet test -c Debug --logger:"console;verbosity=detailed" --filter Cluster
goto loop