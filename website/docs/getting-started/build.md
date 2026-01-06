---
id: build
sidebar_label: Build
title: Build and Test
slug: /getting-started
---

## Clone from Sources

Clone the Garnet repo. Garnet is located on the main branch of that repo.

```bash
git clone git@github.com:microsoft/garnet.git
```

## Build the Project

Make sure .NET 10 SDK is installed, following instructions [here](https://dotnet.microsoft.com/en-us/download). You can use either Linux or Windows; Garnet works equally well on both platforms.

Go to the root folder of the repo and build using .NET CLI, or open `Garnet.slnx` and build using latest Visual Studio 2026 (we recommend using latest Visual Studio 2026 Insiders).

```bash
cd garnet
dotnet build -c Release
```

# Run our Test Suite

As a sanity check, you can run our test suite. The command to run tests in Release mode for .NET 9 target with verbose output to console is shown below (make sure you are in the root folder of the repo).

```bash
dotnet test -c Release -f net9.0 -l "console;verbosity=detailed"
```

:::tip
Tests that use Azure cloud storage are skipped, unless you set the environment variable `RunAzureTests` to `yes` and have Azurite running.
:::

# Deploy Garnet Server

Now, you are ready to deploy the Garnet server. This is simple, run the below:

```bash
cd main/GarnetServer
dotnet run -c Release -f net9.0
```

:::tip
By default, Garnet listens to TCP port `6379`, make sure to adjust your firewall settings when you need to access the server from remote machines.
Also, note that Garnet by default listens to the `IPAddress.Any` and `IPAddress.IPv6Any` endpoints.
Please make sure to adjust these based on your requirements.
:::

To see the configurable options and their defaults, run the below command. You can configure index size, memory size, page size, data file paths and checkpoint paths, IP address to bind to, port number to run on, etc.

```bash
dotnet run -c Release -f net9.0 -- --help
```

:::tip
For running the server with an index size of 512MB (instead of the default), run this:
```bash
dotnet run -c Release -f net9.0 -- -i 512m
```
:::

## Connect using a RESP Client

Garnet uses the RESP protocol, so you can use any Redis client in your favorite client language to talk to the Garnet server. For C# applications, you can either use StackExchange.Redis or our own C# client, called GarnetClient.

On Windows, You can use [RedisInsight](https://github.com/RedisInsight/RedisInsight) which has a graphical interface and a cli, or install [Memurai](https://www.memurai.com/)
(which offers Redis on Windows) and use their **memurai-cli** command line tool. You can also use **redis-cli** on WSL with the below configuration.

- Start GarnetServer in Windows, listening to `0.0.0.0` (`IPAddress.Any`). Use the argument `--bind 0.0.0.0` if necessary.
- Add `GarnetServer.exe` to firewall exceptions (Windows Defender Firewall -> Allow an app through Windows Firewall)
- On WSL, get the host IP address using `ip route show | grep -i default | awk '{ print $3}'`
- Connect from WSL using `redis-cli -h <address>`

If the above does not work to connect from WSL, you can try the [mirrored networking mode](https://learn.microsoft.com/en-us/windows/wsl/networking#mirrored-mode-networking) available in Windows 11 22H2 or later.

With any of these clients, just make sure to use the correct port (e.g., `6379`) when connecting to a Garnet server.
