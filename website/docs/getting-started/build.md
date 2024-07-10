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

Make sure .NET 8 is installed, following instructions [here](https://dotnet.microsoft.com/en-us/download). You can use either Linux or Windows; Garnet works equally well on both platforms.

Go to the root folder of the repo and build using dotnet, or open Garnet.sln and build using Visual Studio 2022 (we recommend the Preview version for the latest features). Make sure Visual Studio is up to date by checking for updates.

```bash
cd garnet
dotnet restore
dotnet build -c Release
```

# Run our Test Suite

As a sanity check, you can run our test suite. The command to run tests in Release mode for .NET 8 with verbose output to console is shown below (make sure you are in the root folder of the repo).

```bash
dotnet test -c Release -f net8.0 -l "console;verbosity=detailed"
```

:::tip
Tests that use Azure cloud storage are skipped, unless you set the environment variable `RunAzureTests` to `yes` and have Azurite running.
:::

# Deploy Garnet Server

Now, you are ready to deploy the Garnet server. This is simple, run the below::

```bash
cd main/GarnetServer
dotnet run -c Release -f net8.0
```

:::tip
By default, Garnet listens to TCP port 6379, make sure to adjust your firewall settings when you need to access the server from remote machines.
:::

To see the configurable options and their defaults, run the below command. You can configure index size, memory size, page size, data file paths and checkpoint paths, IP address to bind to, port number to run on, etc.

```bash
dotnet run -c Release -f net8.0 -- --help
```

:::tip
For running the server with an index size of 512MB (instead of the default), run this:
```bash
dotnet run -c Release -f net8.0 -- -i 512m
```
:::

## Connect using a RESP Client

Garnet uses the RESP protocol, so you can use any Redis client in your favorite client language to talk to the Garnet server. For C# applications, you can either use StackExchange.Redis or our own C# client, called GarnetClient.

On Windows, you can either install **redis-cli** on WSL (Linux), <a href="https://github.com/RedisInsight/RedisInsight" target="_blank">RedisInsight</a> which has a  graphical interface, or install 
<a href="https://www.memurai.com/" target="_blank">Memurai</a> (which offers Redis on Windows) and use their **memurai-cli** command line tool.

With any of these clients, just make sure to use the correct port (e.g., 6379) when connecting to a Garnet server.
