---
id: releases
sidebar_label: Releases
title: Releases
---

## GitHub

Find releases at [https://github.com/microsoft/garnet/releases](https://github.com/microsoft/garnet/releases).

## NuGet

Find releases at [https://www.nuget.org/packages/Microsoft.Garnet](https://www.nuget.org/packages/Microsoft.Garnet). The NuGet
contains Garnet as a library for you to self-host in an application. This can be based on our GarnetServer
application code available [here](https://github.com/microsoft/garnet/blob/main/main/GarnetServer/Program.cs).
A minimal sample is shown below:

```cs
using Garnet;

try
{
    using var server = new GarnetServer(args);
    server.Start();
    Thread.Sleep(Timeout.Infinite);
}
catch (Exception ex)
{
    Console.WriteLine($"Unable to initialize server due to exception: {ex.Message}");
}
```

## Docker

We have very basic Dockerfiles available on GitHub so you can host them yourself. We would appreciate contributions 
to help make our Docker support more comprehensive. Official Docker builds are on our radar for the future.
* [Dockerfile](https://github.com/microsoft/garnet/blob/main/Dockerfile)
* [Dockerfile.ubuntu-x64](https://github.com/microsoft/garnet/blob/main/Dockerfile.ubuntu-x64)
* [Dockerfile.nanoserver-x64](https://github.com/microsoft/garnet/blob/main/Dockerfile.nanoserver-x64)

