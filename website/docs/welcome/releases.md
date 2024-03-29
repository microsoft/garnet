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

On Linux

```console
docker run --network=host --ulimit memlock=-1 ghcr.io/microsoft/garnet
```

On MacOS

```console
docker run -p 6379:6379 --ulimit memlock=-1 ghcr.io/microsoft/garnet
```
You can then use `redis-cli` to connect to `127.0.0.1:6379`.

```console
redis-cli
127.0.0.1:6379> set key value
OK
127.0.0.1:6379> get key
"value"
127.0.0.1:6379>
```
Dockerfile links:
* [Dockerfile](https://github.com/microsoft/garnet/blob/main/Dockerfile)
* [Dockerfile.ubuntu-x64](https://github.com/microsoft/garnet/blob/main/Dockerfile.ubuntu-x64)
* [Dockerfile.nanoserver-x64](https://github.com/microsoft/garnet/blob/main/Dockerfile.nanoserver-x64)

## Docker Compose

Make sure you have installed [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/).

### Download Garnet Docker Compose File

```console
wget https://raw.githubusercontent.com/microsoft/garnet/main/docker-compose.yml
```

### Launch Garnet

```console
docker compose up -d
```

### Confirm image is up

```console
docker ps | grep garnet
# 249b468dcda1   ghcr.io/microsoft/garnet   "/app/GarnetServer -…"   21 seconds ago   Up 20 seconds   0.0.0.0:6379->6379/tcp, :::6379->6379/tcp   garnet-garnet-1
```

### Log follow

```console
docker logs -f garnet-garnet-1
```

### Connect clients

As before, you can then use `redis-cli` or any client library in your application to connect to `127.0.0.1:6379`.

```console
redis-cli
127.0.0.1:6379> set key value
OK
127.0.0.1:6379> get key
"value"
127.0.0.1:6379>
```
