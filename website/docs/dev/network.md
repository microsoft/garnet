---
id: network
sidebar_label: Network Layer
title: Network Layer
---

# Network Layer Design

## Overview
This document describes how the network layer of Garnet server is designed and the various classes it uses to perform network operations to communicate with the client.

On initialization, GarnetServer instantiates GarnetServerTcp object to handle incoming network connections. 
Its `Start` method, invoked when the GarnetServer starts, binds to the IP address and port as specified in the config (See [Configuration page](../getting-started/configuration.md)) and begins accepting new connections from clients. It also registers `AcceptEventArg_Completed` as the callback function to be invoked on receiving a new connection.

The `AcceptEventArg_Completed` method that is called when a new connection is accepted, handles the new connection by creating a new `ServerTcpNetworkHandler` object and adding it to the activeHandlers dictionary using the `HandleNewConnection` method. 

## NetworkHandler
The `ServerTcpNetworkHandler` performs the following steps to receive and process the incoming data.
* On instantiation, allocates the network buffer memory to receive data. 
* Network buffer is allocated using pool of memory governed by the `LimitedFixedBufferPool` class. See [LimitedFixedBufferPool](#network-memory-pool-using-limitedfixedbufferpool) for details. 
* The allocated network buffer is configured to receive data directly from the network.
* It (through base class TcpNetworkHandlerBase) registers `RecvEventArg_Completed` as the callback method to receive data from the socket. 
* Also, validates SSL connection by performing client server authentication using `SslStream.AuthenticateAsServerAsync` method, depending on whether TLS encryption is enabled.

The base class `NetworkHandler` provides functionality for handling network communication, including sending and receiving data over a network connection.
The main parts of the class:
* The class' constructor initializes various fields and properties, including the server hook, network sender, network buffer pool, TLS-related fields, and logger. It also sets up the network and transport receive buffers based on whether TLS is enabled or not.
* The `Start` and `StartAsync` methods are used to begin the network handler, including the authentication phase if TLS is enabled. These methods take optional parameters such as TLS options and remote endpoint name. They internally call the AuthenticateAsServerAsync or AuthenticateAsClientAsync methods to perform authentication.
* The `AuthenticateAsServerAsync` and `AuthenticateAsClientAsync` methods handle authentication process for TLS connections. They use the sslStream object to perform the authentication and establish a secure connection. These methods also handle reading any extra bytes left over after authentication.
* The `OnNetworkReceive` method is called when data is received from the network. This method is responsible for processing the received data. It first checks the status of the TLS reader and performs the necessary transformations on the network and transport receive buffers. Then, it calls the `Process` method to handle the received data.
* The `Process` method is responsible for processing the received data. It checks if there is any data in the transport receive buffer and if there is a message consumer available. If so, it tries to process the request by calling the `TryProcessRequest` method. The message consumer is responsible for retrieving the session provider based on the wire protocol - ASCII is the only format supported currently. The session provider is previously registered when the Garnet server is initialized. The retrieved session provider is then used to create a new session object, if one doesn't exist already. For processing messages, the `GarnetProvider` class is registered as the session provider and is used to create objects of `RespServerSession` class to handle RESP messages.
* The class also performs other operations for buffer management, shifting buffers, logging security information, and disposing of resources.

## GarnetTcpNetworkSender
The GarnetTcpNetworkSender class is a TCP network sender that inherits from the `NetworkSenderBase` class. It is responsible for sending network data over a TCP connection for response messages back to the client. This is instantiated as part of creation of the `ServerTcpNetworkHandler` object.
* It uses the socket, previously created to accept the connection, to send response back to the client.
* The class uses a stack of `GarnetSaeaBuffer` objects to manage reusable send buffers. These buffers are created using a networkPool field of type [`LimitedFixedBufferPool`](#network-memory-pool-using-limitedfixedbufferpool). These buffers are used to store the data that is to be sent over the network.
* The class uses throttling to limit the number of concurrent sends. It has a throttleCount field that keeps track of the number of ongoing sends and a throttle semaphore that controls the maximum number of concurrent sends.

## Network memory pool using LimitedFixedBufferPool
The `LimitedFixedBufferPool` class is a memory pool implementation that provides a pool of memory segments of varying sizes. It is designed to efficiently manage memory allocations and deallocations for improved performance in scenarios where frequent memory allocation and deallocation operations are required. It does this using an array of concurrent queues. Each concurrent queue represents a memory segment of a specific size range. The class provides methods to allocate and deallocate memory segments from the pool.
* The `LimitedFixedBufferPool` class is designed to be thread-safe and can be used in multi-threaded scenarios.
* The pool internally uses concurrent queues to manage memory segments, ensuring efficient allocation and deallocation operations.
* The pool supports memory segments of sizes that are powers of two and greater than or equal to the minimum allocation size.
* The `LimitedFixedBufferPool` class is disposable and should be disposed when no longer needed to release allocated memory segments.

The `Get` method allocates a memory segment from the pool for the requested size. It returns a [`PoolEntry`](#poolentry-class) object representing the allocated memory segment.

The `Return` method returns a memory segment to the pool for reuse.

### PoolEntry Class
The LimitedFixedBufferPool class internally uses the PoolEntry class to represent memory segments. The PoolEntry class provides methods to reuse and manage the allocated memory segments.