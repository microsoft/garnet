---
id: processing
sidebar_label: Processing Layer
title: Processing Layer
---

# Processing Layer

Messages to Garnet server are processed once the networking layer reads the incoming message and passes it to the `RespServerSession` class. 

Within a session, the `TryConsumeMessages` method is responsible for processing the incoming messages by
* Measuring and tracking latency of processing along with bytes consumed and operations performed.
* Process messages further using the `ProcessMessages` method.
* Acts as the catch all for any exceptions occurring during message processing. This ensures that the server continues to run, even if processing of a message causes a critical unhandled exception and all resources are being freed up correctly.

The `ProcessMessages` method does a fast parsing of the incoming data to quickly identify the right command type. Based on the type of the command, further processing is performed by one of the below methods.

    * `ProcessBasicCommands`
    This includes commands like GET, SET, DEL, RENAME, EXISTS, EXPIRE, TTL, INCR, PING, MULTI, EXEC, QUIT, etc.

    * `ProcessArrayCommands`
    If it is not one of the basic commands above, further parsing is performed to check if the incoming data matches any of the array commands - commands related to multiple keys, commands that operate using Object store, etc. like MGET, MSET, KEYS, SCAN, ZADD, ZREM, LPUSH, HSET, etc.

    * `ProcessOtherCommands`
    This caters to processing rest of the commands including `ProcessAdminCommands` (for commands like AUTH, CONFIG, CLUSTER, etc), custom commands, custom transactions, etc.

This method also tracks session level metrics like the number of read, write and total commands that have been processed.
Transaction related operations are also performed here like calls to skip, start, stop and process commands within. See [transactions page](transactions.md) for more details.

Once the initial command is identified as described above, further processing is performed by command specific methods.

Next step of processing in a command specific manner is performed by methods named like Network[Command]. These methods are responsible for reading and parsing of command 
specific parameters from the incoming network buffer. Once the necessary arguments are parsed, they invoke the underlying storage layer method for performing the command operation. Then, they also write the response message back to the client in the right RESP format. For this, they use the buffer created by the network sender.
For example, see `NetworkIncrement` method in `BasicCommands.cs`.

The storage layer methods perform the actual processing logic of the command. They do utilize the underlying Tsavorite layer's APIs for performing operations like Read, RMW, Upset, Delete. Also, additional processing logic is implemented in the Tsavorite callbacks.
For example, see `Increment` method in `MainStoreOps.cs` and related methods in `MainStoreFunctions`.
