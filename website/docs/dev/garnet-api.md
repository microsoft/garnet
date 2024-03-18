---
id: garnet-api
sidebar_label: Garnet API
title: Garnet API
---

The IGarnetAPI interface contains the operators exposed to the public API, which ultimately perform operations over the keys stored in Garnet. It inherits from IGarnetReadApi and IGarnetAdvancedApi. 

For adding an new operator or command to the API, you should add a new method to the IGarnetApi, if this is a write operation, or to the IGarnetReadApi in the case of a read one.

### Adding a new command 

A new command is implemented with the following methods: (See ZADD for further details)

* A private unsafe bool Network[CommandName] in the internal class RespServerSession.

**Example**

```csharp 
private unsafe bool NetworkZADD<TObjectContext>(int count, byte* ptr, ref TObjectContext objectStoreContext)
```

This method contains the operations that read and parse the message from the network buffer and write the resp formatted response from the command, back to the network buffer.

* An internal method Try[CommandName] in the class StorageSession.

**Example**

```csharp
internal GarnetStatus Try[CommandName]<TObjectContext>(ref byte[] key, ref SpanByte input, ref FasterObjectOutput output, ref TObjectContext objectStoreContext)
```

This method is part of the Storage layer. This storage API ONLY performs the RMW or Read operation calls, and it wraps over the Tsavorite API. All the current implemented commands can be found under the folder *Storage*, inside the Garnet.Server project (folder */libs/server/Storage/Session/)*.

* An internal method Try[CommandName] using the ArgSlice type.

```csharp 
internal GarnetStatus Try[CommandName]<TObjectContext>(ArgSlice key, ArgSlice score, ArgSlice member, out int result, ref TObjectContext objectStoreContext)
```

This method is optional. Notice this ArgSlice type is used instead of byte[], also this method does not read any value from the network buffer and it does not rely on the RESP protocol. It is the way to extended or add functionality to the API, that can be used in server procedures that use csharp code. 



