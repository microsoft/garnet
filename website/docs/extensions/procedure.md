---
id: procedure
sidebar_label: Procedures
title: Procedures
---

# Server-Side Procedures

Custom procedures allow adding a new non-transactional procedure and registering it with Garnet. This registered procedure can then be invoked from any Garnet client to perform a multi-command non-transactional operation on the Garnet server.

### Developing custom server side procedures

`CustomProcedure` is the base class for all custom procedures. To develop a new one, this class has to be extended and then include the custom logic. There is one method to be implemented in a new custom procedure:

- `Execute<TGarnetApi>(TGarnetApi garnetApi, ArgSlice input, ref MemoryResult<byte> output)`

The `Execute` method has the core logic of the custom procedure. Its implementation could process input passed in through the (`input`) parameter and perform operations on Garnet by invoking any of the APIs available on `IGarnetApi`. This method then generates the output of the procedure as well.

These are the helper methods for developing custom procedures same as that of custom transactions detailed [here](transactions.md#developing-custom-server-side-transactions).

Registering the custom procedure is done on the server-side by calling the 

`NewProcedure(string name, CustomProcedure customProcedure, RespCommandsInfo commandInfo = null)` 

method on the Garnet server object's `RegisterAPI` object with its name, an instance of the custom procedure class and optional commandInfo.

**NOTE** When invoking APIs on `IGarnetApi` multiple times with large outputs, it is possible to exhaust the internal buffer capacity. If such usage scenarios are expected, the buffer could be reset as described below.
* Retrieve the initial buffer offset using `IGarnetApi.GetScratchBufferOffset`
* Invoke necessary apis on `IGarnetApi`
* Reset the buffer back to where it was using `IGarnetApi.ResetScratchBuffer(offset)`

:::tip 
As a reference of an implementation of a custom procedure, see the example in GarnetServer\Extensions\Sum.cs.
