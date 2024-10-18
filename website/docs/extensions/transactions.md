---
id: transactions
sidebar_label: Transactions
title: Transactions
---

# Server-Side Transactions

Custom transactions allows adding a new transaction and registering it with Garnet. This registered transaction can then be invoked from any Garnet client to perform a transaction on the Garnet server.

### Developing custom server side transactions

`CustomTransactionProcedure` is the base class for all custom transactions. To develop a new one, this class has to be extended and then include the custom logic. There are three methods to be implemented in a new custom transaction:

- `Prepare<TGarnetReadApi>(TGarnetReadApi api, ArgSlice input)`
- `Main<TGarnetApi>(TGarnetApi api, ArgSlice input, ref MemoryResult<byte> output)`
- `Finalize<TGarnetApi>(TGarnetApi api, ArgSlice input, ref MemoryResult<byte> output)`

The `Prepare` method implementation must setup the keys that will be involved in the transaction using utility methods available described below. The `Main` method is where the actual operation is to be performed as the locks required for the keys setup in the `Prepare` method are already obtained. The `Main` method then generates the output of the transaction as well. After the unlock of keys comes the `Finalize` phase, which can contain any non-transactional read and write operations on the store, and can write output as well. `Finalize` allows users to author complex non-transactional scripts as well: `Prepare` should simply return false, while `Main` is left unimplemented.

These are the helper methods for developing custom transactions.
- `AddKey(ArgSlice key, LockType type, bool isObject)` This method is used to add a specified key to the locking set. It takes three parameters: key (the key to be added), type (the type of lock to be applied), and isObject (a boolean value indicating whether the key represents an object).
- `RewindScratchBuffer(ref ArgSlice slice)` This method is responsible for rewinding (popping) the last entry of the scratch buffer if it contains the given ArgSlice. It takes a reference to an ArgSlice parameter and returns a boolean value indicating whether the rewind operation was successful.
- `CreateArgSlice(ReadOnlySpan<byte> bytes)` This method is used to create an ArgSlice in the scratch buffer from a given ReadOnlySpan\<byte>. It takes a ReadOnlySpan\<byte> parameter representing the argument and returns an ArgSlice object.
- `CreateArgSlice(string str)` This method is similar to the previous one, but it creates an ArgSlice in UTF8 format from a given string. It takes a string parameter and returns an ArgSlice object.
- `GetNextArg(ArgSlice input, ref int offset)` This method is used to retrieve the next argument from the input at the specified offset. It takes an ArgSlice parameter representing the input and a reference to an int offset. It returns an ArgSlice object representing the argument as a span. The method internally reads a pointer with a length header to extract the argument.
These member functions provide utility and convenience methods for manipulating and working with the transaction data, scratch buffer, and input arguments within the CustomTransactionProcedure class.

**NOTE** When invoking APIs on `IGarnetApi` multiple times with large outputs, it is possible to exhaust the internal buffer capacity. If such usage scenarios are expected, the buffer could be reset as described below.
* Retrieve the initial buffer offset using `IGarnetApi.GetScratchBufferOffset`
* Invoke necessary apis on `IGarnetApi`
* Reset the buffer back to where it was using `IGarnetApi.ResetScratchBuffer(offset)`

Registering the custom transaction is done on the server-side by calling the `NewTransactionProc(string name, int numParams, Func<CustomTransactionProcedure> proc)` method on the Garnet server object's `RegisterAPI` object with its name, number of parameters and a method that returns an instance of the custom transaction class.\
It is possible to register the custom transaction from the client-side as well (as an admin command, given that the code already resides on the server) by using the `REGISTERCS` command (see [Custom Commands](../dev/custom-commands.md)). 

### Execution

Custom transactions are executed by the `RunTransactionProc` method in the `TransactionManager` class. This method could be invoked either by calling the `RUNTXP` command with the custom transaction details or by using the custom transaction name used when registering.

The initial phase is performed by invoking the `Prepare` method of the custom transaction which adds the keys that need to be locked using the `AddKey` method. If the `Prepare` method fails by returning **false**, the transaction manager's `Reset(false)` is called to reset itself. Otherwise, it proceeds to the next step of invoking the `Run` method of the `TransactionManager` class. See the description above for this method. If the `Run` method fails, the transaction manager is reset too.

Next step, it proceeds to invoking the custom transaction's `Main` method implementation which performs the core logic of the transaction. On successful completion, it proceeds to log the transaction information. On the other hand, if the `Main` method fails by throwing an exception, `Reset(true)` is called to unlock any locked keys and reset itself.

The `Finalize` phase is invoked at the end regardless of the success or failure of the actual transaction. As mentioned earlier, it can contain non-transactional logic over the store, and can generate output.