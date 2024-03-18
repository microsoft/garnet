---
id: transactions
sidebar_label: Transactions
title: Transactions
---

# Transactions

Garnet supports two types of transactions:
1. Custom Server-side Transactions
2. Client-Issued Transactions (Redis)

## Custom Server-side Transactions

Custom transactions allows adding a new transaction and registering it with Garnet on the server side. This registered transaction can then be invoked from any Garnet client to perform a transaction on the Garnet server.
Read more on developing custom server side transactions in the [Transactions page](../extensions/transactions.md) under the Extensions section.

## Client-Issued Transactions (Redis)

You can read more here: [Redis Transactions](https://redis.io/docs/manual/transactions). In this design, transaction operations come in a MULTI/EXEC scope. Every operation in this scope is part of the transaction.
The model does not allow you to use the result of reads inside the MULTI/EXEC scope but allows you to read and monitor keys before (i.e., watch), and if they are unchanged at the time of execution, the transaction will commit.

### Example

```
WATCH mykey
val = GET mykey
val = val + 1 # not Redis command this happens outside
MULTI
SET mykey $val
EXEC
```
In the above example, if **mykey** changes before **EXEC** command, the transaction will abort since the calculation of *val* is invalidated.  

### Transaction Backend

Transactions in Garnet are implemented using the following classes:
- `TransactionManager`
- `WatchVersionMap`
- `WatchedKeyContainer`
- `RespCommandsInfo`

### TransactionManager Class Responsibilities

#### Storing the state of Transaction:

- **Started**: Goes to this state after `MULTI` command, TxnManager will queue any command in this state except EXEC
- **Running**: Goes to this state after EXEC, TxnManager will run the queued commands in this state
- **Aborted**: Goes to this state in case of anything bad happens

#### Queueing Commands:

When TxnManager goes to *Started* state, it will (1) queue any command afterward and (2) save any key that is used in those commands to lock at the execution time using 2PL.
In order to queue commands, they are **let to live in the network buffer**. Using the `TrySkip` function in `RespServerSession`. To lock the keys at the time of execution, we save pointers to the actual memory location of keys in the network buffer using an array of `TxnKeyEntry` that has an `ArgSlice` and the lock type (Shared or Exclusive).

`TrySkip` function uses `RespCommandsInfo` class to skip the correct number of tokens and detects syntax errors. `RespCommandsinfo` stores the number of `Arity` or arguments of each command. E.g., the `GET` command's arity is two. The command token `GET` and one key. We store the minimum number of arguments with a negative value for the commands that can have multiple arguments. `SET` command's arity is  -3 means that it requires at least three arguments (including command toke).

During the `TrySkip` we call `TransactionManager.GetKeys`, which goes over the arguments and stores `TxnKeyEntry` for each key in the arguments.

#### Execution

When the the `TxnState` is *Started* and we encounter the `EXEC` we call `TransactionManager.Run()`. What this functions does:
1. first acquires the `LockableContext` for the main store and/or object store based on the store type.
2. Goes over `TxnKeyEntry`s and locks all the needed keys.
3. Calls `WatchedKeyContainer.ValidateWatchVersion()`
    - It goes over all the watched keys and checks whether their version is the same as the time watch or not
    - if it passes, we proceed with execution otherwise, we call `TransactionManager.Reset(true)` to reset the transaction manager. The `true` argument we pass to `Reset` says that it also needs to unlock the keys.
4. It writes the transaction start indicator in the AOF to recover atomically in case of failure in the middle of the transaction

After that, the TxnState is set to *Running* and the network `readHead` is set to the first command after `MULTI`, and this time we start actually running those commands. When the execution reaches to EXEC again, and we are in *Running* state, it calls `TransactionManager.Commit()`. What it does:

- Unlock all the keys that we locked in `Run`
- Reset `TransactionManager` and `WatchedKeyContainer`
- It also appends the commit message to the AOF

### Recovery Optimization

Garnet does regular checkpoints and changes its version between those checkpoints. In order to get checkpoint consistency, we require transaction operations to have the same version or in other words be in the same checkpoint window.

To enforce this right now, we do the following:

- When TsavoriteStateMachine is in `Prepare` phase, we do not let a transaction start execution to let checkpoint finish
- If there is a running transaction and TsavoriteStateMachine moves to `Prepare` we don't let version change happen until the transaction finishes the execution.
- These two happen using `session.IsInPreparePhase` and two while loop at the beginning of `Run` function

###  Watch Command

It is used to implement optimistic locking.
- Provide a check-and-set (CAS) behavior to transactions.
- Keys are monitored in order to detect changes against them.
- If at least one watched key is modified before the EXEC  command, the whole transaction aborts
- It is implemented through a `Modified` bit in `TsavoriteKV` and a **`VersionMap`** in `Garnet`

#### Version Map

It Monitors modifications on the keys. Every time a watched key gets modified, we increment its version in the version map.
- It has been implemented through a `Hash Index`
- To prevent the overhead for normal operations in the critical path we only increment the version in some cases:
    - For in-memory records, we only increment version **watched keys**. The keys that are watched in Garnet use the `Modified` bit in Tsavorite to track modification (more on Modified bit Below)
    - For records in the disk, we increment the version for **copy-update** RMWs and Upserts. **We intentionally accept this overhead because copy updates are less often, and the overhead is not crucial.**

    - Increment the version in `MainStoreFunctions` and `ObjectStoreFunctions`:
        - `InPlaceUpdater` if it is watched
        - `ConcurrentWriter` if it is watched
        - `ConcurrentDeleter` if it is watched
        - `PostSingleWriter`
        - `PostInitialUpdater`
        - `PostCopyUpdater`
        - `PostSingleDeleter`

#### Modified Bit

The Modified bit tracks modifications in records in Tsavorite. The modified bit for each record gets set to "1" when they get modified and **Remains** "1" until somebody Reset it to zero using the `ResetModified` API.

#### Watch

-   We add a `ClientSesssion.ResetModified(ref Key key)` API.
    -   CAS the `RecordInfo` word into the same word, but with the **modified bit reset**.
- When somebody watches a key in Garnet, we call `ResetModified` API and store that key in `WatchedKeyContainer`.
- At the time of watch, we read a version of that record from the version map and store it alongside the key in `WatchedKeyContainer`.
- At the time of Transaction Execution, we go through all keys in `WatchedKeyContainer` and if their version is still the same, we proceed with the transactions

#### Unwatch

- When a record gets modified in Tsavorite the modified bit gets set automatically
- When a user calls `Unwatch` API in Garnet we simply just reset the `WatchedKeyContainer`
- After every `DISCARD`, `EXEC`, `UNWATCH` command we unwatch everything

### Testing

We have written a micro-benchmark `TxnPerfBench` to test client transactions. The benchmark contains four different workloads:
 - READ_TXN
 - WRITE_TXN
 - READ_WRITE_TXN
 - WATCH_TXN

 It looks like the online benchmark, and can have different percentages of different workloads:
 ```
 dotnet run -c Release -t 2 -b 1 --dbsize 1024 -x --client SERedis --op-workload WATCH_TXN --op-percent 100
 dotnet run -c Release -t 2 -b 1 --dbsize 1024 -x --client SERedis --op-workload READ_TXN,WRITE_TXN --op-percent 50,50
 dotnet run -c Release -t 2 -b 1 --dbsize 1024 -x --client SERedis --op-workload READ_WRITE_TXN --op-percent 100
 ```

Before running the benchmark, we load data with `opts.DbSize` number of records. It also accepts the number of reads and writes per transaction:

`TxnPerfBench(..., int readPerTxn = 4, int writePerTxn = 4)`

- We only support batch size of one. 
- We only support the SE.Redis client for now.

#### READ_TXN

Runs a transaction with `readPerTxn` number of `GET` requests;

#### WRITE_TXN

Runs a transaction with `writePerTxn` number of `SET` requests;

#### READ_WRITE_TXN

Runs a mix of `SET` and `GET` request (`readPerTxn`, `writePerTxn`)

#### WATCH_TXN

This workload watches `readPerTxn` number of keys. Then starts a transaction, reads the watched keys, and writes to `writePerTxn` number of keys.

```
readPerTxn = 2
writePerTxn = 2

WATCH x1
WATCH x2
MULTI
GET x1
GET x2
SET x3 v3
SET x4 v4
EXEC
```
