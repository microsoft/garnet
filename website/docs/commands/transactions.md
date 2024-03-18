---
id: transactions-commands
sidebar_label: Transactions
title: Transactions
slug: transactions
---

Details and examples about how RESP Transactions are implemented in Garnet are found at the [Developer Guide](../dev/transactions)

### DISCARD

#### Syntax

```bash
    DISCARD
```

Flushes all previously queued commands in a transaction and restores the connection state to normal.

#### Resp Reply

Simple string reply: OK.

---
### EXEC

#### Syntax

```bash
    EXEC
```

Executes all previously queued commands in a transaction and restores the connection state to normal.

#### Resp Reply

One of the following:

* Array reply: each element being the reply to each of the commands in the atomic transaction.
* Nil reply: the transaction was aborted because a WATCHed key was touched.

---
### MULTI

#### Syntax

```bash
    MULTI
```

Marks the start of a transaction block. Subsequent commands will be queued for atomic execution using EXEC.

#### Resp Reply

Simple string reply: OK.

---
### UNWATCH

#### Syntax

```bash
    UNWATCH
```

Flushes all the previously watched keys for a transaction.

#### Resp Reply

Simple string reply: OK.

---
### WATCH

#### Syntax

```bash
    WATCH key [key ...]
```

Marks the given keys to be watched for conditional execution of a transaction.

#### Resp Reply

Simple string reply: OK.

---
