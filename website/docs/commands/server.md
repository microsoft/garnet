---
id: server-commands
sidebar_label: Server Management
title: Server Management
slug: server
---


### COMMAND
#### Syntax

```bash
COMMAND
```

Return an array with details about every Garnet command.

#### Resp Reply

Array reply: a nested list of command details.

---
### COMMAND COUNT
#### Syntax

```bash
COMMAND COUNT
```

Returns Integer reply of number of total commands in this Garnet server.

#### Resp Reply

Integer reply: the number of commands returned by COMMAND.

---
### COMMAND INFO
#### Syntax

```bash
COMMAND
```

Return an array with details about every Garnet command.

#### Resp Reply

Array reply: a nested list of command details. The order of the commands in the array is random.

---
### COMMITAOF
#### Syntax

```bash
COMMITAOF
```

This command manually issues a commit to write ahead logging (append-only file)

#### Resp Reply

Simple string reply: AOF file committed

---
### CONFIG GET
#### Syntax

```bash
CONFIG GET parameter [parameter ...]
```

The CONFIG GET command is used to read the configuration parameters of a running Garnet server.

#### Resp Reply

Array reply: a list of configuration parameters matching the provided arguments.

---
### CONFIG SET
#### Syntax

```bash
CONFIG SET parameter value [parameter value ...]
```

The CONFIG SET command is used in order to reconfigure the server at run time without the need to restart Garnet.

#### Resp Reply

Simple string reply: OK when the configuration was set properly. Otherwise an error is returned.

---
### DBSIZE
#### Syntax

```bash
DBSIZE
```

Return the number of keys in the currently-selected database.

#### Resp Reply

Integer reply: the number of keys in the currently-selected database.

---
### FLUSHDB
#### Syntax

```bash
FLUSHDB [ASYNC | SYNC]
```

Delete all the keys of the currently selected DB. This command never fails.

#### Resp Reply

Simple string reply: OK.

---
### LATENCY HELP
#### Syntax

```bash
LATENCY HELP
```

Returns all the supported LATENCY sub-commands

#### Resp Reply

Array reply: a list of LATENCY supported sub-command details.

---
### LATENCY HISTOGRAM
#### Syntax

```bash
LATENCY HISTOGRAM [event [event ...]]
```

Return latency histogram of or more ```<event>``` classes. \
If no commands are specified then all histograms are replied

#### Resp Reply

Array reply

---
### LATENCY RESET
#### Syntax

```bash
LATENCY RESET [event [event ...]]
```

Reset latency data of one or more ```<event>``` (default: reset all data for all event classes).

#### Resp Reply

Simple string reply: OK.

---
### MEMORY USAGE
#### Syntax

```bash
MEMORY USAGE key [SAMPLES count]
```

The MEMORY USAGE command reports the number of bytes that a key and its value require to be stored in RAM.

#### Resp Reply

One of the following:
* Integer reply: the memory usage in bytes.
* Null reply: if the key does not exist.

---
### REPLICAOF

#### Syntax

```bash
REPLICAOF <host port | NO ONE>
```

The REPLICAOF command can change the replication settings of a replica on the fly.

#### Resp Reply

Simple string reply: OK.

---

### TIME
#### Syntax

```bash
TIME
```

The TIME command returns the current server time as a two items lists: a Unix timestamp and the amount of microseconds already elapsed in the current second. Basically the interface is very similar to the one of the gettimeofday system call.

#### Resp Reply

Array reply: specifically, a two-element array consisting of the Unix timestamp in seconds and the microseconds' count.

---