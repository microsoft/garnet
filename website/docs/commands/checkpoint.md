---
id: checkpoint-commands
sidebar_label: Checkpoint
title: Checkpoint API
slug: checkpoint
---

### BGSAVE
#### Syntax

```bash
BGSAVE [SCHEDULE] [DBID]
```

Save all databases inside the Garnet instance in the background. If a DB ID is specified, save save only that specific database.

#### Resp Reply

One of the following:

* Simple string reply: Background saving started.
* Simple string reply: Background saving scheduled.

---

### SAVE

#### Syntax

```bash
SAVE [DBID]
```

The SAVE commands performs a synchronous save of the dataset producing a point in time snapshot of all the data inside the Garnet instance. If a DB ID is specified, only the data inside of that database will be snapshotted.

#### Resp Reply

Simple string reply: OK.

---
### LASTSAVE
#### Syntax

```bash
LASTSAVE [DBID]
```

Return the UNIX TIME of the last DB save executed with success for the current database or, if a DB ID is specified, the last DB save executed with success for the specified database.

#### Resp Reply

Integer reply: UNIX TIME of the last DB save executed with success.

---