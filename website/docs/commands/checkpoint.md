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

The reply is sent before the checkpoint runs, so it does not report whether the checkpoint succeeded. A background
save that fails leaves `LASTSAVE` unchanged and sets `rdb_last_bgsave_status` to `err` in the `PERSISTENCE` section of
[INFO](server.md#info), with details in the server log.

#### Resp Reply

One of the following:

* Simple string reply: Background saving started.
* Simple string reply: Background saving scheduled.
* Error reply: checkpoint already in progress.

---

### SAVE

#### Syntax

```bash
SAVE [DBID]
```

The SAVE commands performs a synchronous save of the dataset producing a point in time snapshot of all the data inside the Garnet instance. If a DB ID is specified, only the data inside of that database will be snapshotted.

#### Resp Reply

One of the following:

* Simple string reply: OK.
* Error reply, if the checkpoint did not complete. Nothing was written, so `LASTSAVE` is left unchanged.

---
### LASTSAVE
#### Syntax

```bash
LASTSAVE [DBID]
```

Return the UNIX TIME of the last DB save executed with success for the current database or, if a DB ID is specified, the last DB save executed with success for the specified database.

Only a checkpoint that completed advances this timestamp, so it can be polled after `BGSAVE` to confirm that the data
was actually persisted.

#### Resp Reply

Integer reply: UNIX TIME of the last DB save executed with success.

---