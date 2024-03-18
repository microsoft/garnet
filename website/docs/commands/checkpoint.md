---
id: checkpoint-commands
sidebar_label: Checkpoint
title: Checkpoint API
slug: checkpoint
---

### BGSAVE
#### Syntax

```bash
BGSAVE [SCHEDULE]
```

Save the DB in background.

#### Resp Reply

One of the following:

* Simple string reply: Background saving started.
* Simple string reply: Background saving scheduled.

---

### SAVE

#### Syntax

```bash
SAVE
```

The SAVE commands performs a synchronous save of the dataset producing a point in time snapshot of all the data inside the Garnet instance.

#### Resp Reply

Simple string reply: OK.

---
### LASTSAVE
#### Syntax

```bash
LASTSAVE
```

Return the UNIX TIME of the last DB save executed with success.

#### Resp Reply

Integer reply: UNIX TIME of the last DB save executed with success.

---