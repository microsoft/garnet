---
id: scripting-commands
sidebar_label: Scripting and functions
title: Scripting and functions
slug: scripting
---

### EVAL

#### Syntax

```bash
    EVAL script numkeys [key [key ...]] [arg [arg ...]]
```

Invoke the execution of a server-side Lua script.

#### Resp Reply

The return value depends on the script that was executed.

---

### EVALSHA

#### Syntax

```bash
    EVALSHA sha1 numkeys [key [key ...]] [arg [arg ...]]
```

Evaluate a script from the server's cache by its SHA1 digest.

#### Resp Reply

The return value depends on the script that was executed.

---

### SCRIPT EXISTS

#### Syntax

```bash
    SCRIPT EXISTS sha1 [sha1 ...]
```

Returns information about the existence of the scripts in the script cache.

#### Resp Reply

Array reply: an array of integers that correspond to the specified SHA1 digest arguments.

---

### SCRIPT FLUSH

#### Syntax

```bash
    SCRIPT FLUSH [ASYNC | SYNC]
```

Flush the Lua scripts cache.

#### Resp Reply

Simple string reply: OK.

---

### SCRIPT LOAD

#### Syntax

```bash
    SCRIPT LOAD script
```

Load a script into the scripts cache, without executing it.

#### Resp Reply

Bulk string reply: the SHA1 digest of the script added into the script cache.

---
