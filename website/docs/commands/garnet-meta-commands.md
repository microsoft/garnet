---
id: garnet-meta-commands
sidebar_label: Garnet Meta-Commands
title: Garnet Meta-Commands
slug: garnet-meta-commands
---

# Garnet Meta-Commands

Here we describe Garnet-specific meta-commands that may envelop any RESP command to support cases like augmented outputs, conditional execution etc.

## ETag-related Meta-Commands

Listed below are all the Garnet eTag-related meta-commands. These commands can envelop any single-key data command.

---

### EXECIFGREATER

#### Syntax

```bash
    EXECIFGREATER etag command [arg [arg ...]]
```

Execute the specified command if the eTag specified is greater than the current eTag of the record stored at the key it operates on.

#### Resp Reply

* Array reply: An array of size 2 containing the response to the command (or null if command was not executed) followed by the eTag of the key after the operation is completed.
    * If the record stored at key has no eTag, an eTag of zero is returned.
    * The meta-command syntax supports only single-key data commands (with the exception of `DEL` and `EXISTS` only if executed with a single key)
    * If command specified is `SET`, the first value is null if the command executed or the current value at key if command was skipped
    * If command specified is `DEL`, the eTag value returned is that of the record prior to its deletion.

---

### EXECIFMATCH

#### Syntax

```bash
    EXECIFMATCH etag command [arg [arg ...]]
```

Execute the specified command if the eTag specified matches the current eTag of the record stored at the key it operates on.

#### Resp Reply

* Array reply: An array of size 2 containing the response to the command (or null if command was not executed) followed by the eTag of the key after the operation is completed.
    * If the record stored at key has no eTag, an eTag of zero is returned.
    * The meta-command syntax supports only single-key data commands (with the exception of `DEL` and `EXISTS` only if executed with a single key)
    * If command specified is `SET`, the first value is null if the command executed or the current value at key if command was skipped
    * If command specified is `DEL`, the eTag value returned is that of the record prior to its deletion.

---

### EXECIFNOTMATCH

#### Syntax

```bash
    EXECIFNOTMATCH etag command [arg [arg ...]]
```

Execute the specified command if the eTag specified does not match the current eTag of the record stored at the key it operates on.

#### Resp Reply

* Array reply: An array of size 2 containing the response to the command (or null if command was not executed) followed by the eTag of the key after the operation is completed.
    * If the record stored at key has no eTag, an eTag of zero is returned.
    * The meta-command syntax supports only single-key data commands (with the exception of `DEL` and `EXISTS` only if executed with a single key)
    * If command specified is `SET`, the first value is null if the command executed or the current value at key if command was skipped
    * If command specified is `DEL`, the eTag value returned is that of the record prior to its deletion.

---

### EXECWITHETAG

#### Syntax

```bash
    EXECWITHETAG command [arg [arg ...]]
```

Execute the specified command and append the eTag of record stored at the key it operates on into the output.

#### Resp Reply

* Array reply: An array of size 2 containing the response to the command followed by the eTag of the record stored at key after the operation is completed.
    * If the record stored at key has no eTag, an eTag of zero is returned.
    * The meta-command syntax supports only single-key data commands (with the exception of `DEL` and `EXISTS` only if executed with a single key)
    * If command specified is `DEL`, the eTag value returned is that of the record prior to its deletion.

---