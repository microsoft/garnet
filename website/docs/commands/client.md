---
id: client-commands
sidebar_label: Client Management
title: Client Management
slug: client
---

### CLIENT ID

#### Syntax

```bash
    CLIENT ID
```

The command just returns the ID of the current connection. Every connection ID has certain guarantees:

1. It is never repeated, so if CLIENT ID returns the same number, the caller can be sure that the underlying client did not disconnect and reconnect the connection, but it is still the same connection.
1. The ID is monotonically incremental. If the ID of a connection is greater than the ID of another connection, it is guaranteed that the second connection was established with the server at a later time.

#### Resp Reply

Integer reply: the ID of the client.

---

### CLIENT INFO

#### Syntax

```bash
    CLIENT INFO
```

The command returns information and statistics about the current client connection in a mostly human readable format.

#### Resp Reply

Bulk string reply: a unique string for the current client, as described at the CLIENT LIST page.

---

### CLIENT GETNAME

#### Syntax

```bash
    CLIENT GETNAME
```

The command returns the name of the current connection as set by CLIENT SETNAME.

#### Resp Reply

Bulk string reply: the name of the client, or an empty string if no name is set.

---

### CLIENT SETNAME

#### Syntax

```bash
    CLIENT SETNAME <name>
```

The command sets the name of the current connection.

#### Resp Reply

Simple string reply: OK if the connection name was successfully set.

---

### CLIENT SETINFO

#### Syntax

```bash
    CLIENT SETINFO <LIB-NAME libname | LIB-VER libver>
```

The command sets the value of a specific section of the current client connection.

Currently the supported attributes are:

- LIB-NAME - meant to hold the name of the client library that's in use.
- LIB-VER - meant to hold the client library's version.

#### Resp Reply

Simple string reply: OK if the section value was successfully set.

---

### CLIENT KILL

#### Syntax

```bash
    CLIENT KILL <ip:port | <[ID client-id] | [TYPE <NORMAL | MASTER |
        SLAVE | REPLICA | PUBSUB>] | [USER username] | [ADDR ip:port] |
        [LADDR ip:port] | [SKIPME <YES | NO>] | [MAXAGE maxage]
        [[ID client-id] | [TYPE <NORMAL | MASTER | SLAVE | REPLICA |
        PUBSUB>] | [USER username] | [ADDR ip:port] | [LADDR ip:port] |
        [SKIPME <YES | NO>] | [MAXAGE maxage] ...]>>
```

The CLIENT KILL command closes a given client connection.

* CLIENT KILL addr:port. This kill the client matching the given address and port.
* CLIENT KILL ADDR ip:port. This kill the client matching the given address and port.
* CLIENT KILL LADDR ip:port. Kill all clients connected to specified local (bind) address.
* CLIENT KILL ID client-id. Allows to kill a client by its unique ID field. Client ID's are retrieved using the CLIENT LIST command.
* CLIENT KILL TYPE type, where type is one of normal, master, replica and pubsub. This closes the connections of all the clients in the specified class. Note that clients blocked into the MONITOR command are considered to belong to the normal class.
* CLIENT KILL USER username. Closes all the connections that are authenticated with the specified ACL username, however it returns an error if the username does not map to an existing ACL user.
* CLIENT KILL SKIPME yes/no. By default this option is set to yes, that is, the client calling the command will not get killed, however setting this option to no will have the effect of also killing the client calling the command.
* CLIENT KILL MAXAGE maxage. Closes all the connections that are older than the specified age, in seconds.

#### Resp Reply

One of the following:

* Simple string reply: OK when called in 3 argument format and the connection has been closed.
* Integer reply: when called in filter/value format, the number of clients killed.

---

### CLIENT LIST

#### Syntax

```bash
    CLIENT LIST [TYPE <NORMAL | MASTER | REPLICA | PUBSUB>]
        [ID client-id [client-id ...]]
```

The CLIENT LIST command returns information and statistics about the client connections server in a mostly human readable format.

You can use one of the optional subcommands to filter the list. The TYPE type subcommand filters the list by clients' type, where type is one of normal, master, replica, and pubsub.

#### Resp Reply

Bulk string reply: information and statistics about client connections.

---

### CLIENT UNBLOCK

#### Syntax

```bash
    CLIENT UNBLOCK <client-id> [TIMEOUT | ERROR]
```

The command unblocks a client blocked by a blocking command such as BRPOP, XREAD, or BLPOP.

The optional argument specifies how to unblock the client:
* TIMEOUT - Unblock the client as if a timeout occurred (default)
* ERROR - Unblock the client returning an error

#### Resp Reply

Integer reply: 
* 1 if the client was unblocked
* 0 if the client wasn't blocked

---