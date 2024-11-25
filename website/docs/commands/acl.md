---
id: acl-commands
sidebar_label: ACL
title: ACL
slug: acl
---

### ACL CAT

#### Syntax

```bash
    ACL CAT [category]
```

The command shows the available ACL categories if called without arguments. If a category name is given, the command shows all the Garnet commands in the specified category.

#### Resp Reply

One of the following:

* Array reply: an array of Bulk string reply elements representing ACL categories or commands in a given category.
* Simple error reply: the command returns an error if an invalid category name is given.

---

### ACL DELUSER

#### Syntax

```bash
    ACL DELUSER username [username ...]
```

Delete all the specified ACL users and terminate all the connections that are authenticated with such users. Note: the special default user cannot be removed from the system, this is the default user that every new connection is authenticated with. The list of users may include usernames that do not exist, in such case no operation is performed for the non existing users.

#### Resp Reply

Integer reply: the number of users that were deleted. This number will not always match the number of arguments since certain users may not exist.

---

### ACL LIST

#### Syntax

```bash
    ACL LIST
```

The command shows the currently active ACL rules in the Garnet server. 

#### Resp Reply

Array reply: an array of Bulk string reply elements.

---

### ACL LOAD

#### Syntax

```bash
    ACL LOAD
```

When Garnet is configured to use an ACL file, this command will reload the ACLs from the file, replacing all the current ACL rules with the ones defined in the file. The command makes sure to have an all or nothing behavior, that is:

* If every line in the file is valid, all the ACLs are loaded.
* If one or more line in the file is not valid, nothing is loaded, and the old ACL rules defined in the server memory continue to be used.

#### Resp Reply

Returns +OK on success, otherwise --ERR message if any.

---

### ACL SAVE

#### Syntax

```bash
    ACL SAVE
```

When Redis is configured to use an ACL file (with the aclfile configuration option), this command will save the currently defined ACLs from the server memory to the ACL file.

#### Resp Reply

Returns +OK on success, otherwise --ERR message if any.

---

### ACL SETUSER

#### Syntax

```bash
    ACL SETUSER username [rule [rule ...]]
```

Create an ACL user with the specified rules or modify the rules of an existing user.

Manipulate Garnet ACL users interactively. If the username does not exist, the command creates the username without any privilege. It then reads from left to right all the rules provided as successive arguments, setting the user ACL rules as specified. If the user already exists, the provided ACL rules are simply applied in addition to the rules already set.

#### Resp Reply

Returns +OK on success, otherwise --ERR message if any.

---

### ACL USERS

#### Syntax

```bash
    ACL USERS
```

The command shows a list of all the usernames of the currently configured users in the Garnet ACL system.

#### Resp Reply

Array reply: list of existing ACL users.

---

### ACL WHOAMI

#### Syntax

```bash
    ACL WHOAMI
```

Return the username the current connection is authenticated with. New connections are authenticated with the "default" user. They can change user using AUTH.

#### Resp Reply

Bulk string reply: the username of the current connection.

---