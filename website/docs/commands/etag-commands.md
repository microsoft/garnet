---
id: etag-commands
sidebar_label: ETags 
title: ETAG
slug: etag 
---

---

## ETag Support

Garnet provides support for ETags on raw strings. By using the ETag-related commands outlined below, you can associate any string-based key-value pair inserted into Garnet with an automatically updated ETag.

Compatibility with non-ETag commands and the behavior of data inserted with ETags are detailed at the end of this document.

---

### **SETWITHETAG**

#### **Syntax**

```bash
SETWITHETAG key value [RETAINETAG]
```

Inserts a key-value string pair into Garnet, associating an ETag that will be updated upon changes to the value.

**Options:**

* RETAINETAG -- Update the Etag associated with the previous key-value pair, while setting the new value for the key. If not etag existed for the previous key this will initialize one.

#### **Response**

One of the following:

- **Integer reply**: A response integer indicating the initial ETag value on success.

---

### **GETWITHETAG**

#### **Syntax**

```bash
GETWITHETAG key
```

Retrieves the value and the ETag associated with the given key.

#### **Response**

One of the following:

- **Array reply**: An array of two items returned on success. The first item is an integer representing the ETag, and the second is the bulk string value of the key. If called on a key-value pair without ETag, the first item will be nil.
- **Nil reply**: If the key does not exist.

---

### **SETIFMATCH**

#### **Syntax**

```bash
SETIFMATCH key value etag
```

Updates the value of a key if the provided ETag matches the current ETag of the key.

#### **Response**

One of the following:

- **Integer reply**: The updated ETag if the value was successfully updated.
- **Nil reply**: If the key does not exist.
- **Error reply (ETag mismatch)**: If the provided ETag does not match the current ETag. If the command is called on a record without an ETag we will return ETag mismatch as well.

---

### **GETIFNOTMATCH**

#### **Syntax**

```bash
GETIFNOTMATCH key etag
```

Retrieves the value if the ETag associated with the key has changed; otherwise, returns a response indicating no change.

#### **Response**

One of the following:

- **Array reply**: If the ETag does not match, an array of two items is returned. The first item is the updated ETag, and the second item is the value associated with the key. If called on a record without an ETag the first item in the array will be nil.
- **Nil reply**: If the key does not exist.
- **Simple string reply**: Returns a string indicating the value is unchanged if the provided ETag matches the current ETag.

---

## Compatibility and Behavior with Non-ETag Commands

ETag commands executed on keys that were not set with `SETWITHETAG` will return a type mismatch error. Additionally, invoking `SETWITHETAG` on an existing key will overwrite the key-value pair and reset the associated ETag.

Below is the expected behavior of ETag-associated key-value pairs when non-ETag commands are used.

- **MSET, BITOP**: These commands will replace an existing ETag-associated key-value pair with a non-ETag key-value pair, effectively removing the ETag.
- **SET**: If only if used with additional option "RETAINETAG" will update the etag while inserting the key-value pair over the existing key-value pair.
- **RENAME**: Renaming an ETag-associated key-value pair will reset the ETag to 0 for the renamed key.

---

### **Same Behavior as Non-ETag Key-Value Pairs**

The following commands do not expose the ETag to the user and behave the same as non-ETag key-value pairs. From the user's perspective, there is no indication that a key-value pair is associated with an ETag.

- **GET**
- **DEL**
- **EXISTS**
- **EXPIRE**
- **PEXPIRE**
- **PERSIST**
- **GETRANGE**
- **TTL**
- **PTTL**
- **GETDEL**
- **STRLEN**
- **GETBIT**
- **BITCOUNT**
- **BITPOS**
- **BITFIELD_RO**

### **Commands That Update ETag Internally**

The following commands update the underlying data and consequently update the ETag of the key-value pair. However, the new ETag will not be exposed to the user until explicitly retrieved via an ETag-related command.

- **SETRANGE**
- **APPEND**
- **INCR**
- **INCRBY**
- **DECR**
- **DECRBY**
- **SETBIT**
- **UNLINK**
- **MGET**
- **BITFIELD**

---