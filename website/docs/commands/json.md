---
id: json
sidebar_label: JSON Module
title: JSON Module
slug: json
---

Below is the full list of JSON Module commands and their implementation status in Garnet.<br/>
Note that this list is subject to change as we continue to expand our API command support with the help of our growing community.

## Json Commands List

| Command                 | Implemented in Garnet | Notes |
| ----------------------- | --------------------- | ----- |
| [JSON.GET](#jsonget)    | ➕                    |       |
| [JSON.SET](#jsonset)    | ➕                    |       |
| JSON.DEL                | ➖                    |       |
| JSON.TYPE               | ➖                    |       |
| JSON.NUMINCRBY         | ➖                    |       |
| JSON.NUMMULTBY         | ➖                    |       |
| JSON.STRAPPEND         | ➖                    |       |
| JSON.STRLEN            | ➖                    |       |
| JSON.ARRAPPEND         | ➖                    |       |
| JSON.ARRINDEX          | ➖                    |       |
| JSON.ARRINSERT         | ➖                    |       |
| JSON.ARRLEN            | ➖                    |       |
| JSON.ARRPOP            | ➖                    |       |
| JSON.ARRTRIM           | ➖                    |       |
| JSON.OBJKEYS           | ➖                    |       |
| JSON.OBJLEN            | ➖                    |       |

## Command Details

### JSON.GET

#### Syntax

```bash
JSON.GET key [INDENT indent-string] [NEWLINE newline-string] [SPACE space-string] [path [path ...]]
```

#### Arguments
- **key**: Key holding the JSON document
- **INDENT**: (Optional) Indentation string for pretty printing
- **NEWLINE**: (Optional) Newline string for pretty printing
- **SPACE**: (Optional) Space string for pretty printing
- **path**: (Optional) [JSONPath-like expression](#supported-json-path-syntax) to specify which elements to retrieve

#### Response Format
Returns one of the following:
- Bulk string: JSON-encoded value if path matches
- Array: When multiple paths are requested
- Null: If key or path doesn't exist

#### Examples

```bash
# Store a complex JSON object
JSON.SET example $ '{"name":"John","address":{"city":"London","postal":"SW1A"},"contacts":[{"type":"email","value":"john@example.com"},{"type":"phone","value":"123-456-789"}]}'

# Get entire document with pretty printing
JSON.GET example . INDENT "  " NEWLINE "\n"
-> {"name":"John","address":{"city":"London","postal":"SW1A"},"contacts":[{"type":"email","value":"john@example.com"},{"type":"phone","value":"123-456-789"}]}

# Get multiple specific paths
JSON.GET example .name .address.city .contacts[0].value
-> ["John","London","john@example.com"]

# Get array elements with filter
JSON.GET example .contacts[?(@.type=="email")].value
-> ["john@example.com"]
```

### JSON.SET

#### Syntax

```bash
JSON.SET key path value [NX | XX]
```

#### Arguments
- **key**: Key to store the JSON document under
- **path**: [JSONPath-like expression](#supported-json-path-syntax) specifying the location to set
- **value**: Valid JSON value or a string representation
- **NX**: Only set the key if it does not exist
- **XX**: Only set the key if it already exists

#### Response Format
Returns one of:
- "OK": Operation successful
- Null: Operation failed (when using NX/XX conditions)

#### Examples

```bash
# Set with NX (only if not exists)
JSON.SET user:3 $ '{"name":"Bob"}' NX
-> OK

# Set with XX (only if exists)
JSON.SET user:3 .age '25' XX
-> OK

# Modify nested array
JSON.SET user:3 .profile.interests[1] '"reading"'
-> OK
```

#### Important Notes
1. Path must start with a dot (.)
2. String values must be properly escaped
3. Arrays are zero-based indexed
4. Setting a non-existing parent path will fail
5. NX and XX are mutually exclusive

## Supported Json Path Syntax

| Path Type          | Syntax                            | Description                                | Example                                | Notes                        |
| ------------------ | --------------------------------- | ------------------------------------------ | -------------------------------------- | ---------------------------- |
| Root               | `.` or `$`                        | References the root JSON document          | `JSON.GET key $`                       | Both notations supported     |
| Property Access    | `.property` or `$.property`       | Accesses direct property of object         | `JSON.GET key $.name`                  | Case sensitive               |
| Nested Property    | `.prop1.prop2` or `$.prop1.prop2` | Accesses nested object properties          | `JSON.GET key $.address.city`          |                              |
| Array Index        | `.array[n]` or `$.array[n]`       | Accesses array element at index n          | `JSON.GET key $.users[0]`              | Zero-based indexing          |
| Array Slice        | `.array[start:end]`               | Retrieves array elements from start to end | `JSON.GET key $.users[1:3]`            | End index is exclusive       |
| Array Last Element | `.array[-1]`                      | Accesses last element of array             | `JSON.GET key $.users[-1]`             |                              |
| All Array Elements | `.[*]` or `$[*]`                  | Selects all elements in array              | `JSON.GET key $[*]`                    |                              |
| Array Filter       | `.[?(@.prop==value)]`             | Filters array elements by condition        | `JSON.GET key $.users[?(@.age>21)]`    |                              |
| Multiple Paths     | `.prop1 .prop2`                   | Retrieves multiple paths at once           | `JSON.GET key $.name $.age`            |                              |
| Wildcard           | `.*` or `$.*`                     | Selects all properties of object           | `JSON.GET key $.address.*`             |                              |
| Array Append       | `.[+]`                            | Appends to array (SET only)                | `JSON.SET key $.list[+] value`         | Not supported as of now      |
| Array Range        | `.[start:]`                       | Selects elements from start to end         | `JSON.GET key $.list[2:]`              |                              |
| Conditional        | `.[?(@.size=="L")]`               | Complex array filtering                    | `JSON.GET key $.items[?(@.size=="L")]` | Supports multiple conditions |
| Nested Arrays      | `.[*][0]` or `$[*][0]`            | Access nested array elements               | `JSON.GET key $.matrix[*][0]`          |                              |

**Special Considerations:**
- Array indices are zero-based
- Property names are case-sensitive
- Invalid paths return null
- Multiple filters can be combined with && and ||