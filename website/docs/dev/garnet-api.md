---
id: garnet-api
sidebar_label: Garnet API
title: Garnet API
---

The **IGarnetApi** interface contains the operators exposed to the public API, which ultimately perform operations over the keys stored in Garnet. It inherits from **IGarnetReadApi** (read-only commands interface) and **IGarnetAdvancedApi** (advanced API calls). 

For adding a new operator or command to the API, add a new method signature to the **IGarnetReadApi** interface in case the command performs read-only operations, or **IGarnetApi** otherwise.

### Adding a new command to Garnet

_If you are trying to add a command for your specific Garnet server instance, see [Custom Commands](custom-commands.md)_

To add a new command to Garnet, follow these steps:

1. If your command operates on an **object** (i.e. List, SortedSet etc.), add a new enum value to the ```[ObjectName]Operation``` enum in ```libs/server/Objects/[ObjectName]/[ObjectName]Object.cs```\
Otherwise, add a new enum value to the ```RespCommand``` enum in ```libs/server/Resp/Parser/RespCommand.cs```.
2. Register the new command name so that the parser can recognize it. Commands are parsed in ```libs/server/Resp/Parser/RespCommand.cs```: **FastParseCommand** provides a SIMD fast path for a fixed set of common commands, and any command that is not matched there falls through to **ArrayParseCommand**, which resolves the command name via **HashLookupCommand**. To make your command resolvable, add an entry for it to the primary hash table in **RespCommandHashLookup.PopulatePrimaryTable()** (```libs/server/Resp/Parser/RespCommandHashLookupData.cs```).
3. Add a new method signature to **IGarnetReadApi**, in case the command performs read-only operations, or to **IGarnetApi** otherwise (```libs/server/API/IGarnetApi.cs```).
4. Add a new method to the **RespServerSession** class. This method will parse the command from the network buffer, call the storage layer API (method declared in step #3) and write the RESP formatted response back to the network buffer (note that the **RespServerSession** class is divided across several .cs files, object-specific commands will reside under ```libs/server/Resp/Objects/[ObjectName]Commands.cs```, while others will reside under ```libs/server/Resp/[Admin|Array|Basic|etc...]Commands.cs```, depending on the command type).
5. In ```libs/server/Resp/RespServerSession.cs```, add the new command case to the **ProcessBasicCommands** or **ProcessArrayCommands** method respectively, calling the method that was added in step #4.
6. Add a new method to the **StorageSession** class. This method is part of the storage layer. This storage API ONLY performs the RMW or Read operation calls, and it wraps the Tsavorite API. Garnet uses a single unified Tsavorite store that is accessed through three contexts, each with its own input/output types and session functions: the **String** context (```MainSessionFunctions```) for raw-string commands, the **Object** context (```ObjectSessionFunctions```) for data-structure-object commands, and the **Unified** context (```UnifiedSessionFunctions```) for type-agnostic commands (a record's ```ValueIsObject``` bit distinguishes inline string bytes from heap object references). A ```VectorSessionFunctions``` session also runs on the same store for the Vector Sets preview. The **StorageSession** class is divided across several .cs files: object-context operations reside under ```libs/server/Storage/Session/ObjectStore/[ObjectName]Ops.cs```, string-context operations mainly reside under ```libs/server/Storage/Session/MainStore/MainStoreOps.cs```, and type-agnostic operations reside under ```libs/server/Storage/Session/UnifiedStore/```. The corresponding Tsavorite callbacks live under ```libs/server/Storage/Functions/{MainStore,ObjectStore,UnifiedStore}/```.\
To implement the storage-level logic of the new command, follow these guidelines according to the new command type:
    * ***Single-key object command***: If you are adding a command that operates on a single object, the implementation of this method will simply be a call ```[Read|RMW]ObjectStoreOperation[WithOutput]```, which in turn will call the ```Operate``` method in ```libs/server/Objects/[ObjectName]/[ObjectName]Object.cs```, where you will have to add a new case for the command and the object-specific command implementation in ```libs/server/Objects/[ObjectName]/[ObjectName]ObjectImpl.cs```
    * ***Multi-key object command***: If you are adding a command that operates on multiple objects, you may need to create a transaction in which you will appropriately lock the keys (using the ```TransactionManager``` instance). You can then operate on multiple objects (for instance using the ```GET``` & ```SET``` operations).
    * ***String command***: If you are adding a command that operates on raw strings, you'll need to call Tsavorite's ```Read``` or ```RMW``` methods via the string context. If you are calling ```RMW```, you will need to implement the initialization and in-place / copy update functionality of the new command in ```libs/server/Storage/Functions/MainStore/RMWMethods.cs```.
7. If the command supports being called in a transaction context, key locking is key-spec driven. Define the command's ```KeySpecs``` in its command info metadata; the transaction layer then automatically acquires the appropriate key locks via ```TxnKeyManager.LockKeys(SimpleRespCommandInfo ...)``` (```libs/server/Transaction/TxnKeyManager.cs```). 
8. Add tests that run the command and check its output under valid and invalid conditions, test using both ```SE.Redis``` and ```LightClient```, if applicable. For object commands, add tests to ```Garnet.test/Resp[ObjectName]Tests.cs```. For other commands, add to ```Garnet.test/RespTests.cs``` or ```Garnet.test/Resp[AdminCommands|etc...]Tests.cs```, depending on the command type.
9. Add newly supported command documentation to the appropriate markdown file under ```website/docs/commands/```, and specify the command as supported in ```website/docs/commands/api-compatibility.md```.
10. Add command info by following the next [section](#adding-command-info)

:::tip
Before you start implementing your command logic, add a basic test that calls the new command, it will be easier to debug and implement missing logic as you go along. 
:::

### Adding command info

Each supported RESP command in Garnet should have an entry in ```libs/resources/RespCommandsInfo.json```, specifying the command's info.\
A command's info can be added manually, but we recommend using the ```CommandInfoUpdater``` tool to update the JSON file (can be found under ```playground/```).

The ```CommandInfoUpdater``` tool calculates the difference between existing commands in ```libs/resources/RespCommandsInfo.json``` and commands specified in ```CommandInfoUpdater/SupportedCommands.cs```. It then attempts to add/remove commands' info as necessary.  
Info for Garnet-only commands is retrieved from ```CommandInfoUpdater/GarnetCommandsInfo.json```, and info for other RESP commands is retrieved from an external RESP server (which you will need to run locally or have access to in order to run this tool).

To add command info to Garnet, follow these steps:
1. Add the supported command and its supported sub-commands (if applicable) to ```CommandInfoUpdater/SupportedCommands.cs```.
2. If you are adding a Garnet-specific command, add its info to ```CommandInfoUpdater/GarnetCommandsInfo.json```.
3. Start some RESP server locally.
4. Build & run the tool (for syntax help run the tool with `-h` or `--help`).

```bash
cd playground/CommandInfoUpdater
dotnet run -- --output ../../libs/resources
```