---
id: custom-commands
sidebar_label: Custom Commands
title: Garnet Custom Commands
---

## Overview

Garnet supports registering custom commands and transactions implemented in C#, both programmatically on the server-side and through running dedicated REGISTER command in the client-side.

## Supported Custom Command Types
1. **Custom Raw String Commands**: To implement a custom raw string command, implement a class that inherits from the `CustomRawStringFunctions` base class (See example: `GarnetServer\DeleteIfMatch.cs`)
2. **Custom Object Commands**: To implement a custom object command, implement your custom object class that inherits from `GarnetObjectBase` base class, as well as a factory that creates your object type that inherits from the `CustomObjectFactory` base class (See example: `GarnetServer\MyDictObject.cs`)
3. **Custom Transaction**: To implement a custom transaction, implement a class that inherits from the `CustomTransactionProcedure` base class (See example: `GarnetServer\Procedures\ReadWriteTxn.cs`)

## Server-Side Command Registration

To register a new custom command from the server-side, use the GarnetServer instance's RegisterApi, and call either `NewCommand` or `NewTransactionProc` according to the custom command type that you are trying to register.
1. **Custom Raw String Commands**: To register a new command using a concrete class implementing `CustomRawStringFunctions`, call `RegisterApi.NewCommand(string name, int numParams, CommandType type, CustomRawStringFunctions customFunctions, long expirationTicks = 0)`, where `customFunctions` is an instance of the new concrete class.
2. **Custom Object Commands**: To register a new command using a concrete class implementing `CustomObjectFactory`, call `RegisterApi.NewCommand(string name, int numParams, CommandType commandType, CustomObjectFactory factory)`, where `factory` is an instance of the new concrete class.
2. **Custom Transaction**: To register a new transaction using a concrete class implementing `CustomTransactionProcedure`, call `NewTransactionProc(string name, int numParams, Func<CustomTransactionProcedure> proc)`, where `proc` is a `Func` that returns an instance of the new concrete class.

## Client-Side Command Registration
To register a new custom command from the client-side, use the dedicated REGISTER command in the client app (**note:** this is an **admin command**). <br/>
The REGISTER command takes assemblies that exist on the server and contain implementations for either of the supported custom command classes, and registers the custom commands on the server according to parameters given by the client <br/>
The REGISTER command supports registering multiple commands and / or transactions from multiple files and / or directories containing C# binary files (*.dll / *.exe).<br/>

### Enabling Client-Side Command Registration
To enable registering client-side custom commands, you must specify a list of allowed paths from which assemblies can potentially be loaded. This is done by setting the configuration parameter `ExtensionBinPaths`.
Example (in garnet.config): 
```
"ExtensionBinPaths": ".\first\path\,..\..\second\path\"
```
**Note #1**: Assemblies can be loaded from any directory level under these specified paths<br/>
**Note #2**: By default, Garnet only allows loading of digitally signed assemblies. To remove that requirement (not recommended), set the configuration parameter `ExtensionAllowOnlySignedAssemblies` to `false`.

### REGISTER Command
To run the REGISTER command, run the REGISTER keyword followed by one or more new command subcommands, followed by the SRC keyword, which is followed by one or more paths to C# binary files or directories containing C# binary files (*.dll / *.exe).<br/>
#### Full command syntax:
```
REGISTER cmdType name numParams className [expTicks] [cmdType name numParams className [expTicks] ...] SRC path [path ...]
```
Each new command that you intend to register, is specified by either of the command type keywords, followed by its parameters.
#### New command sub-command syntax:
```
cmdType name numParams cmdType className [expTicks]
```
**Command Type:**<br/>
The following keywords are legal values for `cmdType`:
*  **READ** (New read command)
*  **READMODIFYWRITE** or **RMW** (New Read-modify-write command)
*  **TRANSACTION** or **TXN** (New transaction)

**Parameters:**
* **name**: Name of the new command
* **numParams**: Number of parameters taken by the new command
* **className**: Class name of concrete class implementing `CustomRawStringFunctions` or `CustomObjectFactory` for new **READ** / **RMW** commands, or `CustomTransactionProcedure` for new **TXN** commands (must exist in assemblies referenced in paths list)<br/>
* **expTicks**: Command expiry in ticks (only relevant for custom raw string commands, ignored otherwise)
    * **-1**: remove existing expiration metadata;
    * **0**: retain whatever it is currently (or no expiration if this is a new entry) - this is the default;
    * **\>0**: => set expiration to given value.

**Note:** If the server could not register one or more of the commands / transactions specified, or if it could not load or enumerate one or more of the files or directories specified in the REGISTER command, no action will be taken (i.e. no command will be registered), and the command execution will fail with an appropriate error message.

### RESP Reply
Returns +OK on success, otherwise --ERR message if any

### REGISTER Errors
* **ERR malformed REGISTER command** - Error in the parsing of the REGISTER command
* **ERR unable to access one or more binary files** - Error accessing or enumerating one or more files or folders specified in path list
* **ERR one or more binary file are not contained in allowed paths** - One or more of the binary paths specified are not contained in the allowed path list according to the server configuration (`ExtensionBinPaths` in garnet.config)
* **ERR unable to load one or more assemblies** - Error loading one or more assemblies from binary files retrieved from path list (assembly load is attempted on all *.dll / *.exe files specified or found in directories specified)
* **ERR one or more assemblies loaded is not digitally signed** - One or more of the loaded assemblies are not digitally signed. This error will occur when trying to load unsigned assemblies when the server configuration is set to allow loading of only digitally signed assemblies (`ExtensionAllowOnlySignedAssemblies` in garnet.conf)
* **ERR unable to instantiate one or more classes from given assemblies** - Error creating an instance of one or more classes specified using loaded assemblies (note that each class should have an empty constructor)
* **ERR unable to register one or more unsupported classes** - Error registering one or more unsupported classes, i.e. one or more of the classes were not implementing one of the expected base classes