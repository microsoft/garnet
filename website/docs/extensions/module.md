---
id: module
sidebar_label: Modules
title: Modules
---

# Modules

Modules help bundle related custom commands, procedures and transactions into a single binary. Once loaded, a module makes all of its contained custom operations available to users.
All modules must derive from the `ModuleBase` class and implement the `OnLoad` method to perform module initialization. All custom commands, procedures and transactions implemented within a module get registered with Garnet during initialization using this method:

`OnLoad(ModuleLoadContext context, string[] args)`

Optional arguments passed in to the module are made available through the `args` parameter. 
The `ModuleLoadContext` exposes the following APIs to register the module and its components:

- `ModuleActionStatus Initialize(string name, uint version)`\
    This must be the first step to register the module with its name and version information. All other registrations must be performed after this.
    The `ModuleActionStatus` enum returned indicates the status of module initialization with `Success` status confirming successful registration of the module.

- `ModuleActionStatus RegisterCommand(string name, CustomRawStringFunctions customFunctions, CommandType type = CommandType.ReadModifyWrite, RespCommandsInfo commandInfo = null, RespCommandDocs commandDocs = null, long expirationTicks = 0)`
    Any custom raw string command should be registered using this method with its `name`, implementation of the raw string command in `customFunctions`, `type` indicating whether it is an ReadModifyWrite or Read command (RMW is the default type), optional `commandInfo` and `commandDocs` to provide details like arity, key specification, acl categories, etc. and the optional `expirationTicks` to govern when the key expires.

- `ModuleActionStatus RegisterTransaction(string name, Func<CustomTransactionProcedure> proc, RespCommandsInfo commandInfo = null, RespCommandDocs commandDocs = null)`
    Transactions should get registered using this with its `name`, a method that returns the transaction implementation `proc` and optional `commandInfo` and `commandDocs`.

- `ModuleActionStatus RegisterType(CustomObjectFactory factory)`
    Custom data types are registered using this method with `factory` being the implementation of the custom object factory that can create instances of the custom object.

- `ModuleActionStatus RegisterCommand(string name, CustomObjectFactory factory, CustomObjectFunctions command, CommandType type = CommandType.ReadModifyWrite, RespCommandsInfo commandInfo = null, RespCommandDocs commandDocs = null)`
    Custom object commands are registered using this with their `name`, `factory` instance registered already using the `RegisterType`, implementation of the custom object command in `command`, `type` indicating whether it is an ReadModifyWrite or Read command (RMW is the default type) and optional `commandInfo` and `commandDocs`.

- `ModuleActionStatus RegisterProcedure(string name, Func<CustomProcedure> customScriptProc, RespCommandsInfo commandInfo = null, RespCommandDocs commandDocs = null)`
    Custom non-transactional procedures are registered using this with their `name`, implementation as `customScriptProc` and optional `commandInfo` and `commandDocs`.

:::tip 
As a reference of an implementation of a module, see the example in playground\SampleModule.
:::

## Loading a module

A module can be loaded at server startup using the `--loadmodulecs` command line option (or the
`LoadModuleCS` configuration setting), or at runtime using the `MODULE LOADCS` command.

Each module is specified as a module path optionally followed by space-separated arguments that are
passed to the module's `OnLoad` method through the `args` parameter:

```
--loadmodulecs "/path/to/MyModule.dll arg0 arg1"
```

The first space-separated token is the module path and the remaining tokens are its arguments. A module
path that itself contains spaces must therefore be wrapped in double quotes. These quotes must be part of
the module specification string that Garnet parses - they are **not** the same as any quoting your shell
performs. Because a shell typically strips its own surrounding quotes, the quotes that need to reach
Garnet usually have to be escaped on the command line (the exact escaping is shell-dependent):

```
# bash-style: the inner quotes are escaped so they are passed through to Garnet
--loadmodulecs "\"/path/to/My Modules/My Module.dll\""
--loadmodulecs "\"/path/to/My Modules/My Module.dll\" arg0 arg1"
```

When the module is specified via the `LoadModuleCS` configuration setting instead (e.g. in a JSON config
file), no shell is involved; the double quotes are simply part of the string value (escaped per JSON):

```
"LoadModuleCS": [ "\"/path/to/My Modules/My Module.dll\" arg0 arg1" ]
```

Multiple modules can be loaded by providing a comma-separated list of module specifications.
