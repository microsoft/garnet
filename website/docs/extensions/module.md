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

- `ModuleActionStatus RegisterCommand(string name, CustomRawStringFunctions customFunctions, CommandType type = CommandType.ReadModifyWrite, RespCommandsInfo commandInfo = null, long expirationTicks = 0)`
    Any custom raw string command should be registered using this method with its `name`, implementation of the raw string command in `customFunctions`, `type` indicating whether it is an ReadModifyWrite or Read command (RMW is the default type), optional `commandInfo` to provide details like arity, key specification, acl categories, etc. and the optional `expirationTicks` to govern when the key expires.

- `ModuleActionStatus RegisterTransaction(string name, Func<CustomTransactionProcedure> proc, RespCommandsInfo commandInfo = null)`
    Transactions should get registered using this with its `name`, a method that returns the tansaction implementation `proc` and optional `commandInfo`.

- `ModuleActionStatus RegisterType(CustomObjectFactory factory)`
    Custom data types are registered using this method with `factory` being the implementation of the custom object factory that can create instances of the custom object.

- `ModuleActionStatus RegisterCommand(string name, CustomObjectFactory factory, CustomObjectFunctions command, CommandType type = CommandType.ReadModifyWrite, RespCommandsInfo commandInfo = null)`
    Custom object commands are registered using this with their `name`, `factory` instance registered already using the `RegisterType`, implementation of the custom object command in `command`, `type` indicating whether it is an ReadModifyWrite or Read command (RMW is the default type) and optional `commandInfo`.

- `ModuleActionStatus RegisterProcedure(string name, CustomProcedure customScriptProc, RespCommandsInfo commandInfo = null)`
    Custom non-transactional procedures are registered using this with their `name`, implementation as `customScriptProc` and optional `commandInfo`.

:::tip 
As a reference of an implementation of a module, see the example in playground\SampleModule.
