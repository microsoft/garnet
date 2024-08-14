---
id: objects
sidebar_label: Objects
title: Objects
---

# Server-Side Object Extensions

Garnet offers different ways to extend its functionality, one of them by adding your own implementation of a Custom Object; similar to what a Set, List or a Sorted Set offers but using your own type of object and custom commmands functionality with the C# language.

In this document we'll explain the implementation of a new object type, based on the Dictionary type of C#, then we'll add some custom commands that will use this new object type.


**Implementing a Custom Object**

In order to add a new object type, you first need to implement a new class that inherits from `CustomObjectBase` class. This class contains essential methods to manage the basic functionality of an object in Garnet. 

**Adding a Factory class**

Once the new Custom Object class implementation has been added, it also requires a class that will manage the creation of the new Object type, this class must derive from `CustomObjectFactory`.

### Developing custom server side object command

`CustomObjectFunctions` is the base class for all custom object commands. To develop a new one, this class has to be extended and then include the custom logic. There are three  methods to be implemented in a new custom object command:

- `NeedInitialUpdate(ReadOnlyMemory<byte> key, ReadOnlySpan<byte> input, ref (IMemoryOwner<byte>, int) output)`\
    The `NeedInitialUpdate` determines whether a new record must be created or not given the key for the record (`key`), the user input (`input`) to be used for computing the value (`value`) for the new record. If this method returns true, a new record is created otherwise not.\
- `Reader(ReadOnlyMemory<byte> key, ReadOnlySpan<byte> input, IGarnetObject value, ref (IMemoryOwner<byte>, int) output, ref ReadInfo readInfo);`\
    The `Reader` method performs a record read, given the key for the record to be read (`key`), the user input for computing `output` from `value`. The `readInfo` helps control whether a record needs to be expired as part of the read operation using the `ReadAction` option. If a command is meant for updating or upserting records, this method need not be overriden as the default implementation is to throw `NotImplementedException`.
- `Updater(ReadOnlyMemory<byte> key, ReadOnlySpan<byte> input, IGarnetObject value, ref (IMemoryOwner<byte>, int) output, ref RMWInfo rmwInfo)`\
    The `Updater` method makes an update for RMW or upsert, given the key (`key`), the given input (`input`), the resulting value to be inserted (`value`), the location where the result of the `input` operation on `value` is to be copied (`output`) and the reference for the record info for this record (used for locking) (`rmwInfo`). If a command is meant for a pure readonly operation, this method need not be overriden as the default implementation is to throw `NotImplementedException`.

There is an optional method available, if needed:
- `InitialUpdater(ReadOnlyMemory<byte> key, ReadOnlySpan<byte> input, IGarnetObject value, ref (IMemoryOwner<byte>, int) output, ref RMWInfo rmwInfo)`\
    The `InitialUpdater` is available to be overriden if any specialized processing is needed when the object is created initially. Otherwise, the default implementation of this method invokes the `Updater` method.

:::tip 
As a reference of an implementation of a Custom Object type, see the example in GarnetServer\Extensions\MyDictObject.cs. Commands that operate on this object are available in MyDictSet.cs and MyDictGet.cs.
