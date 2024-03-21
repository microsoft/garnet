---
id: objects
sidebar_label: Objects
title: Objects
---

# Server-Side Object Extensions

Garnet offers different ways to extend its functionality, one of them by adding your own implementation of a Custom Object; similar to what a Set, List or a Sorted Set offers but using your own type of object and custom commmands functionality with the C# language.

In this document we'll explain the implementation of a new object type, based on the Dictionary type of C#, then we'll add some custom commands that will use this new object type.


**Implementing a Custom Object**

In order to add a new object type, you first need to implement a new class that inherits from GarnetObjectBase class. This class contains essential methods to manage the basic functionality of a key in Garnet. 

**Adding a Factory class**

Once the new Custom Object class implementation has been added, it also requires a class that will manage the creation of the new Object type, this class is CustomObjectFactory.

**The Operate methods**

Every command in Garnet has a unique identifier in order to map the different operators to its corresponding type to which they can be applied. Take as an example the ZADD command, this command is only used on a Sorted Set type. In addition to its core functionality of the ZADD, it contains optional functionality based on each of the flags or switches that the user can pass as part of the command expression. 

These both elements of a command expresion are called Command and Subcommand respectively.
Each of them are applied in the following methods of the GarnetObjectBase class and you will need to add your custom functionality at these specific methods.

The Operate method for a Command with a subcommand operator

```csharp
public abstract void Operate(byte subCommand, ReadOnlySpan<byte> input, ref (IMemoryOwner<byte>, int) output);
```

The Operate method for a Command without subcommand operator

```csharp
public override sealed unsafe bool Operate(ref SpanByte input, ref SpanByteAndMemory output, out long sizeChange)
```


:::tip 
As a reference of an implementation of a Custom Object type, see the example in GarnetServer\MyDictObject.cs.
