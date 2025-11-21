---
id: storefunctions
sidebar_label: StoreFunctions
title: StoreFunctions and Allocator Wrapper
---

# StoreFunctions and Allocator Struct Wrapper

This section discusses both of these because they were part of a change to add two additional type args, `TStoreFunctions` and `TAllocator`, to `TsavoriteKV` as well as the various sessions and `*Context` (e.g. `BasicContext`). The purpose of both of these is to provide better performance by inlining calls. StoreFunctions also provides better logical design for the location of the operations that are store-level rather than session-level, as described below.

From the caller point of view, we have two new type parameters on `TsavoriteKV<TStoreFunctions, TAllocator>`. The `TStoreFunctions` and `TAllocator` are also on `*.Context` (e.g. `BasicContext`) as well. C# allows the 'using' alias only as the first lines of a namespace declaration, and the alias is file-local and recognized by subsequent 'using' aliases, so the "Api" aliases such as `BasicGarnetApi` in multiple files are much longer now.

`TsavoriteKV` constructor has been changed to take 3 parameters:
- `KVSettings<TKey, TValue>`. This replaces the previous long list of parameters. `LogSettings`, `ReadCacheSettings`, and `CheckpointSettings` have become internal classes, used only by `TsavoriteKV` (created from `TsavoriteKVSettings`) when instantiating the Allocators (e.g. the new `AllocatorSettings` has a `LogSettings` member). `SerializerSettings` has been removed in favor of methods on `IStoreFunctions`.
- An instance of `TStoreFunctions`. This is usually obtained by a call to a static `StoreFunctions` factory method to create it, passing the individual components to be contained.
- A factory `Func<>` for the `TAllocator` instantiation.

These are described in more detail below.

## StoreFunctions overview
`StoreFunctions` refers to the set of callback functions that reside at the `TsavoriteKV` level, analogous to `ISessionFunctions` at the session level. Similar to `ISessionFunctions`, there is an `IStoreFunctions`. However, the `ISessionFunctions` implementation can be either a struct or a class' Tsavorite provides the `SessionFunctionsBase` class, which may be inherited from, as a utility. Type parameters implemented by classes, however, do not generate inlined code.

Because `IStoreFunctions` is intended to provide maximum inlining, Tsavorite does not provide a `StoreFunctionsBase`. Instead, Tsavorite provides a `StoreFunctions` struct implementation, with optional implementations passed in, for:
- Key Comparison (previously passed as an `ITsavoriteKeyComparer` interface, which is not inlined)
- Serializers for Value objects. Due to limitations on type arguments, these must be passed as `Func<>` which creates the implementation instance, and because serialization is an expensive operation, we stay with the `IObjectSerializer` interfaces rather than clutter the `IStoreFunctions` interface with the Key and Value Serializer type args.
- Record disposal (previously on `ISessionFunctions` as multiple methods, and now only a single method with a "reason" parameter).
- Checkpoint completion callback (previously on `ISessionFunctions`).

Of course, because `TsavoriteKV` has the `TStoreFunctions` type parameter, this can be any type implemented by the caller, including a class instance (which would be slower).

## Allocator Wrapper overview

As with `StoreFunctions`, the Allocator Wrapper is intended to provide maximal inlining. As noted above, type parameters implemented by classes do not generate inlined code; the JITted code is general, for a single `IntPtr`-sized reference. For structs, however, the JITter generates code specific to that specific struct type, in part because the size can vary (e.g. when pushed on the stack as a parameter).

There is a hack that allows a type parameter implemented by a class to be inlined: the generic type must be for a struct that wraps the class type and makes calls on that class type in a non-generic way. This is the approach the Allocator Wrapper takes:
- The `SpanByteAllocator` and `ObjectAllocator` classes are now the wrapper structs, with a `TStoreFunctions` type arg. These implement the `IAllocator` interface.
- There are new `SpanByteAllocatorImpl` and `ObjectAllocatorImpl` classes that implement most of the functionality as previously, including inheriting from `AllocatorBase`. These also have a `TStoreFunctions` type arg; the `TAllocator` is not needed as a type arg because it is known to be the `XxxAllocator` Wrapper struct. The wrapper structs contain an instance of the `XxxAllocatorImpl` class. 
- `AllocatorBase` itself now contains a `_wrapper` field that is a struct-wrapper instance (which contains the instance pointer of the fully-derived allocator class) that is constrained to the `IAllocator` interface. `AllocatorBase` itself is templated on `TStoreFunctions` and `TAllocator`.

The new Allocator definition supports two interfaces:
- `IAllocatorCallbacks`, which is inherited by `IAllocator`. This contains the derived-Allocator methods called by `AllocatorBase` that we want to inline rather then virtcall. The struct wrapper `AllocatorBase._wrapper` implements `IAllocatorCallbacks`, so the call on `_wrapper` inlines the call to `IAllocatorCallbacks`, which then calls down to the derived `*AllocatorImpl` class implementation.
- `IAllocator : IAllocatorCallbacks`. This is all inlined calls on the Allocator, including `IAllocatorCallbacks`.
  - It turns out not to be possible to keep `IAllocatorCalbacks` as a separate type arg because `IAllocator` must propagate, but `IAllocatorCallbacks` remains as a separate interface (instead of combining it all into `IAllocator`) as the organization may be useful.

There are still a number of abstract `AllocatorBase` methods, for which inlining of the method call is not important due to the time for the overall call. These are mostly IO and Scan/Iteration methods.

Within `TsavoriteKV`, we have:
- `hlog` remains, but is now of type `TAllocator` (the wrapper struct).
- `hlogBase` is new; it is the `AllocatorBase`. All the calls on the allocator that we don’t need to inline (or are not virtual, such as *Address) are now called on hlogBase.
  - It might be cleaner to rename these to `allocator` and `allocatorBase`.

There is a new `AllocatorSettings` class as well that is created by `TsavoriteKV` when instantiating the allocator. Allocator instantiation is done by a factory `Func<AllocatorSettings, TStoreFunctions>` rather that being passed in as an object, because:
- The caller would have to know more internal stuff such as the epoch, whether to create readcache, and so on.
- We create temporary `TsavoriteKV`s, such as when Scanning or Compacting, so there is no way to pass these instances in.
