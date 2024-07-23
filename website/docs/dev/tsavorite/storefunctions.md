---
id: storefunctions
sidebar_label: StoreFunctions
title: StoreFunctions and Allocator Wrapper
---

# StoreFunctions and Allocator Struct Wrapper

This section discusses both of these because they were part of a change to add two additional type args, `TStoreFunctions` and `TAllocator`, to `TsavoriteKV` as well as the various sessions and `*Context` (e.g. `BasicContext`). The purpose of both of these is to provide better performance by inlining calls. StoreFunctions also provides better logical design for the location of the operations that are store-level rather than session-level, as described below.

## StoreFunctions overview
`StoreFunctions` refers to the set of callback functions that reside at the `TsavoriteKV` level, analogous to `ISessionFunctions` at the session level. Similar to `ISessionFunctions`, there is an `IStoreFunctions`. However, the `ISessionFunctions` implementation can be either a struct or a class' Tsavorite provides the `SessionFunctionsBase` class, which may be inherited from, as a utility. Type parameters implemented by classes, however, do not generate inlined code.

Because `IStoreFunctions` is intended to provide maximum inlining, Tsavorite does not provide a `StoreFunctionsBase`. Instead, Tsavorite provides a `StoreFunctions` struct implementation, with optional implementations passed in, for:
- Key Comparison (previously passed as an `ITsavoriteKeyComparer` interface, which is not inlined)
- Key and Value Serializers. Due to limitations on type arguments, these must be passed as `Func<>` which creates the implementation instance, and because serialization is an expensive operation, we stay with the `IObjectSerializer<TKey>` and `IObjectSerializer<TValue>` interfaces rather than clutter the `IStoreFunctions<TKey, TValue>` interface with the Key and Value Serializer type args.
- Record disposal (previously on `ISessionFunctions` as multiple methods, and now only a single method with a "reason" parameter).
- Checkpoint completion callback (previously on `ISessionFunctions`).

## Allocator Wrapper overview

As with `StoreFunctions`, the Allocator Wrapper is intended to provide maximal inlining. As noted above, type parameters implemented by classes do not generate inlined code; the JITted code is general, for a single `IntPtr`-sized reference. For structs, however, the JITter generates code specific to that specific struct type, in part because the size can vary (e.g. when pushed on the stack as a parameter).

There is a hack that allows a type parameter implemented by a class to be inlined: the generic type must be for a struct that wraps the class type and makes calls on that class type in a non-generic way. This is the approach the Allocator Wrapper takes:
- The `BlittableAllocator`, `GenericAllocator`, and `SpanByteAllocator` classes are now the wrapper structs, with `Key`, `Value`, and `TStoreFunctions` type args.
- There are new `BlittableAllocatorImpl`, `GenericAllocatorImpl`, and `SpanByteAllocatorImpl` classes that implement most of the functionality as previously, including inheriting from `AllocatorBase`. These also have `Key`, `Value`, and `TStoreFunctions` type args; the `TAllocator` is not needed as a type arg because it is known to be the `XxxAllocator` Wrapper struct. The wrapper structs contain an instance of the `XxxAllocatorImpl` class. 
- `AllocatorBase` itself now contains a `_wrapper` field that is a struct-wrapper instance (which contains the instance pointer of the fully-derived allocator class). `AllocatorBase` itself is templated on `TStoreFunctions` and `TAllocator`.


# Notes
-	As you notice, a LOT of propagation of the new Type args. I had an idea I might be able to push at least StoreFunctions into Allocator, but no, it’s viral all the way up.
-	At a high level, we have two interfaces:
o	IAllocatorCallbacks. This contains the methods called by AllocatorBase that we want to inline, so we call this, which then calls down to the derived AllocatorImpl class.
o	IAllocator. This is all inlined calls on the allocator, including IAllocatorCallbacks.
	AllocatorBase contains a _wrapper field that is the IAllocator implementation (TAllocator). 
	It turns out I can’t keep IAllocatorCalbacks as a separate type arg because IAllocator must propagate, but I left it in there (instead of combining it all into IAllocator) as the organization may be useful.
-	Below the interfaces we have the implementation class (e.g. BlittableAllocatorImpl) that inherit AllocatorBase.
-	Within TsavoriteKV, we have
o	hlog: this is now TAllocator (the inlined one)
o	hlogBase: The AllocatorBase. All the calls on this that we don’t need to inline (or are not virtual, such as *Address) are now called on hlogBase.
	I’m thinking of changing these to “allocator” and “allocatorBase”
-	I decided to switch over to using TsavoriteKVSettings, because when we go to storageAPI I think we’ll want this or something similar; at least not an ugly pile of parameters.
o	I added a new AllocatorSettings as well.
-	It was tricky deciding what to do about instantiating the Allocator. 
o	If I did it outside TsavoriteKV, the caller would have to know more internal stuff (epoch, whether to create readcache, …).
o	We create tempKVs, so there’s no good way to pass their allocators in
o	So I ended up passing in an allocatorFactory.
-	BlittableIterationTests is the test file switched to the new format.
