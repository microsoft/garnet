// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// The per-connection send buffer stack hands out pinned buffers. A popped entry whose slot is left
    /// populated stays strongly referenced by the array for the lifetime of the connection, so releasing the
    /// buffer subtracts it from the accounting while the pinned memory remains.
    /// </summary>
    [TestFixture]
    public class LightConcurrentStackTests
    {
        sealed class Payload : IDisposable
        {
            public bool Disposed;

            public void Dispose() => Disposed = true;
        }

        [Test]
        public void PoppedEntriesBecomeCollectable()
        {
            var stack = new LightConcurrentStack<Payload>(8);

            var weak = PushPopAndDrop(stack);

            GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
            GC.WaitForPendingFinalizers();
            GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);

            var alive = weak.IsAlive;

            // Without this the JIT may collect the stack itself during the forced collections, in which case
            // the payload becomes unreachable whether or not the vacated slot was cleared and the assertion
            // holds for the wrong reason. Verified: with optimization on and the slot clear reverted, this
            // test passes without the KeepAlive and fails with it.
            GC.KeepAlive(stack);

            ClassicAssert.IsFalse(alive,
                "the popped entry is still rooted by the stack, so releasing it frees the accounting but not the memory");
        }

        /// <summary>
        /// Kept out of the caller so the popped reference cannot be held alive by a stack slot of the test
        /// method's own frame, which would make the assertion pass for the wrong reason.
        /// </summary>
        static WeakReference PushPopAndDrop(LightConcurrentStack<Payload> stack)
        {
            var payload = new Payload();
            var weak = new WeakReference(payload);

            ClassicAssert.IsTrue(stack.TryPush(payload));
            ClassicAssert.IsTrue(stack.TryPop(out var popped, out var disposed));
            ClassicAssert.IsFalse(disposed);
            ClassicAssert.AreSame(payload, popped);

            popped.Dispose();
            ClassicAssert.IsTrue(payload.Disposed);

            return weak;
        }

        /// <summary>
        /// The same defect in the non-concurrent stack, which is what <see cref="Garnet.server.ScratchBufferAllocator"/>
        /// stacks its outgrown pinned buffers on. Its element type is a struct, so the slot cannot simply be
        /// nulled -- the clear is gated on the struct containing references, and the payload here is held
        /// through exactly that shape.
        /// </summary>
        [Test]
        public void PoppedSimpleStackEntriesBecomeCollectable()
        {
            var stack = new SimpleStack<Boxed>(4);

            var weak = PushPopAndDropSimple(stack);

            GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
            GC.WaitForPendingFinalizers();
            GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);

            var alive = weak.IsAlive;

            // As above: without this the stack itself may be collected during the forced collections and the
            // assertion would hold whether or not the slot was cleared.
            GC.KeepAlive(stack);

            ClassicAssert.IsFalse(alive,
                "the popped struct is still in the vacated slot, so the buffer it references stays rooted");
        }

        /// <summary>
        /// A struct that holds a reference, mirroring the shape of the scratch buffer the production stack
        /// carries. Popping returns the struct by value, so the vacated slot is the only remaining root.
        /// </summary>
        struct Boxed
        {
            public byte[] Data;
        }

        static WeakReference PushPopAndDropSimple(SimpleStack<Boxed> stack)
        {
            var data = new byte[64 * 1024];
            var weak = new WeakReference(data);

            stack.Push(new Boxed { Data = data });
            var popped = stack.Pop();
            ClassicAssert.AreSame(data, popped.Data);

            return weak;
        }

        [Test]
        public void DisposeReleasesEveryRemainingEntry()
        {
            var stack = new LightConcurrentStack<Payload>(8);
            var entries = new Payload[4];
            for (var i = 0; i < entries.Length; i++)
            {
                entries[i] = new Payload();
                ClassicAssert.IsTrue(stack.TryPush(entries[i]));
            }

            stack.Dispose();

            foreach (var entry in entries)
                ClassicAssert.IsTrue(entry.Disposed);
            ClassicAssert.IsFalse(stack.TryPush(new Payload()), "a disposed stack must not accept new entries");
            ClassicAssert.IsFalse(stack.TryPop(out _, out var disposed));
            ClassicAssert.IsTrue(disposed);
        }
    }
}