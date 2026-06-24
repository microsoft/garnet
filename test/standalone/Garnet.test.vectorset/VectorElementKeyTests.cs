// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Text;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    [TestFixture]
    public class VectorElementKeyTests : TestBase
    {
        [Test]
        public void Basic([Values(1, 4, 8)] int namespaceLength)
        {
            Span<byte> ns0Bytes = stackalloc byte[namespaceLength];
            Span<byte> ns1Bytes = stackalloc byte[namespaceLength];

            for (var i = 0; i < ns0Bytes.Length; i++)
            {
                ns0Bytes[i] = (byte)(i + 1);
                ns1Bytes[i] = (byte)(i + 2);
            }

            // All different
            {
                var key0 = new VectorElementKey(ns0Bytes, "foo"u8);
                var key1 = new VectorElementKey(ns1Bytes, "bar"u8);

                ClassicAssert.IsTrue(GarnetKeyComparer.Instance.Equals(key0, key0));
                ClassicAssert.IsTrue(GarnetKeyComparer.Instance.Equals(key1, key1));

                ClassicAssert.IsFalse(GarnetKeyComparer.Instance.Equals(key0, key1));
                ClassicAssert.IsFalse(GarnetKeyComparer.Instance.Equals(key1, key0));
            }

            // Same key
            {
                var key0 = new VectorElementKey(ns0Bytes, "foo"u8);
                var key1 = new VectorElementKey(ns1Bytes, "foo"u8);

                ClassicAssert.IsTrue(GarnetKeyComparer.Instance.Equals(key0, key0));
                ClassicAssert.IsTrue(GarnetKeyComparer.Instance.Equals(key1, key1));

                ClassicAssert.IsFalse(GarnetKeyComparer.Instance.Equals(key0, key1));
                ClassicAssert.IsFalse(GarnetKeyComparer.Instance.Equals(key1, key0));
            }

            // Same namespace
            {
                var key0 = new VectorElementKey(ns0Bytes, "foo"u8);
                var key1 = new VectorElementKey(ns0Bytes, "bar"u8);

                ClassicAssert.IsTrue(GarnetKeyComparer.Instance.Equals(key0, key0));
                ClassicAssert.IsTrue(GarnetKeyComparer.Instance.Equals(key1, key1));

                ClassicAssert.IsFalse(GarnetKeyComparer.Instance.Equals(key0, key1));
                ClassicAssert.IsFalse(GarnetKeyComparer.Instance.Equals(key1, key0));
            }

            // Same
            {
                var key0 = new VectorElementKey(ns0Bytes, "foo"u8);
                var key1 = new VectorElementKey(ns0Bytes, "foo"u8);

                ClassicAssert.IsTrue(GarnetKeyComparer.Instance.Equals(key0, key0));
                ClassicAssert.IsTrue(GarnetKeyComparer.Instance.Equals(key1, key1));

                ClassicAssert.IsTrue(GarnetKeyComparer.Instance.Equals(key0, key1));
                ClassicAssert.IsTrue(GarnetKeyComparer.Instance.Equals(key1, key0));
            }
        }

        [Test]
        public unsafe void MakeVectorElementKey()
        {
            const int ContextsPerStart = 100;

            const string EmptyKey = "";
            const string SmallKey = "abcd";
            const string BigKey = "fizzbuzz";

            const ulong ContextStart = 8;
            const ulong ExtendedContextStart = 128;
            const ulong ShortContextStart = 256;
            const ulong IntContextStart = 65_535;
            const ulong UIntContextStart = 2_147_483_647;

            string[] keys = [EmptyKey, SmallKey, BigKey];
            ulong[] contextStarts = [ContextStart, ExtendedContextStart, ShortContextStart, IntContextStart, UIntContextStart];

            foreach (var key0 in keys)
            {
                var alloc0 = GC.AllocateArray<byte>(sizeof(int) + key0.Length, pinned: true);

                foreach (var context0Start in contextStarts)
                {
                    for (ulong i = 0; i < ContextsPerStart; i++)
                    {
                        var context0 = context0Start + i;

                        BinaryPrimitives.WriteInt32LittleEndian(alloc0, key0.Length);
                        _ = Encoding.ASCII.GetBytes(key0, alloc0.AsSpan()[sizeof(int)..]);

                        VectorElementKey vecKey0;
                        fixed (byte* alloc0Ptr = alloc0)
                        {
                            vecKey0 = VectorManager.MakeVectorElementKey(context0, (nint)(alloc0Ptr + 4), (nuint)key0.Length);
                        }

                        var hash0 = GarnetKeyComparer.Instance.GetHashCode64(vecKey0);

                        ClassicAssert.IsTrue(GarnetKeyComparer.Instance.Equals(vecKey0, vecKey0));

                        foreach (var key1 in keys)
                        {
                            var alloc1 = GC.AllocateArray<byte>(sizeof(int) + key1.Length, pinned: true);

                            foreach (var context1Start in contextStarts)
                            {
                                for (ulong j = 0; j < ContextsPerStart; j++)
                                {
                                    var context1 = context1Start + j;

                                    BinaryPrimitives.WriteInt32LittleEndian(alloc1, key1.Length);
                                    _ = Encoding.ASCII.GetBytes(key1, alloc1.AsSpan()[sizeof(int)..]);

                                    VectorElementKey vecKey1;
                                    fixed (byte* alloc1Ptr = alloc1)
                                    {
                                        vecKey1 = VectorManager.MakeVectorElementKey(context1, (nint)(alloc1Ptr + 4), (nuint)key1.Length);
                                    }

                                    var hash1 = GarnetKeyComparer.Instance.GetHashCode64(vecKey1);

                                    ClassicAssert.IsTrue(GarnetKeyComparer.Instance.Equals(vecKey1, vecKey1));

                                    var cmpRes0 = GarnetKeyComparer.Instance.Equals(vecKey0, vecKey1);
                                    var cmpRes1 = GarnetKeyComparer.Instance.Equals(vecKey1, vecKey0);

                                    ClassicAssert.AreEqual(cmpRes0, cmpRes1);

                                    var expected = key0.Equals(key1, StringComparison.Ordinal) && context0 == context1;

                                    ClassicAssert.AreEqual(expected, cmpRes0);
                                }
                            }

                            GC.KeepAlive(alloc1);
                        }
                    }
                }
            }
        }
    }
}
