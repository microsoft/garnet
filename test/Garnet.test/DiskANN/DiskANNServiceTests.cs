using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Allure.NUnit;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.DiskANN
{
    [AllureNUnit]
    [TestFixture]
    public class DiskANNServiceTests
    {
        private delegate void ReadCallbackDelegate(ulong context, uint numKeys, nint keysData, nuint keysLength, nint dataCallback, nint dataCallbackContext);
        private delegate byte WriteCallbackDelegate(ulong context, nint keyData, nuint keyLength, nint writeData, nuint writeLength);
        private delegate byte DeleteCallbackDelegate(ulong context, nint keyData, nuint keyLength);
        private delegate byte ReadModifyWriteCallbackDelegate(ulong context, nint keyData, nuint keyLength, nuint writeLength, nint dataCallback, nint dataCallbackContext);

        private sealed class ContextAndKeyComparer : IEqualityComparer<(ulong Context, byte[] Data)>
        {
            public bool Equals((ulong Context, byte[] Data) x, (ulong Context, byte[] Data) y)
            => x.Context == y.Context && x.Data.AsSpan().SequenceEqual(y.Data);
            public int GetHashCode([DisallowNull] (ulong Context, byte[] Data) obj)
            {
                HashCode hash = default;
                hash.Add(obj.Context);
                hash.AddBytes(obj.Data);

                return hash.ToHashCode();
            }
        }

        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        public void CheckInternalId()
        {
            const ulong Context = 8;

            ConcurrentDictionary<(ulong Context, byte[] Key), byte[]> data = new(new ContextAndKeyComparer());

            unsafe void ReadCallback(
                ulong context,
                uint numKeys,
                nint keysData,
                nuint keysLength,
                nint dataCallback,
                nint dataCallbackContext
            )
            {
                var keyDataSpan = MemoryMarshal.CreateReadOnlySpan(ref Unsafe.AsRef<byte>((byte*)keysData), (int)keysLength);

                var remainingKeyDataSpan = keyDataSpan;
                var dataCallbackDel = (delegate* unmanaged[Cdecl, SuppressGCTransition]<int, nint, nint, nuint, void>)dataCallback;

                for (var index = 0; index < numKeys; index++)
                {
                    var keyLen = BinaryPrimitives.ReadInt32LittleEndian(remainingKeyDataSpan);
                    var keyData = remainingKeyDataSpan.Slice(sizeof(int), keyLen);

                    remainingKeyDataSpan = remainingKeyDataSpan[(sizeof(int) + keyLen)..];

                    var lookup = (context, keyData.ToArray());
                    if (data.TryGetValue(lookup, out var res))
                    {
                        fixed (byte* resPtr = res)
                        {
                            dataCallbackDel(index, dataCallbackContext, (nint)resPtr, (nuint)res.Length);
                        }
                    }
                }
            }

            unsafe byte WriteCallback(ulong context, nint keyData, nuint keyLength, nint writeData, nuint writeLength)
            {
                var keyDataSpan = MemoryMarshal.CreateReadOnlySpan(ref Unsafe.AsRef<byte>((byte*)keyData), (int)keyLength);
                var writeDataSpan = MemoryMarshal.CreateReadOnlySpan(ref Unsafe.AsRef<byte>((byte*)writeData), (int)writeLength);

                var lookup = (context, keyDataSpan.ToArray());

                data[lookup] = writeDataSpan.ToArray();

                return 1;
            }

            unsafe byte DeleteCallback(ulong context, nint keyData, nuint keyLength)
            {
                var keyDataSpan = MemoryMarshal.CreateReadOnlySpan(ref Unsafe.AsRef<byte>((byte*)keyData), (int)keyLength);

                var lookup = (context, keyDataSpan.ToArray());

                if (data.TryRemove(lookup, out _))
                {
                    return 1;
                }

                return 0;
            }

            unsafe byte ReadModifyWriteCallback(ulong context, nint keyData, nuint keyLength, nuint writeLength, nint callback, nint callbackContext)
            {
                var keyDataSpan = MemoryMarshal.CreateReadOnlySpan(ref Unsafe.AsRef<byte>((byte*)keyData), (int)keyLength);

                var lookup = (context, keyDataSpan.ToArray());

                var callbackDel = (delegate* unmanaged[Cdecl, SuppressGCTransition]<nint, nint, nuint, void>)callback;

                _ = data.AddOrUpdate(
                    lookup,
                    key =>
                    {
                        var ret = new byte[writeLength];
                        fixed (byte* retPtr = ret)
                        {
                            callbackDel(callbackContext, (nint)retPtr, (nuint)ret.Length);
                        }

                        return ret;
                    },
                    (key, old) =>
                    {
                        // Garnet guarantees no concurrent RMW update same value, but ConcurrentDictionary doesn't; so use a lock
                        lock (old)
                        {
                            fixed (byte* oldPtr = old)
                            {
                                callbackDel(callbackContext, (nint)oldPtr, (nuint)old.Length);
                            }

                            return old;
                        }
                    }
                );

                return 1;
            }

            ReadCallbackDelegate readDel = ReadCallback;
            WriteCallbackDelegate writeDel = WriteCallback;
            DeleteCallbackDelegate deleteDel = DeleteCallback;
            ReadModifyWriteCallbackDelegate rmwDel = ReadModifyWriteCallback;

            var readFuncPtr = Marshal.GetFunctionPointerForDelegate(readDel);
            var writeFuncPtr = Marshal.GetFunctionPointerForDelegate(writeDel);
            var deleteFuncPtr = Marshal.GetFunctionPointerForDelegate(deleteDel);
            var rmwFuncPtr = Marshal.GetFunctionPointerForDelegate(rmwDel);

            var rawIndex = NativeDiskANNMethods.create_index(Context, 75, 0, VectorQuantType.XPreQ8, 10, 10, readFuncPtr, writeFuncPtr, deleteFuncPtr, rmwFuncPtr);

            Span<byte> id = [0, 1, 2, 3];
            Span<byte> elem = Enumerable.Range(0, 75).Select(static x => (byte)x).ToArray();
            Span<byte> attr = [];

            // Insert
            unsafe
            {
                var insertRes = NativeDiskANNMethods.insert(Context, rawIndex, (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(id)), (nuint)id.Length, VectorValueType.XB8, (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(elem)), (nuint)elem.Length, (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(attr)), (nuint)attr.Length);
                ClassicAssert.AreEqual(1, insertRes);
            }

            // Check valid initially
            var internalId = data[(Context | DiskANNService.InternalIdMap, id.ToArray())];
            unsafe
            {
                var validRes = NativeDiskANNMethods.check_internal_id_valid(Context, rawIndex, (nint)Unsafe.AsPointer(ref internalId[0]), (nuint)internalId.Length);
                ClassicAssert.AreEqual(1, validRes);
            }

            // Remove
            unsafe
            {
                var numRes =
                    NativeDiskANNMethods.remove(
                        Context, rawIndex,
                        (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(id)), (nuint)id.Length
                    );
                ClassicAssert.AreEqual(1, numRes);
            }

            // Check no longer valid
            unsafe
            {
                var validRes = NativeDiskANNMethods.check_internal_id_valid(Context, rawIndex, (nint)Unsafe.AsPointer(ref internalId[0]), (nuint)internalId.Length);
                ClassicAssert.AreEqual(0, validRes);
            }

            GC.KeepAlive(deleteDel);
            GC.KeepAlive(writeDel);
            GC.KeepAlive(readDel);
            GC.KeepAlive(rmwDel);
        }


        [Test]
        public void VADD()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var res1 = db.Execute("VADD", ["foo", "VALUES", "4", "1.0", "1.0", "1.0", "1.0", new byte[] { 1, 0, 0, 0 }, "EF", "128", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = db.Execute("VADD", ["foo", "VALUES", "4", "2.0", "2.0", "2.0", "2.0", new byte[] { 2, 0, 0, 0 }, "EF", "128", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res2);
        }

        [Test]
        public void VSIM()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var res1 = db.Execute("VADD", ["foo", "VALUES", "4", "1.0", "1.0", "1.0", "1.0", new byte[] { 1, 0, 0, 0 }, "EF", "128", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res2 = db.Execute("VADD", ["foo", "VALUES", "4", "2.0", "2.0", "2.0", "2.0", new byte[] { 2, 0, 0, 0 }, "EF", "128", "M", "32"]);
            ClassicAssert.AreEqual(1, (int)res1);

            var res3 = (byte[][])db.Execute("VSIM", ["foo", "VALUES", "4", "0.0", "0.0", "0.0", "0.0", "COUNT", "5", "EF", "128"]);
            ClassicAssert.AreEqual(2, res3.Length);
            ClassicAssert.IsTrue(res3.Any(static x => x.SequenceEqual(new byte[] { 1, 0, 0, 0 })));
            ClassicAssert.IsTrue(res3.Any(static x => x.SequenceEqual(new byte[] { 2, 0, 0, 0 })));

            var res4 = (byte[][])db.Execute("VSIM", ["foo", "ELE", new byte[] { 1, 0, 0, 0 }, "COUNT", "5", "EF", "128"]);
            ClassicAssert.AreEqual(2, res4.Length);
            ClassicAssert.IsTrue(res4.Any(static x => x.SequenceEqual(new byte[] { 1, 0, 0, 0 })));
            ClassicAssert.IsTrue(res4.Any(static x => x.SequenceEqual(new byte[] { 2, 0, 0, 0 })));
        }

        [Test]
        public void Recreate()
        {
            const ulong Context = 8;

            ConcurrentDictionary<(ulong Context, byte[] Key), byte[]> data = new(new ContextAndKeyComparer());

            unsafe void ReadCallback(
                ulong context,
                uint numKeys,
                nint keysData,
                nuint keysLength,
                nint dataCallback,
                nint dataCallbackContext
            )
            {
                var keyDataSpan = MemoryMarshal.CreateReadOnlySpan(ref Unsafe.AsRef<byte>((byte*)keysData), (int)keysLength);

                var remainingKeyDataSpan = keyDataSpan;
                var dataCallbackDel = (delegate* unmanaged[Cdecl, SuppressGCTransition]<int, nint, nint, nuint, void>)dataCallback;

                for (var index = 0; index < numKeys; index++)
                {
                    var keyLen = BinaryPrimitives.ReadInt32LittleEndian(remainingKeyDataSpan);
                    var keyData = remainingKeyDataSpan.Slice(sizeof(int), keyLen);

                    remainingKeyDataSpan = remainingKeyDataSpan[(sizeof(int) + keyLen)..];

                    var lookup = (context, keyData.ToArray());
                    if (data.TryGetValue(lookup, out var res))
                    {
                        fixed (byte* resPtr = res)
                        {
                            dataCallbackDel(index, dataCallbackContext, (nint)resPtr, (nuint)res.Length);
                        }
                    }
                }
            }

            unsafe byte WriteCallback(ulong context, nint keyData, nuint keyLength, nint writeData, nuint writeLength)
            {
                var keyDataSpan = MemoryMarshal.CreateReadOnlySpan(ref Unsafe.AsRef<byte>((byte*)keyData), (int)keyLength);
                var writeDataSpan = MemoryMarshal.CreateReadOnlySpan(ref Unsafe.AsRef<byte>((byte*)writeData), (int)writeLength);

                var lookup = (context, keyDataSpan.ToArray());

                data[lookup] = writeDataSpan.ToArray();

                return 1;
            }

            unsafe byte DeleteCallback(ulong context, nint keyData, nuint keyLength)
            {
                var keyDataSpan = MemoryMarshal.CreateReadOnlySpan(ref Unsafe.AsRef<byte>((byte*)keyData), (int)keyLength);

                var lookup = (context, keyDataSpan.ToArray());

                if (data.TryRemove(lookup, out _))
                {
                    return 1;
                }

                return 0;
            }

            unsafe byte ReadModifyWriteCallback(ulong context, nint keyData, nuint keyLength, nuint writeLength, nint callback, nint callbackContext)
            {
                var keyDataSpan = MemoryMarshal.CreateReadOnlySpan(ref Unsafe.AsRef<byte>((byte*)keyData), (int)keyLength);

                var lookup = (context, keyDataSpan.ToArray());

                var callbackDel = (delegate* unmanaged[Cdecl, SuppressGCTransition]<nint, nint, nuint, void>)callback;

                _ = data.AddOrUpdate(
                    lookup,
                    key =>
                    {
                        var ret = new byte[writeLength];
                        fixed (byte* retPtr = ret)
                        {
                            callbackDel(callbackContext, (nint)retPtr, (nuint)ret.Length);
                        }

                        return ret;
                    },
                    (key, old) =>
                    {
                        // Garnet guarantees no concurrent RMW update same value, but ConcurrentDictionary doesn't; so use a lock
                        lock (old)
                        {
                            fixed (byte* oldPtr = old)
                            {
                                callbackDel(callbackContext, (nint)oldPtr, (nuint)old.Length);
                            }

                            return old;
                        }
                    }
                );

                return 1;
            }

            ReadCallbackDelegate readDel = ReadCallback;
            WriteCallbackDelegate writeDel = WriteCallback;
            DeleteCallbackDelegate deleteDel = DeleteCallback;
            ReadModifyWriteCallbackDelegate rmwDel = ReadModifyWriteCallback;

            var readFuncPtr = Marshal.GetFunctionPointerForDelegate(readDel);
            var writeFuncPtr = Marshal.GetFunctionPointerForDelegate(writeDel);
            var deleteFuncPtr = Marshal.GetFunctionPointerForDelegate(deleteDel);
            var rmwFuncPtr = Marshal.GetFunctionPointerForDelegate(rmwDel);

            var rawIndex = NativeDiskANNMethods.create_index(Context, 75, 0, VectorQuantType.XPreQ8, 10, 10, readFuncPtr, writeFuncPtr, deleteFuncPtr, rmwFuncPtr);

            Span<byte> id = [0, 1, 2, 3];
            Span<byte> elem = Enumerable.Range(0, 75).Select(static x => (byte)x).ToArray();
            Span<byte> attr = [];

            // Insert
            unsafe
            {
                var insertRes = NativeDiskANNMethods.insert(Context, rawIndex, (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(id)), (nuint)id.Length, VectorValueType.XB8, (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(elem)), (nuint)elem.Length, (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(attr)), (nuint)attr.Length);
                ClassicAssert.AreEqual(1, insertRes);
            }

            Span<byte> filter = [];

            // Search
            unsafe
            {
                Span<byte> outputIds = stackalloc byte[1024];
                Span<float> outputDistances = stackalloc float[64];

                nint continuation = 0;

                var numRes =
                    NativeDiskANNMethods.search_vector(
                        Context, rawIndex,
                        VectorValueType.XB8, (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(elem)), (nuint)elem.Length,
                        1f, outputDistances.Length, // SearchExplorationFactor must >= Count
                        (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(filter)), (nuint)filter.Length,
                        0,
                        (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(outputIds)), (nuint)outputIds.Length,
                        (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(outputDistances)), (nuint)outputDistances.Length,
                        (nint)Unsafe.AsPointer(ref continuation)
                    );
                ClassicAssert.AreEqual(1, numRes);

                var firstResLen = BinaryPrimitives.ReadInt32LittleEndian(outputIds);
                var firstRes = outputIds.Slice(sizeof(int), firstResLen);
                ClassicAssert.IsTrue(firstRes.SequenceEqual(id));
            }

            // Drop does not cleanup data, so use it to simulate a process stop and recreate
            {
                NativeDiskANNMethods.drop_index(Context, rawIndex);

                rawIndex = NativeDiskANNMethods.create_index(Context, 75, 0, VectorQuantType.XPreQ8, 10, 10, readFuncPtr, writeFuncPtr, deleteFuncPtr, rmwFuncPtr);
            }

            // Search value
            unsafe
            {
                Span<byte> outputIds = stackalloc byte[1024];
                Span<float> outputDistances = stackalloc float[64];

                nint continuation = 0;

                var numRes =
                    NativeDiskANNMethods.search_vector(
                        Context, rawIndex,
                        VectorValueType.XB8, (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(elem)), (nuint)elem.Length,
                        1f, outputDistances.Length, // SearchExplorationFactor must >= Count
                        (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(filter)), (nuint)filter.Length,
                        0,
                        (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(outputIds)), (nuint)outputIds.Length,
                        (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(outputDistances)), (nuint)outputDistances.Length,
                        (nint)Unsafe.AsPointer(ref continuation)
                    );
                ClassicAssert.AreEqual(1, numRes);

                var firstResLen = BinaryPrimitives.ReadInt32LittleEndian(outputIds);
                var firstRes = outputIds.Slice(sizeof(int), firstResLen);
                ClassicAssert.IsTrue(firstRes.SequenceEqual(id));
            }

            // Search element
            unsafe
            {
                Span<byte> outputIds = stackalloc byte[1024];
                Span<float> outputDistances = stackalloc float[64];

                nint continuation = 0;

                var numRes =
                    NativeDiskANNMethods.search_element(
                        Context, rawIndex,
                        (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(id)), (nuint)id.Length,
                        1f, outputDistances.Length, // SearchExplorationFactor must >= Count
                        (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(filter)), (nuint)filter.Length,
                        0,
                        (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(outputIds)), (nuint)outputIds.Length,
                        (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(outputDistances)), (nuint)outputDistances.Length,
                        (nint)Unsafe.AsPointer(ref continuation)
                    );
                ClassicAssert.AreEqual(1, numRes);

                var firstResLen = BinaryPrimitives.ReadInt32LittleEndian(outputIds);
                var firstRes = outputIds.Slice(sizeof(int), firstResLen);
                ClassicAssert.IsTrue(firstRes.SequenceEqual(id));
            }

            // Remove
            unsafe
            {
                var numRes =
                    NativeDiskANNMethods.remove(
                        Context, rawIndex,
                        (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(id)), (nuint)id.Length
                    );
                ClassicAssert.AreEqual(1, numRes);
            }

            // Insert
            unsafe
            {
                Span<byte> id2 = [4, 5, 6, 7];
                Span<byte> elem2 = Enumerable.Range(0, 75).Select(static x => (byte)(x * 2)).ToArray();
                ReadOnlySpan<byte> attr2 = "{\"foo\": \"bar\"}"u8;

                var insertRes = NativeDiskANNMethods.insert(
                    Context, rawIndex,
                    (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(id2)), (nuint)id2.Length,
                    VectorValueType.XB8, (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(elem2)), (nuint)elem2.Length,
                    (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(attr2)), (nuint)attr2.Length
                );
                ClassicAssert.AreEqual(1, insertRes);
            }

            GC.KeepAlive(deleteDel);
            GC.KeepAlive(writeDel);
            GC.KeepAlive(readDel);
            GC.KeepAlive(rmwDel);
        }
    }
}