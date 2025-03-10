// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using Tsavorite.devices;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    public static class TestUtils
    {
        // Various categories used to group tests
        internal const string SmokeTestCategory = "Smoke";
        internal const string StressTestCategory = "Stress";
        internal const string TsavoriteKVTestCategory = "TsavoriteKV";
        internal const string ReadTestCategory = "Read";
        internal const string TransactionalUnsafeContextTestCategory = "TransactionalUnsafeContext";
        internal const string ReadCacheTestCategory = "ReadCache";
        internal const string LockTestCategory = "Locking";
        internal const string LockTableTestCategory = "LockTable";
        internal const string CheckpointRestoreCategory = "CheckpointRestore";
        internal const string MallocFixedPageSizeCategory = "MallocFixedPageSize";
        internal const string RMWTestCategory = "RMW";
        internal const string IteratorCategory = "Iterator";
        internal const string ModifiedBitTestCategory = "ModifiedBitTest";
        internal const string RevivificationCategory = "Revivification";
        internal const string MultiLevelPageArrayCategory = "MultiLevelPageArray";
        internal const string ObjectIdMapCategory = "ObjectIdMap";
        internal const string OverflowAllocatorCategory = "OverflowAllocator";

        public static ILoggerFactory TestLoggerFactory = CreateLoggerFactoryInstance(TestContext.Progress, LogLevel.Trace);

        /// <summary>
        /// Delete a directory recursively
        /// </summary>
        /// <param name="path">The folder to delete</param>
        /// <param name="wait">If true, loop on exceptions that are retryable, and verify the directory no longer exists. Generally true on SetUp, false on TearDown</param>
        internal static void DeleteDirectory(string path, bool wait = false)
        {
            while (true)
            {
                try
                {
                    if (!Directory.Exists(path))
                        return;
                    foreach (string directory in Directory.GetDirectories(path))
                        DeleteDirectory(directory, wait);
                    break;
                }
                catch
                {
                }
            }

            for (; ; Thread.Yield())
            {
                // Exceptions may happen due to a handle briefly remaining held after Dispose().
                try
                {
                    Directory.Delete(path, true);
                }
                catch (Exception ex) when (ex is IOException ||
                                           ex is UnauthorizedAccessException)
                {
                }
                if (!wait || !Directory.Exists(path))
                    break;
            }
        }

        /// <summary>
        /// Create a clean new directory, removing a previous one if needed.
        /// </summary>
        /// <param name="path"></param>
        internal static void RecreateDirectory(string path)
        {
            if (Directory.Exists(path))
                DeleteDirectory(path);

            // Don't catch; if this fails, so should the test
            Directory.CreateDirectory(path);
        }

        /// <summary>
        /// Create logger factory for given TextWriter and loglevel
        /// E.g. Use with TestContext.Progress to print logs while test is running.
        /// </summary>
        /// <param name="textWriter"></param>
        /// <param name="logLevel"></param>
        /// <param name="scope"></param>
        /// <returns></returns>
        public static ILoggerFactory CreateLoggerFactoryInstance(TextWriter textWriter, LogLevel logLevel, string scope = "")
        {
            return LoggerFactory.Create(builder =>
            {
                builder.AddProvider(new NUnitLoggerProvider(textWriter, scope));
                builder.SetMinimumLevel(logLevel);
            });
        }

        internal static bool IsRunningAzureTests => "yes".Equals(Environment.GetEnvironmentVariable("RunAzureTests")) || "yes".Equals(Environment.GetEnvironmentVariable("RUNAZURETESTS"));

        internal static void IgnoreIfNotRunningAzureTests()
        {
            // Need this environment variable set AND Azure Storage Emulator running
            if (!IsRunningAzureTests)
                Assert.Ignore("Environment variable RunAzureTests is not defined");
        }

        // Used to test the various devices by using the same test with VALUES parameter
        // Cannot use LocalStorageDevice from non-Windows OS platform
        public enum DeviceType
        {
            LSD,
            EmulatedAzure,
            MLSD,
            LocalMemory
        }

        internal const int DefaultLocalMemoryDeviceLatencyMs = 20;   // latencyMs only applies to DeviceType = LocalMemory

        internal static IDevice CreateTestDevice(DeviceType testDeviceType, string filename, int latencyMs = DefaultLocalMemoryDeviceLatencyMs, bool deleteOnClose = false, bool omitSegmentIdFromFilename = false)
        {
            IDevice device = null;
            bool preallocateFile = false;
            long capacity = Devices.CAPACITY_UNSPECIFIED;
            bool recoverDevice = false;

            switch (testDeviceType)
            {
                case DeviceType.LSD when !OperatingSystem.IsWindows():
                    Assert.Ignore($"Skipping {nameof(DeviceType.LSD)} on non-Windows platforms");
                    break;
                case DeviceType.LSD when OperatingSystem.IsWindows():
                    bool useIoCompletionPort = false;
                    bool disableFileBuffering = true;
                    device = new LocalStorageDevice(filename, preallocateFile, deleteOnClose, disableFileBuffering, capacity, recoverDevice, useIoCompletionPort);
                    break;
                case DeviceType.EmulatedAzure:
                    IgnoreIfNotRunningAzureTests();
                    device = new AzureStorageDevice(AzureEmulatedStorageString, AzureTestContainer, AzureTestDirectory, Path.GetFileName(filename), deleteOnClose: deleteOnClose, logger: TestLoggerFactory.CreateLogger("asd"));
                    break;
                case DeviceType.MLSD:
                    device = new ManagedLocalStorageDevice(filename, preallocateFile, deleteOnClose, true, capacity, recoverDevice);
                    break;
                // Emulated higher latency storage device - takes a disk latency arg (latencyMs) and emulates an IDevice using main memory, serving data at specified latency
                case DeviceType.LocalMemory:
                    device = new LocalMemoryDevice(1L << 28, 1L << 25, 2, sector_size: 512, latencyMs: latencyMs);  // 64 MB (1L << 26) is enough for our test cases
                    break;
            }

            if (omitSegmentIdFromFilename)
                device.Initialize(segmentSize: -1L, omitSegmentIdFromFilename: omitSegmentIdFromFilename);
            return device;
        }

        private static string ConvertedClassName(bool forAzure = false)
        {
            // Make this all under one root folder named {prefix}, which is the base namespace name. All UT namespaces using this must start with this prefix.
            const string prefix = "Tsavorite.test";
            const string prefix2 = "NUnit.Framework.Internal.TestExecutionContext";

            if (TestContext.CurrentContext.Test.ClassName.StartsWith($"{prefix}."))
            {
                var suffix = TestContext.CurrentContext.Test.ClassName.Substring(prefix.Length + 1);
                return forAzure ? suffix : $"{prefix}/{suffix}";
            }
            else if (TestContext.CurrentContext.Test.ClassName.StartsWith($"{prefix2}+"))
            {
                var suffix = TestContext.CurrentContext.Test.ClassName.Substring(prefix2.Length + 1);
                return forAzure ? suffix : $"{prefix2}/{suffix}";
            }
            else
            {
                Assert.Fail($"Expected {prefix} prefix was not found");
                return "";
            }
        }

        // Tsavorite paths are too long; as a workaround (possibly temporary) remove the class name (many long test method names repeat much of the class name).
        //internal static string MethodTestDir => Path.Combine(TestContext.CurrentContext.TestDirectory, $"{ConvertedClassName()}_{TestContext.CurrentContext.Test.MethodName}");
        internal static string MethodTestDir => Path.Combine(TestContext.CurrentContext.TestDirectory, $"Tsavorite.test/{TestContext.CurrentContext.Test.MethodName}");

        internal static string AzureTestContainer
        {
            get
            {
                var container = ConvertedClassName(forAzure: true).Replace('.', '-').ToLower();
                NameValidator.ValidateContainerName(container);
                return container;
            }
        }

        internal static string AzureTestDirectory => TestContext.CurrentContext.Test.MethodName;

        internal const string AzureEmulatedStorageString = "UseDevelopmentStorage=true;";
        internal static AzureStorageNamedDeviceFactoryCreator AzureStorageNamedDeviceFactoryCreator = IsRunningAzureTests ? new(AzureEmulatedStorageString) : null;

        public enum AllocatorType
        {
            FixedBlittable,
            SpanByte,
            Generic
        }

        internal enum CompletionSyncMode { Sync, Async }

        public enum ReadCopyDestination { Tail, ReadCache }

        public enum FlushMode { NoFlush, ReadOnly, OnDisk }

        public enum KeyEquality { Equal, NotEqual }

        public enum ReadCacheMode { UseReadCache, NoReadCache }

        public enum KeyContentionMode { Contention, NoContention }

        public enum BatchMode { Batch, NoBatch }

        public enum UpdateOp { Upsert, RMW, Delete }

        public enum HashModulo { NoMod = 0, Hundred = 100, Thousand = 1000 }

        public enum ScanIteratorType { Pull, Push }

        public enum ScanMode { Scan, Iterate }

        public enum WaitMode { Wait, NoWait }

        internal static (Status status, TOutput output) GetSinglePendingResult<TValue, TInput, TOutput, TContext>(CompletedOutputIterator<TValue, TInput, TOutput, TContext> completedOutputs)
            => GetSinglePendingResult(completedOutputs, out _);

        internal static (Status status, TOutput output) GetSinglePendingResult<TValue, TInput, TOutput, TContext>(CompletedOutputIterator<TValue, TInput, TOutput, TContext> completedOutputs, out RecordMetadata recordMetadata)
        {
            ClassicAssert.IsTrue(completedOutputs.Next());
            var result = (completedOutputs.Current.Status, completedOutputs.Current.Output);
            recordMetadata = completedOutputs.Current.RecordMetadata;
            ClassicAssert.IsFalse(completedOutputs.Next());
            completedOutputs.Dispose();
            return result;
        }

        internal static async ValueTask DoTwoThreadRandomKeyTest(int count, bool doRandom, Action<int> first, Action<int> second, Action<int> verification)
        {
            Task[] tasks = new Task[2];

            var rng = new Random(101);
            for (var iter = 0; iter < count; ++iter)
            {
                var arg = doRandom ? rng.Next(count) : iter;
                tasks[0] = Task.Factory.StartNew(() => first(arg));
                tasks[1] = Task.Factory.StartNew(() => second(arg));

                await Task.WhenAll(tasks);

                verification(arg);
            }
        }

        internal static unsafe bool FindHashBucketEntryForKey<TValue, TStoreFunctions, TAllocator>(this TsavoriteKV<TValue, TStoreFunctions, TAllocator> store, SpanByte key, out HashBucketEntry entry)
            where TStoreFunctions : IStoreFunctions<TValue>
            where TAllocator : IAllocator<TValue, TStoreFunctions>
        {
            HashEntryInfo hei = new(store.storeFunctions.GetKeyHashCode64(key));
            var success = store.FindTag(ref hei);
            entry = hei.entry;
            return success;
        }

        internal static unsafe SpanByte SpanByteFrom<T>(ref T localVar) where T : unmanaged
            => new (Unsafe.SizeOf<T>(), (IntPtr)Unsafe.AsPointer(ref localVar));
    }

    /// <summary>Deterministic equality comparer for ints</summary>
    public sealed class IntKeyComparer : IKeyComparer
    {
        /// <summary>
        /// The default instance.
        /// </summary>
        /// <remarks>Used to avoid allocating new comparers.</remarks>
        public static readonly IntKeyComparer Instance = new();

        /// <inheritdoc />
        public bool Equals(SpanByte k1, SpanByte k2) => k1.AsRef<int>() == k2.AsRef<int>();

        /// <inheritdoc />
        public long GetHashCode64(SpanByte k) => Utility.GetHashCode(k.AsRef<int>());
    }

    /// <summary>Deterministic equality comparer for longs</summary>
    public sealed class LongKeyComparer : IKeyComparer
    {
        /// <summary>
        /// The default instance.
        /// </summary>
        /// <remarks>Used to avoid allocating new comparers.</remarks>
        public static readonly LongKeyComparer Instance = new();

        /// <inheritdoc />
        public bool Equals(SpanByte k1, SpanByte k2) => k1.AsRef<long>() == k2.AsRef<long>();

        /// <inheritdoc />
        public long GetHashCode64(SpanByte k) => Utility.GetHashCode(k.AsRef<long>());
    }

    /// <summary>Deterministic equality comparer for longs with hash modulo</summary>
    internal class LongKeyComparerModulo : IKeyComparer
    {
        internal long mod;

        internal LongKeyComparerModulo(long mod) => this.mod = mod;

        public bool Equals(SpanByte k1, SpanByte k2) => k1.AsRef<long>() == k2.AsRef<long>();

        public long GetHashCode64(SpanByte k) => mod == 0 ? k.AsRef<long>() : k.AsRef<long>() % mod;
    }

    /// <summary>Deterministic equality comparer for SpanBytes with hash modulo</summary>
    internal struct SpanByteKeyComparerModulo : IKeyComparer
    {
        readonly HashModulo modRange;

        internal SpanByteKeyComparerModulo(HashModulo mod) => modRange = mod;

        public readonly bool Equals(SpanByte k1, SpanByte k2) => SpanByteComparer.StaticEquals(k1, k2);

        // Force collisions to create a chain
        public readonly long GetHashCode64(SpanByte k)
        {
            var value = SpanByteComparer.StaticGetHashCode64(k);
            return modRange != HashModulo.NoMod ? value % (long)modRange : value;
        }
    }

    static class StaticTestUtils
    {
        internal static (Status status, TOutput output) GetSinglePendingResult<TValue, TInput, TOutput, TContext, Functions, TStoreFunctions, TAllocator>(
                this ITsavoriteContext<TValue, TInput, TOutput, TContext, Functions, TStoreFunctions, TAllocator> sessionContext)
            where Functions : ISessionFunctions<TValue, TInput, TOutput, TContext>
            where TStoreFunctions : IStoreFunctions<TValue>
            where TAllocator : IAllocator<TValue, TStoreFunctions>
            => sessionContext.GetSinglePendingResult(out _);

        internal static (Status status, TOutput output) GetSinglePendingResult<TValue, TInput, TOutput, TContext, Functions, TStoreFunctions, TAllocator>(
                this ITsavoriteContext<TValue, TInput, TOutput, TContext, Functions, TStoreFunctions, TAllocator> sessionContext, out RecordMetadata recordMetadata)
            where Functions : ISessionFunctions<TValue, TInput, TOutput, TContext>
            where TStoreFunctions : IStoreFunctions<TValue>
            where TAllocator : IAllocator<TValue, TStoreFunctions>
        {
            _ = sessionContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            return TestUtils.GetSinglePendingResult(completedOutputs, out recordMetadata);
        }

        public static unsafe ref T AsRef<T>(this SpanByte spanByte) where T : unmanaged
        {
            ClassicAssert.IsTrue(spanByte.Length == sizeof(T));
            return ref *(T*)spanByte.ToPointer();
        }

        internal static unsafe SpanByte Set<T>(this SpanByte spanByte, T value) where T : unmanaged
        {
            ClassicAssert.IsTrue(spanByte.Length == sizeof(T));
            spanByte.AsRef<T>() = value;
            return spanByte;
        }
    }
}