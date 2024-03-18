// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
#if WINDOWS
using System.Runtime.InteropServices;
#endif
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using Tsavorite.core;
using Tsavorite.devices;

namespace Tsavorite.test
{
    public static class TestUtils
    {
        // Various categories used to group tests
        internal const string SmokeTestCategory = "Smoke";
        internal const string StressTestCategory = "Stress";
        internal const string TsavoriteKVTestCategory = "TsavoriteKV";
        internal const string ReadTestCategory = "Read";
        internal const string LockableUnsafeContextTestCategory = "LockableUnsafeContext";
        internal const string ReadCacheTestCategory = "ReadCache";
        internal const string LockTestCategory = "Locking";
        internal const string LockTableTestCategory = "LockTable";
        internal const string CheckpointRestoreCategory = "CheckpointRestore";
        internal const string MallocFixedPageSizeCategory = "MallocFixedPageSize";
        internal const string RMWTestCategory = "RMW";
        internal const string ModifiedBitTestCategory = "ModifiedBitTest";
        internal const string RevivificationCategory = "Revivification";

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
#if WINDOWS
            LSD,
#endif
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
#if WINDOWS
                case DeviceType.LSD:
                    bool useIoCompletionPort = false;
                    bool disableFileBuffering = true;
#if NETSTANDARD || NET
                    if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))    // avoids CA1416 // Validate platform compatibility
#endif
                        device = new LocalStorageDevice(filename, preallocateFile, deleteOnClose, disableFileBuffering, capacity, recoverDevice, useIoCompletionPort);
                    break;
#endif
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

        public enum AllocatorType
        {
            FixedBlittable,
            SpanByte,
            Generic
        }

        internal enum SyncMode { Sync, Async }

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

        internal static (Status status, TOutput output) GetSinglePendingResult<TKey, TValue, TInput, TOutput, TContext>(CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext> completedOutputs)
            => GetSinglePendingResult(completedOutputs, out _);

        internal static (Status status, TOutput output) GetSinglePendingResult<TKey, TValue, TInput, TOutput, TContext>(CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext> completedOutputs, out RecordMetadata recordMetadata)
        {
            Assert.IsTrue(completedOutputs.Next());
            var result = (completedOutputs.Current.Status, completedOutputs.Current.Output);
            recordMetadata = completedOutputs.Current.RecordMetadata;
            Assert.IsFalse(completedOutputs.Next());
            completedOutputs.Dispose();
            return result;
        }

        internal static async ValueTask<(Status status, Output output)> CompleteAsync<Key, Value, Input, Output, Context>(ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> resultTask)
        {
            var readCompleter = await resultTask;
            return readCompleter.Complete();
        }

        internal static async ValueTask<Status> CompleteAsync<Key, Value, Context>(ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Key, Value, Context>> resultTask)
        {
            var result = await resultTask;
            while (result.Status.IsPending)
                result = await result.CompleteAsync().ConfigureAwait(false);
            return result.Status;
        }

        internal static async ValueTask<Status> CompleteAsync<Key, Value, Context>(ValueTask<TsavoriteKV<Key, Value>.RmwAsyncResult<Value, Value, Context>> resultTask)
        {
            var result = await resultTask;
            while (result.Status.IsPending)
                result = await result.CompleteAsync().ConfigureAwait(false);
            return result.Status;
        }

        internal static async ValueTask<Status> CompleteAsync<Key, Value, Input, Output, Context>(ValueTask<TsavoriteKV<Key, Value>.DeleteAsyncResult<Input, Output, Context>> resultTask)
        {
            var deleteCompleter = await resultTask;
            return deleteCompleter.Complete();
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

        internal static unsafe bool FindHashBucketEntryForKey<Key, Value>(this TsavoriteKV<Key, Value> store, ref Key key, out HashBucketEntry entry)
        {
            HashEntryInfo hei = new(store.Comparer.GetHashCode64(ref key));
            var success = store.FindTag(ref hei);
            entry = hei.entry;
            return success;
        }
    }
}