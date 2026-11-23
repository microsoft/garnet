// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using System.Threading;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    using ObjectStoreFunctions = StoreFunctions<TestObjectKey.Comparer, FlushFailureTests.FailureRecordTriggers>;
    using SpanByteStoreFunctions = StoreFunctions<SpanByteComparer, SpanByteRecordTriggers>;

    /// <summary>
    /// An exception that escapes a page flush or close terminates the process, since the log cannot advance past the range it failed on.
    /// Each test runs its scenario in a child process, which the termination would otherwise end the test host with, and checks how the
    /// process ended and what it wrote.
    /// </summary>
    [TestFixture]
    public class FlushFailureTests : TestBase
    {
        /// <summary>The argument with which <see cref="ChildProcessProgram"/> runs a scenario.</summary>
        internal const string ChildArgument = "flush-failure";

        /// <summary>Where a scenario injects its failure.</summary>
        public enum Failure
        {
            /// <summary><see cref="IRecordTriggers.OnFlush"/> throws, in the object allocator's ReadOnly flush.</summary>
            OnFlush,

            /// <summary>The read-only observer throws, in the object allocator's ReadOnly flush.</summary>
            ObjectStoreObserver,

            /// <summary>The value serializer throws, in the object allocator's ReadOnly flush.</summary>
            Serialization,

            /// <summary>The read-only observer throws, in the ReadOnly flush of the other allocators, here a SpanByte store's.</summary>
            SpanByteStoreObserver,

            /// <summary><see cref="IRecordTriggers.OnEvict"/> throws, in a page close.</summary>
            OnEvict,

            /// <summary><see cref="IRecordTriggers.OnFlush"/> throws an exception whose <see cref="Exception.ToString"/> throws.</summary>
            Unformattable,
        }

        /// <summary>Exit code of a child to whose caller the failure escaped.</summary>
        const int ExitFailureEscaped = 3;

        /// <summary>Exit code of a child that was not terminated.</summary>
        const int ExitNotTerminated = 4;

        /// <summary>Prefix of the line in which a child reports the start and end of the range it flushes or closes.</summary>
        const string RangePrefix = "Range ";

        /// <summary>SEM_NOGPFAULTERRORBOX</summary>
        const uint NoGpFaultErrorBox = 0x0002;

        static readonly TimeSpan ChildTimeout = TimeSpan.FromSeconds(60);

        [DllImport("kernel32.dll")]
        static extern uint SetErrorMode(uint mode);

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            _ = Directory.CreateDirectory(MethodTestDir);
        }

        [TearDown]
        public void TearDown()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            OnTearDown();
        }

        /// <summary>
        /// An exception from a ReadOnly flush or a page close, whether a record trigger, the read-only observer or the value serializer
        /// throws it, terminates the process with a message that names the operation, the log addresses it failed on and the exception,
        /// even if the exception cannot format itself. The child catches exceptions from the operation it triggers, as Garnet's network
        /// handler does, so an exception that escaped would be reported rather than end the child.
        /// </summary>
        [Test]
        [Category(TsavoriteKVTestCategory)]
        public void FailureTerminatesProcess([Values] Failure failure)
        {
            var (exitCode, output, errors) = RunChildProcess(failure);
            var report = $"Exit code {exitCode}{Environment.NewLine}Output:{Environment.NewLine}{output}{Environment.NewLine}Errors:{Environment.NewLine}{errors}";

            Assert.That(exitCode, Is.Not.EqualTo(0).And.Not.EqualTo(ExitFailureEscaped).And.Not.EqualTo(ExitNotTerminated), report);

            var range = Regex.Match(output, $@"^{RangePrefix}(\d+) (\d+)", RegexOptions.Multiline);
            Assert.That(range.Success, Is.True, report);
            var (start, end) = (range.Groups[1].Value, range.Groups[2].Value);

            var state = failure switch
            {
                Failure.SpanByteStoreObserver => $"OnPagesMarkedReadOnly failed; allocator state is unrecoverable. Range [{start}, {end}) may not be flushed. FlushedUntilAddress={start}",
                Failure.OnEvict => $"OnPagesClosedWorker failed; allocator state is unrecoverable. ClosedUntilAddress={start}, OngoingCloseUntilAddress={end}",
                _ => $"OnPagesMarkedReadOnlyWorker failed; allocator state is unrecoverable. LastIssuedFlushedUntilAddress={start}, OngoingFlushedUntilAddress={end}",
            };
            var exception = failure == Failure.Unformattable
                ? $"{typeof(UnformattableException).FullName}; its ToString() threw {typeof(InvalidOperationException).FullName}"
                : $"{typeof(InvalidOperationException).FullName}: {InjectedMessage(failure)}";
            Assert.That(errors, Does.Contain($"{state}{Environment.NewLine}Caught exception:{Environment.NewLine}{exception}"), report);
        }

        /// <summary>Runs <paramref name="failure"/> in a child process, and returns how the process ended and what it wrote.</summary>
        static (int ExitCode, string Output, string Errors) RunChildProcess(Failure failure)
        {
            var startInfo = new ProcessStartInfo(DotNetHostPath())
            {
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            };
            startInfo.ArgumentList.Add(typeof(FlushFailureTests).Assembly.Location);
            startInfo.ArgumentList.Add(ChildArgument);
            startInfo.ArgumentList.Add(failure.ToString());
            startInfo.ArgumentList.Add(MethodTestDir);

            // Collect-on-crash settings inherited from the test host would dump each intended termination.
            startInfo.Environment["DOTNET_DbgEnableMiniDump"] = "0";

            using var process = Process.Start(startInfo);
            var output = process.StandardOutput.ReadToEndAsync();
            var errors = process.StandardError.ReadToEndAsync();
            if (!process.WaitForExit(ChildTimeout))
            {
                process.Kill(entireProcessTree: true);
                process.WaitForExit();
                Assert.Fail($"The child process did not exit within {ChildTimeout}.{Environment.NewLine}Output:{Environment.NewLine}{output.Result}"
                    + $"{Environment.NewLine}Errors:{Environment.NewLine}{errors.Result}");
            }

            return (process.ExitCode, output.Result, errors.Result);
        }

        /// <summary>
        /// The dotnet host of the runtime that runs this process. On Windows the test host is an apphost rather than dotnet, so the host is
        /// found from the runtime's directory, which is &lt;dotnet root&gt;/shared/Microsoft.NETCore.App/&lt;version&gt;/.
        /// </summary>
        static string DotNetHostPath()
        {
            var root = Path.GetFullPath(Path.Combine(RuntimeEnvironment.GetRuntimeDirectory(), "..", "..", ".."));
            var host = Path.Combine(root, OperatingSystem.IsWindows() ? "dotnet.exe" : "dotnet");
            return File.Exists(host) ? host : "dotnet";
        }

        /// <summary>
        /// Runs <paramref name="failure"/> in this process, a child that the failure should terminate. Returns an exit code only if it did not.
        /// </summary>
        internal static int RunChild(Failure failure, string directory)
        {
            // Suppresses the Windows Error Reporting dialog, without which each termination takes seconds rather than a fraction of one.
            if (OperatingSystem.IsWindows())
                _ = SetErrorMode(NoGpFaultErrorBox);

            var trigger = failure == Failure.SpanByteStoreObserver ? PrepareSpanByteStore(directory) : PrepareObjectStore(failure, directory);
            try
            {
                trigger();
            }
            catch (Exception ex)
            {
                // The type only, since the injected exception may not format itself.
                Console.WriteLine($"The failure escaped to the caller: {ex.GetType().FullName}");
                return ExitFailureEscaped;
            }

            // A flush or close that this thread did not drain runs on another thread; give it time to terminate the process.
            Thread.Sleep(TimeSpan.FromSeconds(10));
            Console.WriteLine("The process was not terminated");
            return ExitNotTerminated;
        }

        /// <summary>
        /// Creates an object store that injects <paramref name="failure"/> and holds a few records, and returns the operation that flushes
        /// them, or for <see cref="Failure.OnEvict"/> flushes and closes them.
        /// </summary>
        static Action PrepareObjectStore(Failure failure, string directory)
        {
            var store = new TsavoriteKV<ObjectStoreFunctions, ObjectAllocator<ObjectStoreFunctions>>(new()
            {
                IndexSize = 1L << 13,
                LogDevice = Devices.CreateLogDevice(Path.Join(directory, "hlog.log"), deleteOnClose: true),
                ObjectLogDevice = Devices.CreateLogDevice(Path.Join(directory, "hlog.obj.log"), deleteOnClose: true),
                LogMemorySize = 1L << 16,
                PageSize = 1L << 12,
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new FailureSerializer(failure), new FailureRecordTriggers(failure)),
                (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

            if (failure == Failure.ObjectStoreObserver)
                _ = store.Log.Subscribe(new FailureObserver(failure));

            using (var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete>(new TestObjectFunctionsDelete()))
            {
                // Few enough to stay on the first page, so that nothing is flushed before the trigger.
                var bContext = session.BasicContext;
                for (var i = 0; i < 3; i++)
                    _ = bContext.Upsert(new TestObjectKey { key = i }, new TestObjectValue { value = i }, 0);
            }

            var tailAddress = store.Log.TailAddress;
            if (failure == Failure.OnEvict)
            {
                ReportRange(store.Log.HeadAddress, tailAddress);
                return () => store.Log.ShiftHeadAddress(tailAddress, wait: false);
            }

            ReportRange(store.Log.FlushedUntilAddress, tailAddress);
            return () => store.Log.ShiftReadOnlyAddress(tailAddress, wait: false);
        }

        /// <summary>
        /// Creates a SpanByte store whose read-only observer throws and that holds a few records, and returns the operation that flushes them.
        /// </summary>
        static Action PrepareSpanByteStore(string directory)
        {
            var store = new TsavoriteKV<SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>>(new()
            {
                IndexSize = 1L << 13,
                LogDevice = Devices.CreateLogDevice(Path.Join(directory, "hlog.log"), deleteOnClose: true),
                LogMemorySize = 1L << 16,
                PageSize = 1L << 12,
            }, StoreFunctions.Create(SpanByteComparer.Instance, SpanByteRecordTriggers.Instance),
                (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));
            _ = store.Log.Subscribe(new FailureObserver(Failure.SpanByteStoreObserver));

            using (var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions()))
            {
                var bContext = session.BasicContext;
                Span<int> keyAndValue = stackalloc int[1];
                for (var i = 0; i < 3; i++)
                {
                    keyAndValue[0] = i;
                    var bytes = MemoryMarshal.Cast<int, byte>(keyAndValue);
                    _ = bContext.Upsert(TestSpanByteKey.FromPinnedSpan(bytes), bytes, Empty.Default);
                }
            }

            var tailAddress = store.Log.TailAddress;
            ReportRange(store.Log.FlushedUntilAddress, tailAddress);
            return () => store.Log.ShiftReadOnlyAddress(tailAddress, wait: false);
        }

        static void ReportRange(long start, long end) => Console.WriteLine($"{RangePrefix}{start} {end}");

        static string InjectedMessage(Failure failure) => $"Injected {failure} failure";

        static InvalidOperationException Injected(Failure failure) => new(InjectedMessage(failure));

        /// <summary>Record triggers whose <see cref="IRecordTriggers.OnFlush"/> or <see cref="IRecordTriggers.OnEvict"/> throws.</summary>
        internal readonly struct FailureRecordTriggers : IRecordTriggers
        {
            readonly Failure failure;

            public FailureRecordTriggers(Failure failure) => this.failure = failure;

            public bool CallOnFlush => failure is Failure.OnFlush or Failure.Unformattable;

            public bool CallOnEvict => failure == Failure.OnEvict;

            public void OnFlush(ref LogRecord logRecord, long logicalAddress)
            {
                if (failure == Failure.Unformattable)
                    throw new UnformattableException();
                throw Injected(failure);
            }

            public void OnEvict(ref LogRecord logRecord, EvictionSource source) => throw Injected(failure);
        }

        /// <summary>Serializes <see cref="TestObjectValue"/>s, or for <see cref="Failure.Serialization"/> throws.</summary>
        sealed class FailureSerializer(Failure failure) : BinaryObjectSerializer<IHeapObject>
        {
            public override void Deserialize(out IHeapObject obj) => obj = new TestObjectValue { value = reader.ReadInt32() };

            public override void Serialize(IHeapObject obj)
            {
                if (failure == Failure.Serialization)
                    throw Injected(failure);
                writer.Write(((TestObjectValue)obj).value);
            }
        }

        /// <summary>A read-only observer that throws.</summary>
        sealed class FailureObserver(Failure failure) : IObserver<ITsavoriteScanIterator>
        {
            public void OnNext(ITsavoriteScanIterator iterator) => throw Injected(failure);

            public void OnCompleted() { }

            public void OnError(Exception error) { }
        }

        /// <summary>An exception whose <see cref="ToString"/> throws.</summary>
        sealed class UnformattableException() : Exception(InjectedMessage(Failure.Unformattable))
        {
            public override string ToString() => throw new InvalidOperationException("Injected ToString() failure");
        }
    }
}