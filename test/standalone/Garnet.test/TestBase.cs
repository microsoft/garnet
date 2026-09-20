// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Garnet.test
{
    /// <summary>
    /// Base class for tests — tracks currently running tests for diagnostics.
    /// </summary>
    public abstract class TestBase
    {
        // Thread-safe collection to store currently running tests
        public static readonly ConcurrentDictionary<string, bool> RunningTests = new();

        [SetUp]
        public void TrackRunningTest()
        {
            // Add test to the running list
            RunningTests[TestContext.CurrentContext.Test.Name] = true;

            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration {TestContext.CurrentContext.CurrentRepeatCount + 1}: {TestContext.CurrentContext.Test.Name} ***");
        }

        [TearDown]
        public void RemoveRunningTest()
        {
            Assert.That(RunningTests.TryRemove(TestContext.CurrentContext.Test.Name, out _), Is.True, $"Could not find running test {TestContext.CurrentContext.Test.Name}");
        }
    }
}

[SetUpFixture]
public sealed class GlobalUnhandledExceptionHandling
{
    [OneTimeSetUp]
    public void Install()
    {
        AppDomain.CurrentDomain.UnhandledException += (s, e) =>
        {
            DumpTests();
        };

        TaskScheduler.UnobservedTaskException += (s, e) =>
        {
            DumpTests();
            e.SetObserved(); // Optionally mark observed so it doesn't escalate later
        };

        static void DumpTests()
        {
            if (Garnet.test.TestBase.RunningTests.Count == 0)
                return;
            var sb = new StringBuilder();
            _ = sb.AppendLine("*** CURRENTLY RUNNING TESTS ***:");
            foreach (var key in Garnet.test.TestBase.RunningTests.Keys)
                _ = sb.AppendLine(key);
            Console.WriteLine(sb.ToString());
        }
    }
}