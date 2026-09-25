// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Tsavorite.test
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

        /// <summary>
        /// Installs process-wide handlers that dump the currently running tests when an exception escapes a
        /// background thread or an unobserved task, which is otherwise reported with no indication of which
        /// test was in flight. NUnit discovers a <c>[SetUpFixture]</c> only in the assembly under test, never
        /// in a referenced one, so each test assembly calls this from its own fixture.
        /// </summary>
        public static void InstallUnhandledExceptionHandlers()
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
                if (RunningTests.Count == 0)
                    return;
                var sb = new StringBuilder();
                _ = sb.AppendLine("*** CURRENTLY RUNNING TESTS ***:");
                foreach (var key in RunningTests.Keys)
                    _ = sb.AppendLine(key);
                Console.WriteLine(sb.ToString());
            }
        }
    }
}