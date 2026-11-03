// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// Modeled on test/standalone/Garnet.test/TestBase.cs.
// Tracks currently running tests for diagnostics on unhandled exceptions.

using System.Collections.Concurrent;
using System.Diagnostics;
using NUnit.Framework;

namespace Garnet.test.sentinel
{
    /// <summary>
    /// Base class for Garnet.test.sentinel tests. Tracks currently running tests so
    /// that unhandled-exception diagnostics can identify them.
    /// </summary>
    public abstract class TestBase
    {
        public static readonly ConcurrentDictionary<string, bool> RunningTests = new();

        [SetUp]
        public void TrackRunningTest()
        {
            RunningTests[TestContext.CurrentContext.Test.Name] = true;

            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration {TestContext.CurrentContext.CurrentRepeatCount + 1}: {TestContext.CurrentContext.Test.Name} ***");
        }

        [TearDown]
        public void RemoveRunningTest()
        {
            Assert.That(RunningTests.TryRemove(TestContext.CurrentContext.Test.Name, out _), Is.True,
                $"Could not find running test {TestContext.CurrentContext.Test.Name}");
        }
    }
}
