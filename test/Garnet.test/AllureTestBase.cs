// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Text;
using System.Threading.Tasks;
using Allure.Net.Commons;
using NUnit.Framework;

namespace Garnet.test
{
    /// <summary>
    /// Used as base class for Allure tests to label environment
    /// </summary>
    public abstract class AllureTestBase
    {
        // Thread-safe collection to store currently running tests
        public static readonly ConcurrentDictionary<string, bool> RunningTests = new();

        [SetUp]
        public void LabelEnvironment()
        {
            var os = RuntimeInformation.IsOSPlatform(OSPlatform.Linux) ? nameof(OSPlatform.Linux) :
                        RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? nameof(OSPlatform.Windows) :
                        RuntimeInformation.IsOSPlatform(OSPlatform.OSX) ? nameof(OSPlatform.OSX) :
                        "unknown";

            var frameworkAttr = Assembly.GetExecutingAssembly()
                .GetCustomAttribute<TargetFrameworkAttribute>();
            var framework = frameworkAttr?.FrameworkName.Split(',').LastOrDefault()?.Replace("Version=v", "net") ?? "unknown";

            var config = Assembly.GetExecutingAssembly()
                .GetCustomAttribute<AssemblyConfigurationAttribute>()?.Configuration ?? "unknown";
            var timestamp = DateTime.Now.ToString("M/d/yyyy");
            var fullName = $"[{os}, {framework}, {config}]";
            var namespaceName = GetType().Namespace ?? "UnknownNamespace";

            AllureLifecycle.Instance.UpdateTestCase(x =>
            {
                // Remove any default suite/subSuite labels that NUnit added
                x.labels.RemoveAll(l => l.name == "suite" || l.name == "subSuite");

                // apply your custom hierarchy
                x.labels.Add(Label.ParentSuite($"{namespaceName} - {timestamp}"));
                x.labels.Add(Label.Suite(os));
                x.labels.Add(Label.SubSuite($"{framework} | {config}"));
            });

            // allows to separate out tests based on config but still hold history
            AllureApi.AddTestParameter("env", fullName);

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
            if (Garnet.test.AllureTestBase.RunningTests.Count == 0)
                return;
            var sb = new StringBuilder();
            _ = sb.AppendLine("*** CURRENTLY RUNNING TESTS ***:");
            foreach (var key in Garnet.test.AllureTestBase.RunningTests.Keys)
                _ = sb.AppendLine(key);
            Console.WriteLine(sb.ToString());
        }
    }
}