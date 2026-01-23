// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
using System;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using Allure.Net.Commons;
using NUnit.Framework;

namespace Garnet.test
{

    /// <summary>
    /// Used as base class for Allure tests to label environment
    /// </summary>
    public abstract class AllureTestBase
    {
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
        }
    }
}