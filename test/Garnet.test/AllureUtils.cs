// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
using System;
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
            string os = RuntimeInformation.IsOSPlatform(OSPlatform.Linux) ? "Linux" :
                        RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "Windows" :
                        "unknown";
            var frameworkAttr = Assembly.GetExecutingAssembly()
            .GetCustomAttribute<TargetFrameworkAttribute>();

            var framework = "unknown";
            if (frameworkAttr != null)
            {
                var parts = frameworkAttr.FrameworkName.Split(',');
                if (parts.Length > 0)
                {
                    var lastPart = parts[parts.Length - 1];
                    framework = lastPart.Replace("Version=v", "net");
                }
            }
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