// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.sentinel
{
    /// <summary>
    /// Centralised gating logic for tests that spawn external redis-server or
    /// redis-sentinel processes. Two gates, both must be open for an external
    /// process to be launched:
    ///
    ///   1. Compile-time: ENABLE_SENTINEL_TESTS_FULL is defined only when the
    ///      project is built with -p:EnableSentinelTests=true. Without it, the
    ///      code paths that call Process.Start for redis-server / redis-sentinel
    ///      are compiled out.
    ///
    ///   2. Runtime: the GARNET_SENTINEL_TESTS environment variable must be set
    ///      to "1". This lets a developer build with the full symbol defined
    ///      but still keep tests off in their shell, and lets CI pass -p:EnableSentinelTests=true
    ///      without accidentally running external-process tests in environments
    ///      that don't have the redis binary available.
    ///
    /// Tests that depend on the gate should call <see cref="RequireExternalProcesses"/>
    /// at the top of each test method, matching the existing Assert.Ignore pattern
    /// used elsewhere in the Garnet test suite (TestUtils.cs:257).
    /// </summary>
    internal static class SentinelGate
    {
        /// <summary>
        /// Environment variable consulted at runtime. Set to "1" to enable.
        /// </summary>
        public const string EnvironmentVariable = "GARNET_SENTINEL_TESTS";

        /// <summary>
        /// True iff ENABLE_SENTINEL_TESTS_FULL is defined at compile time.
        /// </summary>
        public static bool CompileTimeGateOpen =>
#if ENABLE_SENTINEL_TESTS_FULL
            true;
#else
            false;
#endif

        /// <summary>
        /// True iff GARNET_SENTINEL_TESTS=1 in the environment.
        /// </summary>
        public static bool RuntimeGateOpen =>
            System.Environment.GetEnvironmentVariable(EnvironmentVariable) == "1";

        /// <summary>
        /// True iff both gates are open. When false, the test must skip itself
        /// via <see cref="RequireExternalProcesses"/>.
        /// </summary>
        public static bool ExternalProcessesAllowed => CompileTimeGateOpen && RuntimeGateOpen;

        /// <summary>
        /// Call from a test method. If external processes are not allowed by
        /// both gates, calls <see cref="Assert.Ignore(string)"/> with a reason,
        /// causing the test to be reported as Ignored rather than Failed.
        /// </summary>
        public static void RequireExternalProcesses()
        {
            if (CompileTimeGateOpen && !RuntimeGateOpen)
            {
                ClassicAssert.Ignore(
                    $"External-process tests require environment variable {EnvironmentVariable}=1. " +
                    "Build with -p:EnableSentinelTests=true to enable the compile-time gate.");
            }
            else if (!CompileTimeGateOpen)
            {
                ClassicAssert.Ignore(
                    "External-process tests are not enabled in this build. " +
                    "Rebuild with -p:EnableSentinelTests=true to enable.");
            }
        }
    }
}
