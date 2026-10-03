// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;

namespace Garnet.test
{
    /// <summary>
    /// Per-assembly setup. Reserves this project's contiguous cluster port run before any test runs, and
    /// installs the unhandled-exception handlers, because NUnit discovers a SetUpFixture only in the assembly
    /// under test and <c>TestBase</c> lives in a referenced assembly. The fixture sits in <c>Garnet.test</c> so
    /// that it covers the nested <c>Garnet.test.cluster</c> namespace holding this project's tests.
    /// </summary>
    [SetUpFixture]
    public class ClusterTestProjectSetup
    {
        [OneTimeSetUp]
        public void SetUpProject()
        {
            TestBase.InstallUnhandledExceptionHandlers();
            cluster.ClusterTestContext.ReservePorts(System.Reflection.Assembly.GetExecutingAssembly());
        }
    }
}