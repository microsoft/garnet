// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;

namespace Garnet.test
{
    /// <summary>
    /// Per-assembly setup. Forces the port slot to resolve before any test runs: this project uses the default
    /// port assignment and so never calls <see cref="cluster.ClusterTestContext.SetPort"/>, which is what
    /// otherwise resolves the slot. It also installs the unhandled-exception handlers, because NUnit runs a
    /// SetUpFixture only for the assembly under test and <c>TestBase</c> now lives in a referenced assembly.
    /// The fixture sits in <c>Garnet.test</c> so that it covers the nested <c>Garnet.test.cluster</c> namespace
    /// holding this project's tests.
    /// </summary>
    [SetUpFixture]
    public class ClusterTestProjectSetup
    {
        [OneTimeSetUp]
        public void SetUpProject()
        {
            TestBase.InstallUnhandledExceptionHandlers();
            TestUtils.EnsurePortSlotResolved();
        }
    }
}