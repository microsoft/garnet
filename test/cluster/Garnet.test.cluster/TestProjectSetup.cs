// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;

namespace Garnet.test
{
    /// <summary>
    /// Forces the port slot to resolve before any test runs. This project uses the default port assignment and
    /// so never calls <see cref="cluster.ClusterTestContext.SetPort"/>, which is what otherwise resolves the
    /// slot. The fixture sits in <c>Garnet.test</c> so that it covers the nested <c>Garnet.test.cluster</c>
    /// namespace holding this project's tests, and because <c>GlobalUnhandledExceptionHandling</c> already
    /// occupies the global namespace in this assembly; NUnit permits one SetUpFixture per namespace.
    /// </summary>
    [SetUpFixture]
    public class ClusterTestProjectSetup
    {
        [OneTimeSetUp]
        public void SetPort() => TestUtils.EnsurePortSlotResolved();
    }
}