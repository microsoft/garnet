// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;

namespace Garnet.test
{
    /// <summary>
    /// Forces the port slot to resolve before any test runs. This project uses the default port assignment and
    /// so never calls <see cref="TestUtils.SetTestPort"/>, which is what otherwise resolves the slot.
    /// The fixture is namespaced rather than global because <c>GlobalUnhandledExceptionHandling</c> already
    /// occupies the global namespace in this assembly, and NUnit permits one SetUpFixture per namespace.
    /// </summary>
    [SetUpFixture]
    public class TestProjectSetup
    {
        [OneTimeSetUp]
        public void SetPort() => TestUtils.EnsurePortSlotResolved();
    }
}