// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.test;
using NUnit.Framework;

namespace BfTreeInterop.test
{
    [SetUpFixture]
    public class TestProjectSetup
    {
        [OneTimeSetUp]
        public void SetPort() => TestUtils.SetTestPort(TestPortAssignment.GarnetTestBfTreeInterop);
    }
}