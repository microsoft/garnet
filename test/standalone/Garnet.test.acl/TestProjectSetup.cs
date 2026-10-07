// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;

namespace Garnet.test
{
    [SetUpFixture]
    public class TestProjectSetup
    {
        [OneTimeSetUp]
        public void SetPort() => TestUtils.ReserveTestPorts(System.Reflection.Assembly.GetExecutingAssembly());
    }
}