// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;

namespace Garnet.test
{
    /// <summary>
    /// Per-assembly setup. Reserves this project's TCP ports before any test runs, and installs the
    /// unhandled-exception handlers, because NUnit discovers a SetUpFixture only in the assembly under test
    /// and <c>TestBase</c> lives in a referenced assembly.
    /// </summary>
    [SetUpFixture]
    public class TestProjectSetup
    {
        [OneTimeSetUp]
        public void SetUpProject()
        {
            TestBase.InstallUnhandledExceptionHandlers();
            TestUtils.ReserveTestPorts(System.Reflection.Assembly.GetExecutingAssembly());
        }
    }
}