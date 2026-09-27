// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;

namespace Tsavorite.test
{
    /// <summary>
    /// Per-assembly setup. Installs the handlers that report which tests were in flight when an exception
    /// escapes a background thread, which NUnit runs only for the assembly under test.
    /// </summary>
    [SetUpFixture]
    public class TestProjectSetup
    {
        [OneTimeSetUp]
        public void SetUpProject() => TestBase.InstallUnhandledExceptionHandlers();
    }
}