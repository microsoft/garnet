// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.test
{
    /// <summary>
    /// Entry point of this assembly, which the test host loads without calling it. Tests start the assembly as a child process to run
    /// a scenario that terminates the process (see <see cref="FlushFailureTests"/>).
    /// </summary>
    internal static class ChildProcessProgram
    {
        private static int Main(string[] args)
            => args.Length == 3 && args[0] == FlushFailureTests.ChildArgument
                ? FlushFailureTests.RunChild(Enum.Parse<FlushFailureTests.Failure>(args[1]), args[2])
                : 0;
    }
}