// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.Resp.ETag
{
    [TestFixture]
    public class GeneralETagCoverageTests : ETagCoverageTestsBase
    {
        protected HashSet<RespCommand> ExcludedCommands =
        [
            RespCommand.WATCH,
            RespCommand.WATCHMS,
            RespCommand.WATCHOS,
            RespCommand.SPUBLISH,
            RespCommand.SSUBSCRIBE
        ];

        public override void DataSetUp(bool nxKey = false) { }

        [Test]
        public void AllDataCommandsCovered()
        {
            var tests =
                typeof(ETagCoverageTestsBase).Assembly.GetTypes()
                    .Where(t =>
                        t != typeof(ETagCoverageTestsBase) &&
                        typeof(ETagCoverageTestsBase).IsAssignableFrom(t))
                    .SelectMany(t => t.GetMethods()).Where(static mtd => mtd.GetCustomAttribute<TestAttribute>() != null);

            HashSet<string> covered = new();

            foreach (var test in tests)
            {
                if (test.Name == nameof(AllDataCommandsCovered))
                    continue;

                ClassicAssert.IsTrue(test.Name.EndsWith("ETagTestAsync"), $"Expected all tests in {nameof(RespCommandTests)} except {nameof(AllDataCommandsCovered)} to be per-command and end with ETagTestAsync, unexpected test: {test.Name}");

                var command = test.Name[..^"ETagTestAsync".Length];
                covered.Add(command);
            }

            // Check tests against RespCommand
            var allDataCommands = Enum.GetValues<RespCommand>().Where(c => c.IsDataCommand()).Except(NoKeyCommands)
                .Except(ExcludedCommands)
                .Select(static x => x.NormalizeForACLs()).Distinct();
            var notCovered = allDataCommands.Where(cmd =>
                !covered.Contains(cmd.ToString().Replace("_", ""), StringComparer.OrdinalIgnoreCase));

            ClassicAssert.IsEmpty(notCovered,
                $"Commands in RespCommand not covered by ETag Tests:{Environment.NewLine}{string.Join(Environment.NewLine, notCovered.OrderBy(static x => x))}");
        }
    }
}
