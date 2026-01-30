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
    public class GeneralEtagCoverageTests : EtagCoverageTestsBase
    {
        public override void DataSetUp() => throw new System.NotImplementedException();

        [Test]
        public void AllCommandsCovered()
        {
            var tests =
                typeof(EtagCoverageTestsBase).Assembly.GetTypes()
                    .Where(t =>
                        t != typeof(EtagCoverageTestsBase) &&
                        typeof(EtagCoverageTestsBase).IsAssignableFrom(t))
                    .SelectMany(t => t.GetMethods()).Where(static mtd => mtd.GetCustomAttribute<TestAttribute>() != null);

            HashSet<string> covered = new();

            foreach (var test in tests)
            {
                if (test.Name == nameof(AllCommandsCovered))
                    continue;

                ClassicAssert.IsTrue(test.Name.EndsWith("ETagAdvancedTestAsync"), $"Expected all tests in {nameof(RespCommandTests)} except {nameof(AllCommandsCovered)} to be per-command and end with ETagAdvancedTestAsync, unexpected test: {test.Name}");

                var command = test.Name[..^"ETagAdvancedTestAsync".Length];
                covered.Add(command);
            }

            // Check tests against RespCommand
            var allWriteCommands = Enum.GetValues<RespCommand>().Where(c => c.IsDataCommand() && c.IsWriteOnly()).Except(NoKeyDataCommands)
                .Select(static x => x.NormalizeForACLs()).Distinct();
            var notCovered = allWriteCommands.Where(cmd =>
                !covered.Contains(cmd.ToString().Replace("_", ""), StringComparer.OrdinalIgnoreCase));

            ClassicAssert.IsEmpty(notCovered,
                $"Commands in RespCommand not covered by ETag Tests:{Environment.NewLine}{string.Join(Environment.NewLine, notCovered.OrderBy(static x => x))}");
        }
    }
}
