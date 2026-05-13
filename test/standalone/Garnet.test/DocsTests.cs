// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using Allure.NUnit;
using NUnit.Framework;

namespace Garnet.test
{
    [AllureNUnit]
    [TestFixture]
    public partial class DocsTests : AllureTestBase
    {
        [GeneratedRegex(@"^\s*\|\s*\|\s*\[(?<cmd>[^\]]+)\]\(.+?\)\s*\|\s*(?<sig>[➖])")]
        private static partial Regex CommandLinkAndMinusRegex();

        [Test]
        public void CommandsWithLinksShouldNotHaveMinusSign()
        {
            var markdownPath = Path.GetFullPath(Path.Join(TestUtils.RootTestsProjectPath, "../website/docs/commands/api-compatibility.md"));
            Assert.That(File.Exists(markdownPath));

            var lines = File.ReadAllLines(markdownPath);
            var issues = new List<string>();
            foreach (var line in lines)
            {
                var match = CommandLinkAndMinusRegex().Match(line);
                if (match.Success)
                    issues.Add($"Command {match.Groups["cmd"].Value}: remove the docs link or use the '➕' sign.");
            }

            if (issues.Count > 0)
                Assert.Fail(string.Join('\n', issues));
        }
    }
}