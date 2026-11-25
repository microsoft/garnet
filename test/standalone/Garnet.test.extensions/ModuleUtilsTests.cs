// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using Garnet.server;
using NUnit.Framework;

namespace Garnet.test;

[TestFixture]
public class ModuleUtilsTests
{
    [TestCase(SearchOption.TopDirectoryOnly, true)]
    [TestCase(SearchOption.AllDirectories, false)]
    public void LoadAssembliesHonorsSearchOption(SearchOption searchOption, bool expectedSuccess)
    {
        var directory = Directory.CreateTempSubdirectory();
        try
        {
            var nested = directory.CreateSubdirectory("nested");
            File.WriteAllText(Path.Combine(nested.FullName, "invalid.dll"), "Not an assembly.");

            var success = ModuleUtils.LoadAssemblies([directory.FullName], [directory.FullName], true,
                out _, out _, searchOption: searchOption);

            Assert.That(success, Is.EqualTo(expectedSuccess));
        }
        finally
        {
            directory.Delete(true);
        }
    }
}