// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    [TestFixture]
    public class ModuleUtilsTests : TestBase
    {
        [Test]
        // Plain path, no arguments
        [TestCase("/opt/garnet/Module.dll", "/opt/garnet/Module.dll", new string[] { })]
        [TestCase(@"C:\Garnet\Modules\MyModule.dll", @"C:\Garnet\Modules\MyModule.dll", new string[] { })]
        // Path containing spaces, no arguments (issue #1951)
        [TestCase(@"C:\Users\John Doe\Garnet Modules\MyModule.dll", @"C:\Users\John Doe\Garnet Modules\MyModule.dll", new string[] { })]
        [TestCase("/opt/My Modules/My Module.dll", "/opt/My Modules/My Module.dll", new string[] { })]
        // Path with arguments (no spaces in path) - backward compatibility
        [TestCase(@"C:\Garnet\Modules\MyModule.dll arg0 arg1", @"C:\Garnet\Modules\MyModule.dll", new string[] { "arg0", "arg1" })]
        // Path with spaces AND arguments
        [TestCase(@"C:\Users\John Doe\MyModule.dll arg0 arg1", @"C:\Users\John Doe\MyModule.dll", new string[] { "arg0", "arg1" })]
        // Explicitly quoted path with arguments
        [TestCase("\"C:\\Users\\John Doe\\MyModule.dll\" arg0 arg1", @"C:\Users\John Doe\MyModule.dll", new string[] { "arg0", "arg1" })]
        [TestCase("\"/opt/My Modules/My Module.dll\"", "/opt/My Modules/My Module.dll", new string[] { })]
        // Lowercase .exe with arguments is split via the extension heuristic (path does not exist here)
        [TestCase("/opt/My Modules/My Module.exe arg0", "/opt/My Modules/My Module.exe", new string[] { "arg0" })]
        // Uppercase extension is not a recognized module extension (matching is case-sensitive): a
        // non-existent spec is treated as a whole path rather than split
        [TestCase("/opt/My Modules/My Module.EXE arg0", "/opt/My Modules/My Module.EXE arg0", new string[] { })]
        // Leading/trailing whitespace is trimmed
        [TestCase("   /opt/garnet/Module.dll   ", "/opt/garnet/Module.dll", new string[] { })]
        // Directory path (no recognized extension) - whole spec treated as the path
        [TestCase("/opt/My Modules/bin", "/opt/My Modules/bin", new string[] { })]
        public void ParseModuleSpecTest(string moduleSpec, string expectedPath, string[] expectedArgs)
        {
            var parsed = ModuleUtils.TryParseModuleSpec(moduleSpec, out var modulePath, out var moduleArgs);
            ClassicAssert.IsTrue(parsed);
            ClassicAssert.AreEqual(expectedPath, modulePath);
            CollectionAssert.AreEqual(expectedArgs, moduleArgs);
        }

        [Test]
        [TestCase(null)]
        [TestCase("")]
        [TestCase("   ")]
        public void ParseModuleSpecEmptyTest(string moduleSpec)
        {
            var parsed = ModuleUtils.TryParseModuleSpec(moduleSpec, out var modulePath, out var moduleArgs);
            ClassicAssert.IsFalse(parsed);
            ClassicAssert.IsNull(modulePath);
            CollectionAssert.IsEmpty(moduleArgs);
        }

        [Test]
        // Empty quoted path
        [TestCase("\"\"")]
        [TestCase("\"\" arg0")]
        // Whitespace-only quoted path
        [TestCase("\"   \"")]
        [TestCase("\"   \" arg0")]
        // Unterminated quote
        [TestCase("\"/opt/My Modules/My Module.dll")]
        [TestCase("\"/opt/My Modules/My Module.dll arg0")]
        public void ParseModuleSpecInvalidQuotedTest(string moduleSpec)
        {
            var parsed = ModuleUtils.TryParseModuleSpec(moduleSpec, out var modulePath, out var moduleArgs);
            ClassicAssert.IsFalse(parsed);
            ClassicAssert.IsNull(modulePath);
            CollectionAssert.IsEmpty(moduleArgs);
        }

        [Test]
        public void ParseModuleSpecProbesFilesystemForPathBoundary()
        {
            var baseDir = Path.Combine(Path.GetTempPath(), "garnet_moduleutils_" + Path.GetRandomFileName());
            try
            {
                // A directory whose name contains a space and a ".dll" fragment; the module file lives inside it.
                var trickyDir = Path.Combine(baseDir, "plugin.dll dir");
                Directory.CreateDirectory(trickyDir);
                var moduleFile = Path.Combine(trickyDir, "Module.dll");
                File.WriteAllBytes(moduleFile, []);

                // No args: the whole path (with spaces and a ".dll" directory fragment) is resolved on disk,
                // instead of being split at the first ".dll" token.
                var parsed = ModuleUtils.TryParseModuleSpec(moduleFile, out var path, out var args);
                ClassicAssert.IsTrue(parsed);
                ClassicAssert.AreEqual(moduleFile, path);
                CollectionAssert.IsEmpty(args);

                // With args: the path is resolved on disk and the trailing tokens become arguments.
                parsed = ModuleUtils.TryParseModuleSpec($"{moduleFile} arg0 arg1", out path, out args);
                ClassicAssert.IsTrue(parsed);
                ClassicAssert.AreEqual(moduleFile, path);
                CollectionAssert.AreEqual(new[] { "arg0", "arg1" }, args);

                // A directory module followed by an argument that itself ends in ".dll".
                var moduleDir = Path.Combine(baseDir, "moduledir");
                Directory.CreateDirectory(moduleDir);
                parsed = ModuleUtils.TryParseModuleSpec($"{moduleDir} arg.dll", out path, out args);
                ClassicAssert.IsTrue(parsed);
                ClassicAssert.AreEqual(moduleDir, path);
                CollectionAssert.AreEqual(new[] { "arg.dll" }, args);
            }
            finally
            {
                if (Directory.Exists(baseDir))
                    Directory.Delete(baseDir, recursive: true);
            }
        }
    }
}