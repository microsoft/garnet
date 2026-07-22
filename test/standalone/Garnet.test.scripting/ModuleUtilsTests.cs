// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    [TestFixture]
    public class ModuleUtilsTests : TestBase
    {
        [Test]
        // Plain path (no spaces), no arguments
        [TestCase("/opt/garnet/Module.dll", "/opt/garnet/Module.dll", new string[] { })]
        [TestCase(@"C:\Garnet\Modules\MyModule.dll", @"C:\Garnet\Modules\MyModule.dll", new string[] { })]
        // Plain path (no spaces) with arguments
        [TestCase(@"C:\Garnet\Modules\MyModule.dll arg0 arg1", @"C:\Garnet\Modules\MyModule.dll", new string[] { "arg0", "arg1" })]
        // Quoted path containing spaces, no arguments (issue #1951)
        [TestCase("\"/opt/My Modules/My Module.dll\"", "/opt/My Modules/My Module.dll", new string[] { })]
        [TestCase("\"C:\\Users\\John Doe\\Garnet Modules\\MyModule.dll\"", @"C:\Users\John Doe\Garnet Modules\MyModule.dll", new string[] { })]
        // Quoted path containing spaces, with arguments
        [TestCase("\"C:\\Users\\John Doe\\MyModule.dll\" arg0 arg1", @"C:\Users\John Doe\MyModule.dll", new string[] { "arg0", "arg1" })]
        // Unquoted path containing spaces is split on the first space: the path must be quoted to keep spaces
        [TestCase("/opt/My Modules/My Module.dll", "/opt/My", new string[] { "Modules/My", "Module.dll" })]
        // Leading/trailing whitespace is trimmed
        [TestCase("   /opt/garnet/Module.dll   ", "/opt/garnet/Module.dll", new string[] { })]
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
    }
}