// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Reflection;
using System.Text;
using Allure.NUnit;
using Garnet.fuzz;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Tests that assert the basics of Garnet.fuzz still work, so they aren't broken between fuzzing runs.
    /// </summary>
    [AllureNUnit]
    [TestFixture]
    public class FuzzTargetTests : AllureTestBase
    {
        [TearDown]
        public void TearDown()
        {
            TestUtils.OnTearDown();
        }

        [Test]
        public void TestsForAllTargets()
        {
            // Ensure that all targets added in the future also have tests here

            var res = new StringBuilder();

            foreach (var target in Enum.GetValues<FuzzTargets>())
            {
                var mtd = typeof(FuzzTargetTests).GetMethod(target.ToString(), BindingFlags.Public | BindingFlags.Instance);

                if (mtd == null)
                {
                    _ = res.AppendLine($"No method found for {target}");
                }
                else if (mtd.GetCustomAttribute<TestAttribute>() == null)
                {
                    _ = res.AppendLine($"No [Test] on method for {target}");
                }
            }

            ClassicAssert.IsEmpty(res.ToString());
        }

        [Test]
        public void GarnetEndToEnd()
        {
            fuzz.Targets.GarnetEndToEnd.Fuzz([]);
        }

        [Test]
        public void LuaScriptCompilation()
        {
            fuzz.Targets.LuaScriptCompilation.Fuzz([]);
        }

        [Test]
        public void RespCommandParsing()
        {
            fuzz.Targets.RespCommandParsing.Fuzz([]);
        }
    }
}