// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if DEBUG
using System.Threading;
using Allure.NUnit;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    [AllureNUnit]
    [TestFixture]
    public class NetworkTests : AllureTestBase
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disablePubSub: false, enableTLS: true);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.OnTearDown();
        }

        [Test]
        public void NetworkExceptions([ValuesPrefix("Network")] ExceptionInjectionType exception)
        {
            ExceptionInjectionHelper.EnableException(exception);
            try
            {
                for (int i = 0; i < 3; i++)
                {
                    using var db1 = TestUtils.GetGarnetClient(useTLS: true);
                    try
                    {
                        db1.Connect();
                    }
                    catch
                    {
                        // Ignore connection exceptions
                    }

                    // Wait for connection to fail due to server-side exception
                    while (db1.IsConnected)
                    {
                        Thread.Sleep(100);
                    }
                    ClassicAssert.IsFalse(db1.IsConnected);
                }
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(exception);
            }

            // Finally, we should connect successfully
            using var db2 = TestUtils.GetGarnetClient(useTLS: true);
            db2.Connect();

            string origValue = "abcdefg";
            ManualResetEventSlim e = new();
            db2.StringSet("mykey", origValue, (c, retValue) =>
            {
                ClassicAssert.AreEqual("OK", retValue);
                e.Set();
            });

            e.Wait();
            e.Reset();

            db2.StringGet("mykey", (c, retValue) =>
            {
                ClassicAssert.AreEqual(origValue, retValue);
                e.Set();
            });
            e.Wait();
        }
    }
}
#endif