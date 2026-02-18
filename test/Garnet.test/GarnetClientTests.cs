// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    [AllureNUnit]
    [TestFixture]
    public class GarnetClientTests : AllureTestBase
    {

        readonly string[,] worldcities = new string[,] {
                {"51.6889350991489" ,"Hertogenbosch"},
                {"29.832041799755" ,"15MayCity"},
                {"29.9723457997422" ,"6October"},
                {"43.3712091005418" ,"ACoruna"},
                {"9.690232799424" ,"ANSOUMANIYA"},
                {"9.71045299942484" ,"ANSOUMANYAH"},
                {"50.7763509992898" ,"Aachen"},
                {"57.0462626002815" ,"Aalborg"},
                {"56.1496277998528" ,"Aarhus"},
                {"68.7095879972654" ,"Aasiaat"},
                {"5.11273499949875" ,"Aba"},
                {"30.3636096997085" ,"Abadan"},
                {"-1.72182799983967" ,"Abaetetuba"},
                {"44.018620200532" ,"AbagBanner"},
                {"8969942957" ,"Abakaliki"},
                {"53.6940294823123" ,"Abakan"},
                {"-13.637348200271" ,"Abancay"},
                {"206027005319" ,"Abashiri"},
                {"521161996794" ,"Abbotsford"},
                {"36588996066" ,"Abbottabad"},
                {"81571996591" ,"Abdi"},
                {"6.7269041994137" ,"Abengourou"},
                {"49805004172" ,"Aberdeen"},
                {"5699948468" ,"Abidjan"},
                {"639989997222" ,"Abiko"},
                {"32.446449999595" ,"Abilene"},
                {"8699947724" ,"Abobo"},
                {"30.1450542997269" ,"Abohar"},
                {"119940068" ,"Abomey"},
                {"369942394" ,"Abomey-Calavi"},
                {"62929958837" ,"Abong-Mbang"},
                {"11.450623499533" ,"Aboudei`a"},
                {"39.4631905001925" ,"Abrantes"},
                {"071899975481" ,"AbreuLima"},
                {"30384003999" ,"Abri"},
                {"90206000128" ,"AbuDelelq"},
                {"1069995933" ,"AbuGharaq"},
                {"19.5378928003486" ,"AbuHamad"},
                {"11.4646720995342" ,"AbuJibeha"},
                {"51699996804" ,"AbuKabir"},
                {"05111996198" ,"AbuKamal"},
                {"22.3567809439398" ,"AbuSimbel"},
                {"162999832" ,"AbuZenima"},
                {"9.064330499403" ,"Abuja"},
                {"80294997704" ,"Abeche"},
                {"13.5897966997435" ,"Acajutla"},
                {"16.8680495001213" ,"Acapulco"},
                {"9.55079609941846" ,"Acarigua"},
                {"5.55710959946969" ,"Accra"},
                {"882479939142" ,"Achaguas"},
                {"21.254670000408" ,"Achalpur"},
                {"362499004082" ,"Acheng"},
                {"94845999064" ,"Achinsk"},
                {"5680995004585" ,"Achocalla"},
                {"32.9240047205798" ,"Acre"},
                {"11730995339" ,"AdDaein"},
                {"61159996651" ,"AdDali"},
                {"53303996091" ,"AdDiwaniyah"},
                {"11.8073635995631" ,"Ad-Damazin"},
                {"37029997147" ,"Adachi"},
                {"02609939314" ,"Adama"},
                {"63598998466" ,"Adana"},
                {"9.01079339940166" ,"AddisAbaba"},
                {"90872996439" ,"Adde"},
                {"9281804998736" ,"Adelaide"},
                {"19.6759452003563" ,"Adilabad"},
                {"37.763953199949" ,"Adiyaman"},
                {"12109948253" ,"Adjame"},
                {"19939301" ,"AdoEkiti"},
                {"15.6253312999809" ,"Adoni"},
                {"64562997297" ,"Adre"},
                {"2.14602869977114" ,"Afgooye"},
                {"34.1125199996055" ,"Aflou"},
                {"6.37343724316886" ,"Afosu"},
                {"36.5083794997896" ,"Afrin"},
            };


        ManualResetEventSlim waiter;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            waiter = new ManualResetEventSlim();
        }

        [TearDown]
        public void TearDown()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        static void WaitAndReset(ManualResetEventSlim e)
        {
            e.Wait();
            e.Reset();
        }

        [Test]
        public void SetGetWithCallback([Values] bool useTLS)
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: useTLS);
            server.Start();

            using var db = TestUtils.GetGarnetClient(useTLS: useTLS);
            db.Connect();

            string origValue = "abcdefg";
            ManualResetEventSlim e = new();
            db.StringSet("mykey", origValue, (c, retValue) =>
            {
                ClassicAssert.AreEqual("OK", retValue);
                e.Set();
            });

            e.Wait();
            e.Reset();

            db.StringGet("mykey", (c, retValue) =>
            {
                ClassicAssert.AreEqual(origValue, retValue);
                e.Set();
            });
            e.Wait();
        }

        [Test]
        public void SimpleMetricsTest([Values] bool recordLatency)
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();

            using var db = TestUtils.GetGarnetClient(recordLatency: recordLatency);
            db.Connect();

            var metrics = db.GetLatencyMetrics();
            ClassicAssert.AreEqual(0, metrics.Length);

            if (recordLatency)
            {
                int count = 0;
                while (count++ < 100)
                {
                    var task = db.StringGetAsync("mykey");
                    task.Wait();
                    var resp = task.Result;
                    ClassicAssert.AreEqual(null, resp);
                }

                metrics = db.GetLatencyMetrics();
                ClassicAssert.AreEqual(8, metrics.Length);
            }

            db.Dispose();
            metrics = db.GetLatencyMetrics();
            ClassicAssert.AreEqual(0, metrics.Length); // Should be 0 after dispose
        }

        [Test]
        public async Task SimpleStringArrayTest([Values] bool stringParams)
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();

            using var db = TestUtils.GetGarnetClient();
            db.Connect();

            long n = stringParams ?
                int.Parse(await db.ExecuteForStringResultAsync("INCRBY", ["myKey", "10"])) :
                int.Parse(await db.ExecuteForStringResultAsync(Encoding.ASCII.GetBytes("$6\r\nINCRBY\r\n"), [Encoding.ASCII.GetBytes("myKey"), Encoding.ASCII.GetBytes("10")]));

            ClassicAssert.AreEqual(10, n);
        }

        [Test]
        public async Task SimpleNoArgsTest()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();

            using var db = TestUtils.GetGarnetClient();
            db.Connect();

            var result = await db.ExecuteForStringResultAsync("PING");
            ClassicAssert.AreEqual("PONG", result);

            result = await db.ExecuteForStringResultAsync("ASKING");
            ClassicAssert.AreEqual("OK", result);
        }

        [Test]
        public async Task SimpleIncrTest([Values] bool stringParams)
        {
            ManualResetEventSlim e = new();
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();

            var key = "mykey";
            using var db = TestUtils.GetGarnetClient();
            db.Connect();

            long expectedV = 1;
            long v = stringParams ? await db.StringIncrement(key) : await db.StringIncrement(Encoding.ASCII.GetBytes(key));
            ClassicAssert.AreEqual(expectedV, v);

            expectedV += 10;
            v = stringParams ? await db.StringIncrement(key, 10) : await db.StringIncrement(Encoding.ASCII.GetBytes(key), 10);
            ClassicAssert.AreEqual(expectedV, v);

            expectedV++;
            if (stringParams)
            {
                db.StringIncrement(key, (c, retValue) =>
                {
                    long v = long.Parse(retValue);
                    ClassicAssert.AreEqual(expectedV, v);
                    e.Set();
                });
            }
            else
            {
                db.StringIncrement(Encoding.ASCII.GetBytes(key), (c, retValue) =>
                {
                    long v = long.Parse(retValue);
                    ClassicAssert.AreEqual(expectedV, v);
                    e.Set();
                });
            }
            WaitAndReset(e);

            expectedV += 7;
            if (stringParams)
            {
                db.StringIncrement(key, 7, (c, retValue) =>
                {
                    long v = long.Parse(retValue);
                    ClassicAssert.AreEqual(expectedV, v);
                    e.Set();
                });
            }
            else
            {
                db.StringIncrement(Encoding.ASCII.GetBytes(key), 7, (c, retValue) =>
                {
                    long v = long.Parse(retValue);
                    ClassicAssert.AreEqual(expectedV, v);
                    e.Set();
                });
            }
            WaitAndReset(e);
        }

        [Test]
        public async Task SimpleDecrTest([Values] bool stringParams)
        {
            ManualResetEventSlim e = new();
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();

            var key = "mykey";
            using var db = TestUtils.GetGarnetClient();
            db.Connect();

            long expectedV = -1;
            long v = stringParams ? await db.StringDecrement(key) : await db.StringDecrement(Encoding.ASCII.GetBytes(key));
            ClassicAssert.AreEqual(expectedV, v);

            expectedV -= 10;
            v = stringParams ? await db.StringDecrement(key, 10) : await db.StringDecrement(Encoding.ASCII.GetBytes(key), 10);
            ClassicAssert.AreEqual(expectedV, v);

            expectedV--;
            if (stringParams)
            {
                db.StringDecrement(key, (c, retValue) =>
                {
                    long v = long.Parse(retValue);
                    ClassicAssert.AreEqual(expectedV, v);
                    e.Set();
                });
            }
            else
            {
                db.StringDecrement(Encoding.ASCII.GetBytes(key), (c, retValue) =>
                {
                    long v = long.Parse(retValue);
                    ClassicAssert.AreEqual(expectedV, v);
                    e.Set();
                });
            }
            WaitAndReset(e);

            expectedV -= 7;
            if (stringParams)
            {
                db.StringDecrement(key, 7, (c, retValue) =>
                {
                    long v = long.Parse(retValue);
                    ClassicAssert.AreEqual(expectedV, v);
                    e.Set();
                });
            }
            else
            {
                db.StringDecrement(Encoding.ASCII.GetBytes(key), 7, (c, retValue) =>
                {
                    long v = long.Parse(retValue);
                    ClassicAssert.AreEqual(expectedV, v);
                    e.Set();
                });
            }
            WaitAndReset(e);
        }

        [Test]
        public async Task CanUseSetNxStringResultAsync()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();

            using var db = TestUtils.GetGarnetClient();
            db.Connect();

            var result = await db.ExecuteForStringResultAsync("SET", ["mykey", "Hello", "NX"]);
            ClassicAssert.AreEqual("OK", result);

            result = await db.ExecuteForStringResultAsync("SET", ["mykey", "World", "NX"]);
            ClassicAssert.AreEqual(null, result);

            var resultMykey = await db.StringGetAsync("mykey");
            ClassicAssert.AreEqual("Hello", resultMykey);
        }

        [Test]
        public async Task ShouldNotThrowExceptionForEmptyArrayResponseAsync()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();

            using var db = TestUtils.GetGarnetClient();
            await db.ConnectAsync();

            ClassicAssert.DoesNotThrowAsync(async () =>
            {
                var result = await db.ExecuteForStringResultAsync("KEYS", ["*"]);
                ClassicAssert.IsNull(result);
            });
        }

        [Test]
        public async Task CanUseMGetTests([Values] bool disableObjectStore)
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, disableObjects: disableObjectStore);
            server.Start();

            using var db = TestUtils.GetGarnetClient();
            db.Connect();
            var nKeys = worldcities.GetLength(0);
            var keys = new string[nKeys];
            var keysMemory = new Memory<byte>[nKeys];

            for (int i = 0; i < nKeys; i++)
            {
                var result = await db.ExecuteForStringResultAsync("SET", [worldcities[i, 1], worldcities[i, 0]]);
                keys[i] = worldcities[i, 1];
                keysMemory[i] = Encoding.ASCII.GetBytes(keys[i]);
                ClassicAssert.AreEqual("OK", result);
            }

            var keysValues = await db.StringGetAsMemoryAsync(keysMemory);
            for (int i = 0; i < keysValues.Length; i++)
            {
                ClassicAssert.IsTrue(keysValues[i].Span.SequenceEqual(Encoding.ASCII.GetBytes(worldcities[i, 0])));
            }

            waiter.Reset();

            //StringGetAsMemory with Memory result array with callback type
            db.StringGetAsMemory(keysMemory, SimpleMemoryResultArrayCallback, 1);
            waiter.Wait();
            waiter.Reset();

            //StringGetAsync without cancellation
            var tokenSource = new CancellationTokenSource();
            var token = tokenSource.Token;

            var tReadValues = ReadValuesMGet(keys, token);
            tReadValues.GetAwaiter().GetResult();
            ClassicAssert.IsTrue(tReadValues.IsCompletedSuccessfully);

            //StringGetAsync with cancellation
            tokenSource.Cancel();
            tReadValues = ReadValuesMGet(keys, token);
            Assert.Throws<TaskCanceledException>(() => tReadValues.GetAwaiter().GetResult());

            //StringGetAsync
            var vals = await db.StringGetAsync(keys);
            for (int i = 0; i < nKeys; i++)
            {
                ClassicAssert.AreEqual(worldcities[i, 0], vals[i]);
            }

            //StringGet with string array callback type
            db.StringGet(keys, SimpleStringArrayCallback, 1);
            waiter.Wait();
            waiter.Reset();

            //Dispose
            for (int i = 0; i < keysValues.Length; i++)
            {
                keysValues[i].Dispose();
            }

            tokenSource.Dispose();
        }

        private async Task<int> ReadValuesMGet(string[] keys, CancellationToken t)
        {
            using var db = TestUtils.GetGarnetClient();
            db.Connect();
            var vals = new string[keys.Length];
            vals = await db.StringGetAsync(keys, t);

            int i = 0;
            while (i < keys.Length)
            {
                ClassicAssert.AreEqual(worldcities[i, 0], vals[i]);
                i++;
            }
            return i;
        }

        private void SimpleMemoryResultArrayCallback(long context, MemoryResult<byte>[] result, MemoryResult<byte> error)
        {
            int i = 0;
            while (i < result.Length)
            {
                ClassicAssert.IsTrue(result[i].Span.SequenceEqual(Encoding.ASCII.GetBytes(worldcities[i, 0])));
                //Dispose
                result[i].Dispose();
                i++;
            }
            waiter.Set();
        }

        private void SimpleStringArrayCallback(long context, string[] result, string error)
        {
            for (int i = 0; i < result.Length; i++)
            {
                ClassicAssert.AreEqual(result[i], worldcities[i, 0]);
            }
            waiter.Set();
        }


        [Test]
        [Explicit("Test sometimes hangs")]
        public async Task CanDoBulkDeleteTests([Values] bool useStringType)
        {
            //KeyDeleteAsync
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();

            using var db = TestUtils.GetGarnetClient();
            db.Connect();

            var nKeys = worldcities.GetLength(0);
            var iterationSize = 20;
            var nKeysObjectStore = 10;
            var keys = new string[nKeys + nKeysObjectStore];
            var keysMemoryByte = new Memory<byte>[nKeys + nKeysObjectStore];

            for (int i = 0; i < nKeys; i++)
            {
                // create in the main store
                var result = await db.ExecuteForStringResultAsync("SET", [worldcities[i, 1], worldcities[i, 0]]);
                keys[i] = worldcities[i, 1];
                keysMemoryByte[i] = Encoding.ASCII.GetBytes(keys[i]);
                ClassicAssert.AreEqual("OK", result);
            }

            for (int x = 0; x < nKeysObjectStore; x++)
            {
                // create in the object store
                var result = await db.ExecuteForStringResultAsync("ZADD", [$"myzset{x}", "1", "KEY1", "2", "KEY2"]);
                ClassicAssert.AreEqual("2", result);
                keys[nKeys + x] = $"myzset{x}";
                keysMemoryByte[nKeys + x] = Encoding.ASCII.GetBytes(keys[nKeys + x]);
            }

            if (useStringType)
            {
                //delete the first 20 keys added previously
                var keysDeleted = await db.KeyDeleteAsync(keys.Take(20).ToArray());
                ClassicAssert.AreEqual(iterationSize, keysDeleted);

                //try to delete the next 20 keys using a cancellation token
                var sc = new CancellationTokenSource();
                var t = sc.Token;
                ManualResetEventSlim mrObj = new(false);
                var tDeletingK = Task.Run(async () => { await DeleteKeysWithCT([.. keys.Skip(iterationSize).Take(iterationSize)], null, mrObj, t); });

                // send the cancellation so the task throws an exception
                sc.Cancel();
                mrObj.Set();    // TODO: mrObj not needed

                Assert.Throws<OperationCanceledException>(() => tDeletingK.Wait(sc.Token));

                //delete the last keys using a callback
                mrObj.Reset();
                db.KeyDelete(keys.Skip(iterationSize).ToArray(), (ct, result, e) =>
                {
                    ClassicAssert.AreEqual(keys.Length - iterationSize, result);
                    mrObj.Set();
                });
                mrObj.Wait();
                mrObj.Reset();
            }
            else
            {
                //delete with Memory<Byte> type
                var keysDeletedMB = await db.KeyDeleteAsync(keysMemoryByte.Take(20).ToArray());
                ClassicAssert.AreEqual(iterationSize, keysDeletedMB);

                var sc = new CancellationTokenSource();
                var t = sc.Token;
                ManualResetEventSlim mrObj = new(false);

                // try delete using Memory<byte> type
                var tDeletingKeysMB = Task.Run(async () => { _ = await DeleteKeysWithCT(null, [.. keysMemoryByte.Skip(iterationSize).Take(iterationSize)], mrObj, t, true); });
                sc.Cancel();
                mrObj.Set();
                Assert.Throws<OperationCanceledException>(() => tDeletingKeysMB.Wait(sc.Token));

                //delete the last keys with callback and Memory<byte>
                mrObj.Reset();
                db.KeyDelete(keysMemoryByte.Skip(iterationSize).ToArray(), (ct, result, e) =>
                {
                    ClassicAssert.AreEqual(keysMemoryByte.Length - iterationSize, result);
                    mrObj.Set();
                });
                mrObj.Wait();
                mrObj.Reset();
            }

            //check that none of the keys exist
            foreach (var key in keys)
            {
                var result = await db.ExecuteForStringResultAsync("EXISTS", [key]);
                ClassicAssert.AreEqual("0", result);
            }
        }

        private static async Task<long> DeleteKeysWithCT(string[] keys, Memory<byte>[] keysMB, ManualResetEventSlim mreObj, CancellationToken t, bool useMemoryType = false)
        {
            using var db = TestUtils.GetGarnetClient();
            db.Connect();

            //wait until is signaled to proceed
            mreObj.Wait();
            if (!t.IsCancellationRequested)
            {
                long result;
                if (useMemoryType)
                    result = await db.KeyDeleteAsync(keysMB, t);
                else
                    result = await db.KeyDeleteAsync(keys, t);
                return result;
            }
            else
                t.ThrowIfCancellationRequested();

            return 0;
        }

        [Test]
        public async Task MultipleSocketPing([Values] bool useTls)
        {
            var unixSocketPath = "./unix-socket-ping-test.sock";
            var unixSocketEndpoint = new UnixDomainSocketEndPoint(unixSocketPath);
            var tcpEndpoint = new IPEndPoint(IPAddress.Loopback, TestUtils.TestPort);

            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, [unixSocketEndpoint, tcpEndpoint], enableTLS: useTls, unixSocketPath: unixSocketPath);
            server.Start();

            using var db = TestUtils.GetGarnetClient(unixSocketEndpoint, useTLS: useTls);
            await db.ConnectAsync();

            var result = await db.ExecuteForStringResultAsync("PING");
            ClassicAssert.AreEqual("PONG", result);

            using var tcpClient = TestUtils.GetGarnetClient(tcpEndpoint, useTLS: useTls);
            await tcpClient.ConnectAsync();

            result = await db.ExecuteForStringResultAsync("PING");
            ClassicAssert.AreEqual("PONG", result);
        }
    }
}