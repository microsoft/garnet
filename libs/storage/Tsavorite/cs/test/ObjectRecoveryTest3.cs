// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test.recovery.objects
{
    [TestFixture]
    public class ObjectRecoveryTests3
    {
        int iterations;
        string TsavoriteFolderPath { get; set; }

        [SetUp]
        public void Setup()
        {
            TsavoriteFolderPath = TestUtils.MethodTestDir;
            TestUtils.RecreateDirectory(TsavoriteFolderPath);
        }

        [TearDown]
        public void TearDown()
        {
            TestUtils.DeleteDirectory(TsavoriteFolderPath);
        }

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async ValueTask ObjectRecoveryTest3(
            [Values] CheckpointType checkpointType,
            [Values(1000)] int iterations,
            [Values] bool isAsync)
        {
            this.iterations = iterations;
            Prepare(out _, out _, out IDevice log, out IDevice objlog, out TsavoriteKV<MyKey, MyValue> h, out MyContext context);

            var session1 = h.NewSession<MyInput, MyOutput, MyContext, MyFunctions>(new MyFunctions());
            var tokens = Write(session1, context, h, checkpointType);
            Read(session1, context, false, iterations);
            session1.Dispose();

            h.TryInitiateHybridLogCheckpoint(out Guid token, checkpointType);
            h.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            tokens.Add((iterations, token));
            Destroy(log, objlog, h);

            foreach (var item in tokens)
            {
                Prepare(out _, out _, out log, out objlog, out h, out context);

                if (isAsync)
                    await h.RecoverAsync(default, item.Item2);
                else
                    h.Recover(default, item.Item2);

                var session2 = h.NewSession<MyInput, MyOutput, MyContext, MyFunctions>(new MyFunctions());
                Read(session2, context, false, item.Item1);
                session2.Dispose();

                Destroy(log, objlog, h);
            }
        }

        private void Prepare(out string logPath, out string objPath, out IDevice log, out IDevice objlog, out TsavoriteKV<MyKey, MyValue> h, out MyContext context)
        {
            logPath = Path.Combine(TsavoriteFolderPath, $"RecoverTests.log");
            objPath = Path.Combine(TsavoriteFolderPath, $"RecoverTests_HEAP.log");
            log = Devices.CreateLogDevice(logPath);
            objlog = Devices.CreateLogDevice(objPath);
            h = new TsavoriteKV<MyKey, MyValue>
                (1L << 20,
                new LogSettings
                {
                    LogDevice = log,
                    ObjectLogDevice = objlog,
                    SegmentSizeBits = 12,
                    MemorySizeBits = 12,
                    PageSizeBits = 9
                },
                new CheckpointSettings()
                {
                    CheckpointDir = Path.Combine(TsavoriteFolderPath, "check-points")
                },
                new SerializerSettings<MyKey, MyValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyValueSerializer() }
             );
            context = new MyContext();
        }

        private static void Destroy(IDevice log, IDevice objlog, TsavoriteKV<MyKey, MyValue> h)
        {
            // Dispose Tsavorite instance and log
            h.Dispose();
            log.Dispose();
            objlog.Dispose();
        }

        private List<(int, Guid)> Write(ClientSession<MyKey, MyValue, MyInput, MyOutput, MyContext, MyFunctions> session, MyContext context, TsavoriteKV<MyKey, MyValue> store, CheckpointType checkpointType)
        {
            var tokens = new List<(int, Guid)>();
            for (int i = 0; i < iterations; i++)
            {
                var _key = new MyKey { key = i, name = string.Concat(Enumerable.Repeat(i.ToString(), 100)) };
                var value = new MyValue { value = i.ToString() };
                session.Upsert(ref _key, ref value, context, 0);

                if (i % 1000 == 0 && i > 0)
                {
                    store.TryInitiateHybridLogCheckpoint(out Guid token, checkpointType);
                    store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
                    tokens.Add((i, token));
                }
            }
            return tokens;
        }

        private void Read(ClientSession<MyKey, MyValue, MyInput, MyOutput, MyContext, MyFunctions> session, MyContext context, bool delete, int iter)
        {
            for (int i = 0; i < iter; i++)
            {
                var key = new MyKey { key = i, name = string.Concat(Enumerable.Repeat(i.ToString(), 100)) };
                MyInput input = default;
                MyOutput g1 = new();
                var status = session.Read(ref key, ref input, ref g1, context, 0);

                if (status.IsPending)
                {
                    session.CompletePending(true);
                    context.FinalizeRead(ref status, ref g1);
                }

                Assert.IsTrue(status.Found);
                Assert.AreEqual(i.ToString(), g1.value.value);
            }

            if (delete)
            {
                var key = new MyKey { key = 1, name = "1" };
                var input = default(MyInput);
                var output = new MyOutput();
                session.Delete(ref key, context, 0);
                var status = session.Read(ref key, ref input, ref output, context, 0);

                if (status.IsPending)
                {
                    session.CompletePending(true);
                    context.FinalizeRead(ref status, ref output);
                }

                Assert.IsFalse(status.Found);
            }
        }
    }
}