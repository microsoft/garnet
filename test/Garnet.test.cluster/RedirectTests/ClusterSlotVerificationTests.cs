// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    public class ClusterSlotVerificationTests
    {
        BaseCommand[] commands;
        ClusterTestContext context;

        readonly int sourceIndex = 0;
        readonly int targetIndex = 1;
        readonly int otherIndex = 2;

        readonly int iterations = 10;

        private void InitializeCommands()
        {
            commands =
            [
                new GET(),
                new SET(),
                new MGET(),
                new MSET(),
                new PFADD(),
                new PFCOUNT(),
                new PFMERGE(),
                new SETBIT(),
                new GETBIT(),
                new BITCOUNT(),
                new BITPOS(),
                new BITOP(),
                new BITFIELD(),
                new BITFIELD_RO(),
                new SETRANGE(),
                new GETRANGE(),
                new INCR(),
                new APPEND(),
                new STRLEN(),
            ];
        }

        private void ConfigureSlotForMigration()
        {
            var srcEndpoint = context.clusterTestUtils.GetEndPoint(sourceIndex).ToIPEndPoint();
            var trgtEndpoint = context.clusterTestUtils.GetEndPoint(targetIndex).ToIPEndPoint();
            var srcNodeId = context.clusterTestUtils.ClusterMyId(sourceIndex, logger: context.logger);
            var trgtNodeId = context.clusterTestUtils.ClusterMyId(targetIndex, logger: context.logger);

            var slot = HashSlotUtils.HashSlot(BaseCommand.HashTag.ToArray());
            // Set slot to MIGRATING state
            var resp = context.clusterTestUtils.SetSlot(srcEndpoint, slot, "MIGRATING", trgtNodeId);
            Assert.AreEqual("OK", resp);

            // Set slot to IMPORTING state
            resp = context.clusterTestUtils.SetSlot(trgtEndpoint, slot, "IMPORTING", srcNodeId);
            Assert.AreEqual("OK", resp);
        }

        private void ResetSlot()
        {
            var srcEndpoint = context.clusterTestUtils.GetEndPoint(sourceIndex).ToIPEndPoint();
            var trgtEndpoint = context.clusterTestUtils.GetEndPoint(targetIndex).ToIPEndPoint();
            var srcNodeId = context.clusterTestUtils.ClusterMyId(sourceIndex, logger: context.logger);
            var trgtNodeId = context.clusterTestUtils.ClusterMyId(targetIndex, logger: context.logger);

            var slot = HashSlotUtils.HashSlot(BaseCommand.HashTag.ToArray());
            // Set slot to STABLE state
            var resp = context.clusterTestUtils.SetSlot(srcEndpoint, slot, "STABLE", "");
            Assert.AreEqual("OK", resp);

            // Set slot to STABLE state
            resp = context.clusterTestUtils.SetSlot(trgtEndpoint, slot, "STABLE", "");
            Assert.AreEqual("OK", resp);
        }

        [SetUp]
        public virtual void Setup()
        {
            context = new ClusterTestContext();
            context.Setup([]);

            context.CreateInstances(3);
            context.CreateConnection();

            // Assign all slots to node 0
            context.clusterTestUtils.AddSlotsRange(sourceIndex, [(0, 16383)], logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(sourceIndex, 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(targetIndex, 2, logger: context.logger);

            context.clusterTestUtils.Meet(sourceIndex, targetIndex, logger: context.logger);
            context.clusterTestUtils.WaitUntilNodeIsKnown(targetIndex, sourceIndex, logger: context.logger);
            InitializeCommands();
        }

        [TearDown]
        public virtual void TearDown()
        {
            context?.TearDown();
        }

        [Test, Order(1)]
        [Category("SLOT_VERIFY")]
        public void ClusterCLUSTERDOWNTest()
        {
            var requestNodeIndex = otherIndex;
            foreach (var command in commands)
            {
                for (var i = 0; i < iterations; i++)
                    SERedisClusterDown(command);
            }

            foreach (var command in commands)
            {
                for (var i = 0; i < iterations; i++)
                    GarnetClientSessionClusterDown(command);
            }

            void SERedisClusterDown(BaseCommand command)
            {
                try
                {
                    context.clusterTestUtils.GetServer(requestNodeIndex).Execute(command.Command, command.GetSingleSlotRequest());
                }
                catch (Exception ex)
                {
                    Assert.AreEqual("CLUSTERDOWN Hash slot not served", ex.Message, command.Command);
                    return;
                }
                Assert.Fail("Should not reach here", command.Command);
            }

            void GarnetClientSessionClusterDown(BaseCommand command)
            {
                var client = context.clusterTestUtils.GetGarnetClientSession(requestNodeIndex);
                try
                {
                    client.ExecuteAsync(command.GetSingleSlotRequestWithCommand).GetAwaiter().GetResult();
                }
                catch (Exception ex)
                {
                    Assert.AreEqual("CLUSTERDOWN Hash slot not served", ex.Message, command.Command);
                    return;
                }
                Assert.Fail("Should not reach here", command.Command);
            }
        }

        [Test, Order(2)]
        [Category("SLOT_VERIFY")]
        public void ClusterOKTest()
        {
            var requestNodeIndex = sourceIndex;
            foreach (var command in commands)
            {
                for (var i = 0; i < iterations; i++)
                    SERedisOKTest(command);
            }

            foreach (var command in commands)
            {
                for (var i = 0; i < iterations; i++)
                    GarnetClientSessionOK(command);
            }

            void SERedisOKTest(BaseCommand command)
            {
                try
                {
                    _ = context.clusterTestUtils.GetServer(requestNodeIndex).Execute(command.Command, command.GetSingleSlotRequest());
                }
                catch (Exception ex)
                {
                    Assert.Fail(ex.Message, command.Command);
                }
            }

            void GarnetClientSessionOK(BaseCommand command)
            {
                var client = context.clusterTestUtils.GetGarnetClientSession(requestNodeIndex);
                try
                {
                    if (command.ArrayResponse)
                        _ = client.ExecuteForArrayAsync(command.GetSingleSlotRequestWithCommand).GetAwaiter().GetResult();
                    else
                        _ = client.ExecuteAsync(command.GetSingleSlotRequestWithCommand).GetAwaiter().GetResult();
                }
                catch (Exception ex)
                {
                    Assert.Fail(command.Command, ex, command.Command);
                }
            }
        }

        [Test, Order(3)]
        [Category("SLOT_VERIFY")]
        public void ClusterCROSSSLOTTest()
        {
            var requestNodeIndex = sourceIndex;
            foreach (var command in commands)
            {
                for (var i = 0; i < iterations; i++)
                    SERedisCrossslotTest(command);
            }

            foreach (var command in commands)
            {
                for (var i = 0; i < iterations; i++)
                    GarnetClientSessionCrossslotTest(command);
            }

            void SERedisCrossslotTest(BaseCommand command)
            {
                if (!command.IsArrayCommand)
                    return;
                try
                {
                    _ = context.clusterTestUtils.GetServer(requestNodeIndex).Execute(command.Command, command.GetCrossSlotRequest());
                }
                catch (Exception ex)
                {
                    Assert.AreEqual("CROSSSLOT Keys in request do not hash to the same slot", ex.Message, command.Command);
                    return;
                }
                Assert.Fail("Should not reach here", command.Command);
            }

            void GarnetClientSessionCrossslotTest(BaseCommand command)
            {
                if (!command.IsArrayCommand)
                    return;
                var client = context.clusterTestUtils.GetGarnetClientSession(requestNodeIndex);
                try
                {
                    client.ExecuteAsync(command.GetCrossslotRequestWithCommand).GetAwaiter().GetResult();
                }
                catch (Exception ex)
                {
                    Assert.AreEqual("CROSSSLOT Keys in request do not hash to the same slot", ex.Message, command.Command);
                    return;
                }
                Assert.Fail("Should not reach here", command.Command);
            }
        }

        [Test, Order(4)]
        [Category("SLOT_VERIFY")]
        public void ClusterMOVEDTest()
        {
            var requestNodeIndex = targetIndex;
            var address = "127.0.0.1";
            var port = context.clusterTestUtils.GetPortFromNodeIndex(sourceIndex);

            foreach (var command in commands)
            {
                for (var i = 0; i < iterations; i++)
                    SERedisMOVEDTest(command);
            }

            foreach (var command in commands)
            {
                for (var i = 0; i < iterations; i++)
                    GarnetClientSessionMOVEDTest(command);
            }

            void SERedisMOVEDTest(BaseCommand command)
            {
                try
                {
                    context.clusterTestUtils.GetServer(requestNodeIndex).Execute(command.Command, command.GetSingleSlotRequest(), CommandFlags.NoRedirect);
                }
                catch (Exception ex)
                {
                    Assert.IsTrue(ex.Message.StartsWith("Key has MOVED"), command.Command);
                    var tokens = ex.Message.Split(' ');
                    Assert.IsTrue(tokens.Length > 10 && tokens[2].Equals("MOVED"), command.Command);

                    var _address = tokens[5].Split(':')[0];
                    var _port = int.Parse(tokens[5].Split(':')[1]);
                    var _slot = int.Parse(tokens[8]);
                    Assert.AreEqual(address, _address, command.Command);
                    Assert.AreEqual(port, _port, command.Command);
                    Assert.AreEqual(command.GetSlot, _slot, command.Command);
                    return;
                }
                Assert.Fail("Should not reach here", command.Command);
            }

            void GarnetClientSessionMOVEDTest(BaseCommand command)
            {
                var client = context.clusterTestUtils.GetGarnetClientSession(requestNodeIndex);
                try
                {
                    client.ExecuteAsync(command.GetSingleSlotRequestWithCommand).GetAwaiter().GetResult();
                }
                catch (Exception ex)
                {
                    Assert.AreEqual($"MOVED {command.GetSlot} {address}:{port}", ex.Message, command.Command);
                    return;
                }
                Assert.Fail("Should not reach here", command.Command);
            }
        }

        [Test, Order(5)]
        [Category("SLOT_VERIFY")]
        public void ClusterASKTest()
        {
            var requestNodeIndex = sourceIndex;
            var address = "127.0.0.1";
            var port = context.clusterTestUtils.GetPortFromNodeIndex(targetIndex);
            ConfigureSlotForMigration();

            foreach (var command in commands)
            {
                for (var i = 0; i < iterations; i++)
                    SERedisASKTest(command);
            }

            foreach (var command in commands)
            {
                for (var i = 0; i < iterations; i++)
                    GarnetClientSessionASKTest(command);
            }

            void SERedisASKTest(BaseCommand command)
            {
                RedisResult result = default;
                try
                {
                    result = context.clusterTestUtils.GetServer(requestNodeIndex).Execute(command.Command, command.GetSingleSlotRequest(), CommandFlags.NoRedirect);
                }
                catch (Exception ex)
                {
                    var tokens = ex.Message.Split(' ');
                    Assert.IsTrue(tokens.Length > 10 && tokens[0].Equals("Endpoint"), command.Command);

                    var _address = tokens[1].Split(':')[0];
                    var _port = int.Parse(tokens[1].Split(':')[1]);
                    var _slot = int.Parse(tokens[4]);
                    Assert.AreEqual(address, _address, command.Command);
                    Assert.AreEqual(port, _port, command.Command);
                    Assert.AreEqual(command.GetSlot, _slot, command.Command);
                    return;
                }
                Assert.Fail("Should not reach here", command.Command);
            }

            void GarnetClientSessionASKTest(BaseCommand command)
            {
                var client = context.clusterTestUtils.GetGarnetClientSession(requestNodeIndex);
                try
                {
                    client.ExecuteAsync(command.GetSingleSlotRequestWithCommand).GetAwaiter().GetResult();
                }
                catch (Exception ex)
                {
                    Assert.AreEqual($"ASK {command.GetSlot} {address}:{port}", ex.Message, command.Command);
                    return;
                }
                Assert.Fail("Should not reach here", command.Command);
            }
        }

        [Test, Order(6)]
        [Category("SLOT_VERIFY")]
        public void ClusterTRYAGAINTest()
        {
            var requestNodeIndex = sourceIndex;
            foreach (var command in commands)
            {
                for (var i = 0; i < iterations; i++)
                    SERedisTRYAGAINTest(command);
            }

            void SERedisTRYAGAINTest(BaseCommand command)
            {
                if (!command.IsArrayCommand)
                    return;

                foreach (var setup in command.SetupSingleSlotRequest())
                {
                    var setupParameters = setup.Slice(1).ToArray();
                    try
                    {
                        _ = context.clusterTestUtils.GetServer(requestNodeIndex).Execute(setup[0], setupParameters, CommandFlags.NoRedirect);
                    }
                    catch (Exception ex)
                    {
                        context.logger?.LogError(ex, "Failed executing setup {command}", command.Command);
                    }
                }

                ConfigureSlotForMigration();
                try
                {
                    context.clusterTestUtils.GetServer(requestNodeIndex).Execute(command.Command, command.GetSingleSlotRequest(), CommandFlags.NoRedirect);
                }
                catch (Exception ex)
                {
                    Assert.AreEqual("TRYAGAIN Multiple keys request during rehashing of slot", ex.Message, command.Command);
                    return;
                }
                finally
                {
                    ResetSlot();
                    try
                    {
                        _ = context.clusterTestUtils.GetServer(requestNodeIndex).Execute("DEL", command.GetSingleSlotKeys.ToArray(), CommandFlags.NoRedirect);
                    }
                    catch (Exception ex)
                    {
                        context.logger?.LogError(ex, "Failed executing cleanup {command}", command.Command);
                    }
                }

                Assert.Fail("Should not reach here", command.Command);
            }
        }
    }
}