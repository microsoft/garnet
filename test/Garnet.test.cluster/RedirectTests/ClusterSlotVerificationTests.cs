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

        private void InitializeCommands()
        {
            commands =
            [

            ];

            commands =
            [
                new GET(),
                new SET(),
                new MGET(),
                new MSET()
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
                SERedisClusterDown(command);
            }

            foreach (var command in commands)
            {
                GarnetClientSessionClusterDown(command);
            }

            void SERedisClusterDown(BaseCommand command)
            {
                try
                {
                    context.clusterTestUtils.GetServer(requestNodeIndex).Execute(command.ToString, command.GetSingleSlotRequest());
                }
                catch (Exception ex)
                {
                    Assert.AreEqual("CLUSTERDOWN Hash slot not served", ex.Message);
                }
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
                    Assert.AreEqual("CLUSTERDOWN Hash slot not served", ex.Message);
                }
            }
        }

        [Test, Order(2)]
        [Category("SLOT_VERIFY")]
        public void ClusterOKTest()
        {
            var requestNodeIndex = sourceIndex;
            foreach (var command in commands)
            {
                SERedisOKTest(command);
            }

            foreach (var command in commands)
            {
                GarnetClientSessionOK(command);
            }

            void SERedisOKTest(BaseCommand command)
            {
                try
                {
                    _ = context.clusterTestUtils.GetServer(requestNodeIndex).Execute(command.ToString, command.GetSingleSlotRequest());
                }
                catch (Exception ex)
                {
                    Assert.Fail(ex.Message);
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
                    Assert.Fail(command.ToString, ex);
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
                SERedisCrossslotTest(command);
            }

            foreach (var command in commands)
            {
                GarnetClientSessionCrossslotTest(command);
            }

            void SERedisCrossslotTest(BaseCommand command)
            {
                try
                {
                    if (command.IsArrayCommand)
                        _ = context.clusterTestUtils.GetServer(requestNodeIndex).Execute(command.ToString, command.GetCrossSlotRequest());
                }
                catch (Exception ex)
                {
                    Assert.AreEqual("CROSSSLOT Keys in request do not hash to the same slot", ex.Message);
                }
            }

            void GarnetClientSessionCrossslotTest(BaseCommand command)
            {
                var client = context.clusterTestUtils.GetGarnetClientSession(requestNodeIndex);
                try
                {
                    if (command.IsArrayCommand)
                        client.ExecuteAsync(command.GetCrossslotRequestWithCommand).GetAwaiter().GetResult();
                }
                catch (Exception ex)
                {
                    Assert.AreEqual("CROSSSLOT Keys in request do not hash to the same slot", ex.Message);
                }
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
                SERedisMOVEDTest(command);
            }

            foreach (var command in commands)
            {
                GarnetClientSessionMOVEDTest(command);
            }

            void SERedisMOVEDTest(BaseCommand command)
            {
                try
                {
                    context.clusterTestUtils.GetServer(requestNodeIndex).Execute(command.ToString, command.GetSingleSlotRequest(), CommandFlags.NoRedirect);
                }
                catch (Exception ex)
                {
                    Assert.IsTrue(ex.Message.StartsWith("Key has MOVED"));
                    var tokens = ex.Message.Split(' ');
                    Assert.IsTrue(tokens.Length > 10 && tokens[2].Equals("MOVED"));

                    var _address = tokens[5].Split(':')[0];
                    var _port = int.Parse(tokens[5].Split(':')[1]);
                    var _slot = int.Parse(tokens[8]);
                    Assert.AreEqual(address, _address);
                    Assert.AreEqual(port, _port);
                    Assert.AreEqual(command.GetSlot, _slot);
                }
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
                    Assert.AreEqual($"MOVED {command.GetSlot} {address}:{port}", ex.Message);
                }
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
                SERedisASKTest(command);
            }

            foreach (var command in commands)
            {
                GarnetClientSessionASKTest(command);
            }

            void SERedisASKTest(BaseCommand command)
            {
                RedisResult result = default;
                try
                {
                    result = context.clusterTestUtils.GetServer(requestNodeIndex).Execute(command.ToString, command.GetSingleSlotRequest(), CommandFlags.NoRedirect);
                }
                catch (Exception ex)
                {
                    var tokens = ex.Message.Split(' ');
                    Assert.IsTrue(tokens.Length > 10 && tokens[0].Equals("Endpoint"));

                    var _address = tokens[1].Split(':')[0];
                    var _port = int.Parse(tokens[1].Split(':')[1]);
                    var _slot = int.Parse(tokens[4]);
                    Assert.AreEqual(address, _address);
                    Assert.AreEqual(port, _port);
                    Assert.AreEqual(command.GetSlot, _slot);
                }
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
                    Assert.AreEqual($"ASK {command.GetSlot} {address}:{port}", ex.Message);
                }
            }
        }

        [Test, Order(6)]
        [Category("SLOT_VERIFY")]
        public void ClusterTRYAGAINTest()
        {
            var requestNodeIndex = sourceIndex;
            foreach (var command in commands)
            {
                SERedisTRYAGAINTest(command);
            }

            void SERedisTRYAGAINTest(BaseCommand command)
            {
                if (!command.IsArrayCommand)
                    return;

                var setup = command.SetupSingleSlotRequest();
                var setupParameters = setup.Slice(1).ToArray();
                try
                {
                    _ = context.clusterTestUtils.GetServer(requestNodeIndex).Execute(setup[0], setupParameters, CommandFlags.NoRedirect);
                }
                catch (Exception ex)
                {
                    context.logger?.LogError(ex, "Failed executing setup");
                }

                ConfigureSlotForMigration();
                try
                {
                    context.clusterTestUtils.GetServer(requestNodeIndex).Execute(command.ToString, command.GetSingleSlotRequest(), CommandFlags.NoRedirect);
                }
                catch (Exception ex)
                {
                    Assert.AreEqual("TRYAGAIN Multiple keys request during rehashing of slot", ex.Message);
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
                        context.logger?.LogError(ex, "Failed executing cleanup");
                    }
                }
            }
        }
    }
}
