// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Allure.NUnit;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    public sealed class BaseCommandComparer : IEqualityComparer<BaseCommand>
    {
        public static readonly BaseCommandComparer Instance = new();

        public bool Equals(BaseCommand x, BaseCommand y) => x.Command.Equals(y.Command);

        public unsafe int GetHashCode([DisallowNull] BaseCommand obj) => obj.Command.GetHashCode();
    }

    [AllureNUnit]
    [TestFixture]
    [NonParallelizable]
    public class ClusterSlotVerificationTests : AllureTestBase
    {
        static readonly HashSet<BaseCommand> TestCommands = new(BaseCommandComparer.Instance)
            {
                new GET(),
                new SET(),
                new MGET(),
                new MSET(),
                new GETEX(),
                new GETSET(),
                new SETNX(),
                new SUBSTR(),
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
                new INCRBYFLOAT(),
                new APPEND(),
                new STRLEN(),
                new RENAME(),
                new DEL(),
                new GETDEL(),
                new EXISTS(),
                new PERSIST(),
                new EXPIRE(),
                new TTL(),
                new DUMP(),
                new RESTORE(),
                new SDIFFSTORE(),
                new SDIFF(),
                new SMOVE(),
                new SUNIONSTORE(),
                new SUNION(),
                new SINTERSTORE(),
                new SINTER(),
                new LMOVE(),
                new EVAL(),
                new EVALSHA(),
                new LPUSH(),
                new LPOP(),
                new LMPOP(),
                new ZMPOP(),
                new BLPOP(),
                new BLMOVE(),
                new BRPOPLPUSH(),
                new LLEN(),
                new LTRIM(),
                new LRANGE(),
                new LINDEX(),
                new LINSERT(),
                new LREM(),
                new RPOPLPUSH(),
                new LSET(),
                new SADD(),
                new SREM(),
                new SCARD(),
                new SMEMBERS(),
                new SISMEMBER(),
                new SMISMEMBER(),
                new SPOP(),
                new SRANDMEMBER(),
                new GEOADD(),
                new GEOHASH(),
                new GEOSEARCHSTORE(),
                new ZADD(),
                new ZREM(),
                new ZCARD(),
                new ZRANGE(),
                new ZREVRANGEBYLEX(),
                new ZRANGESTORE(),
                new ZSCORE(),
                new ZMSCORE(),
                new ZPOPMAX(),
                new ZCOUNT(),
                new ZLEXCOUNT(),
                new ZINCRBY(),
                new ZRANK(),
                new ZREMRANGEBYRANK(),
                new ZRANDMEMBER(),
                new ZDIFF(),
                new ZDIFFSTORE(),
                new ZINTER(),
                new ZINTERCARD(),
                new ZINTERSTORE(),
                new ZUNION(),
                new ZUNIONSTORE(),
                new HEXPIRE(),
                new ZEXPIRE(),
                new ZPEXPIRE(),
                new ZEXPIREAT(),
                new ZPEXPIREAT(),
                new ZTTL(),
                new ZPTTL(),
                new ZEXPIRETIME(),
                new ZPEXPIRETIME(),
                new ZPERSIST(),
                new ZCOLLECT(),
                new HSET(),
                new HGET(),
                new HGETALL(),
                new HMGET(),
                new HRANDFIELD(),
                new HLEN(),
                new HSTRLEN(),
                new HDEL(),
                new HEXISTS(),
                new HKEYS(),
                new HINCRBY(),
                new HEXPIRE(),
                new HPEXPIRE(),
                new HEXPIREAT(),
                new HPEXPIREAT(),
                new HTTL(),
                new HPTTL(),
                new HEXPIRETIME(),
                new HPEXPIRETIME(),
                new HPERSIST(),
                new HCOLLECT(),
                new CLUSTERGETPROC(),
                new CLUSTERSETPROC(),
                new WATCH(),
                new WATCHMS(),
                new WATCHOS(),
                new SINTERCARD(),
                new LCS(),
            };

        ClusterTestContext context;
        readonly int sourceIndex = 0;
        readonly int targetIndex = 1;
        readonly int otherIndex = 2;
        readonly int iterations = 3;

        private void AssertSlotsNotAssigned(int requestNodeIndex)
        {
            var config = context.clusterTestUtils.ClusterNodes(requestNodeIndex);
            for (var i = 0; i < 16384; i++)
                ClassicAssert.IsNull(config.GetBySlot(i));
        }

        /// <summary>
        /// Issue SetSlot commands to configure slot for migration
        /// </summary>
        private void ConfigureSlotForMigration()
        {
            var srcEndpoint = context.clusterTestUtils.GetEndPoint(sourceIndex).ToIPEndPoint();
            var trgtEndpoint = context.clusterTestUtils.GetEndPoint(targetIndex).ToIPEndPoint();
            var srcNodeId = context.clusterTestUtils.ClusterMyId(sourceIndex, logger: context.logger);
            var trgtNodeId = context.clusterTestUtils.ClusterMyId(targetIndex, logger: context.logger);

            var slot = HashSlotUtils.HashSlot(BaseCommand.HashTag.ToArray());
            // Set slot to MIGRATING state
            var resp = context.clusterTestUtils.SetSlot(srcEndpoint, slot, "MIGRATING", trgtNodeId);
            ClassicAssert.AreEqual("OK", resp);

            // Set slot to IMPORTING state
            resp = context.clusterTestUtils.SetSlot(trgtEndpoint, slot, "IMPORTING", srcNodeId);
            ClassicAssert.AreEqual("OK", resp);
        }

        /// <summary>
        /// Reset slot to stable state
        /// </summary>
        private void ResetSlot()
        {
            var srcEndpoint = context.clusterTestUtils.GetEndPoint(sourceIndex).ToIPEndPoint();
            var trgtEndpoint = context.clusterTestUtils.GetEndPoint(targetIndex).ToIPEndPoint();
            var srcNodeId = context.clusterTestUtils.ClusterMyId(sourceIndex, logger: context.logger);
            var trgtNodeId = context.clusterTestUtils.ClusterMyId(targetIndex, logger: context.logger);

            var slot = HashSlotUtils.HashSlot(BaseCommand.HashTag.ToArray());
            // Set slot to STABLE state
            var resp = context.clusterTestUtils.SetSlot(srcEndpoint, slot, "STABLE", "");
            ClassicAssert.AreEqual("OK", resp);

            // Set slot to STABLE state
            resp = context.clusterTestUtils.SetSlot(trgtEndpoint, slot, "STABLE", "");
            ClassicAssert.AreEqual("OK", resp);
        }

        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            context = new ClusterTestContext();
            context.Setup([]);

            context.CreateInstances(3, enableLua: true);

            context.RegisterCustomTxn(
                "CLUSTERGETPROC",
                () => new TestClusterReadOnlyCustomTxn(),
                new RespCommandsInfo { Arity = TestClusterReadOnlyCustomTxn.Arity });

            context.RegisterCustomTxn(
                "CLUSTERSETPROC",
                () => new TestClusterReadWriteCustomTxn(),
                new RespCommandsInfo { Arity = TestClusterReadWriteCustomTxn.Arity });

            context.CreateConnection();

            // Assign all slots to node 0
            var resp = context.clusterTestUtils.AddSlotsRange(sourceIndex, [(0, 16383)], logger: context.logger);
            ClassicAssert.AreEqual("OK", resp);
            context.clusterTestUtils.SetConfigEpoch(sourceIndex, 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(targetIndex, 2, logger: context.logger);

            context.clusterTestUtils.Meet(sourceIndex, targetIndex, logger: context.logger);
            context.clusterTestUtils.WaitUntilNodeIsKnown(targetIndex, sourceIndex, logger: context.logger);
        }

        [OneTimeTearDown]
        public virtual void OneTimeTearDown()
        {
            context?.TearDown();
        }

        string ClusterState()
        {
            var clusterStatus = $"{GetNodeInfo(context.clusterTestUtils.ClusterNodes(sourceIndex))}\n" +
                $"{GetNodeInfo(context.clusterTestUtils.ClusterNodes(targetIndex))}\n" +
                $"{GetNodeInfo(context.clusterTestUtils.ClusterNodes(otherIndex))}\n";

            static string GetNodeInfo(ClusterConfiguration nodeConfig)
            {
                var output = $"[{nodeConfig.Origin}]";

                foreach (var node in nodeConfig.Nodes)
                    output += $"\n\t{node.Raw}";
                return output;
            }
            return clusterStatus;
        }

        [Test, Order(1), NonParallelizable]
        [Category("SLOT_VERIFY")]
        public void ClusterCLUSTERDOWNTest()
        {
            foreach (var command in TestCommands)
            {
                var requestNodeIndex = otherIndex;

                Initialize(command);

                for (var i = 0; i < iterations; i++)
                    SERedisClusterDown(command);

                for (var i = 0; i < iterations; i++)
                    GarnetClientSessionClusterDown(command);

                void SERedisClusterDown(BaseCommand command)
                {
                    AssertSlotsNotAssigned(requestNodeIndex);
                    var ex = Assert.Throws<RedisServerException>(() => context.clusterTestUtils.GetServer(requestNodeIndex).Execute(command.Command, command.GetSingleSlotRequest()),
                        $"Expected exception was not thrown. Command: {command.Command} \n{ClusterState()}");
                    ClassicAssert.AreEqual("CLUSTERDOWN Hash slot not served", ex.Message, command.Command);
                }

                void GarnetClientSessionClusterDown(BaseCommand command)
                {
                    var client = context.clusterTestUtils.GetGarnetClientSession(requestNodeIndex);
                    AssertSlotsNotAssigned(requestNodeIndex);
                    var ex = Assert.Throws<Exception>(() => client.ExecuteAsync(command.GetSingleSlotRequestWithCommand).GetAwaiter().GetResult(),
                        $"Expected exception was not thrown. Command: {command.Command} \n{ClusterState()}");
                    ClassicAssert.AreEqual("CLUSTERDOWN Hash slot not served", ex.Message, command.Command);
                }
            }
        }

        [Test, Order(2), NonParallelizable]
        [Category("SLOT_VERIFY")]
        public void ClusterOKTest()
        {
            foreach (var command in TestCommands)
            {
                var requestNodeIndex = sourceIndex;

                Initialize(command);
                for (var i = 0; i < iterations; i++)
                    SERedisOKTest(command);

                for (var i = 0; i < iterations; i++)
                    GarnetClientSessionOK(command);

                try
                {
                    var resp = (string)context.clusterTestUtils.GetServer(requestNodeIndex).Execute("DEL", [.. command.GetSingleSlotKeys], CommandFlags.NoRedirect);
                }
                catch (Exception ex)
                {
                    context.logger?.LogError(ex, "Failed executing cleanup {command}", command.Command);
                    Assert.Fail($"Failed executing cleanup. Command: {command.Command} \n{ClusterState()}");
                }

                void SERedisOKTest(BaseCommand command)
                {
                    try
                    {
                        _ = context.clusterTestUtils.GetServer(requestNodeIndex).Execute(command.Command, command.GetSingleSlotRequest());
                    }
                    catch (Exception ex)
                    {
                        if (!command.RequiresExistingKey)
                            Assert.Fail($"{ex.Message}. Command: {command.Command} \n{ClusterState()}");
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
                        if (!command.RequiresExistingKey)
                            Assert.Fail($"{ex.Message}. Command: {command.Command} \n{ClusterState()}");
                    }
                }
            }
        }

        [Test, Order(3)]
        [Category("SLOT_VERIFY")]
        public void ClusterCROSSSLOTTest()
        {
            foreach (var command in TestCommands)
            {
                var requestNodeIndex = sourceIndex;

                Initialize(command);

                for (var i = 0; i < iterations; i++)
                    SERedisCrossslotTest(command);

                for (var i = 0; i < iterations; i++)
                    GarnetClientSessionCrossslotTest(command);

                void SERedisCrossslotTest(BaseCommand command)
                {
                    if (!command.IsArrayCommand)
                        return;
                    var ex = Assert.Throws<RedisServerException>(() => context.clusterTestUtils.GetServer(requestNodeIndex).Execute(command.Command, command.GetCrossSlotRequest()),
                        $"Expected exception was not thrown. Command: {command.Command} \n{ClusterState()}");
                    ClassicAssert.AreEqual("CROSSSLOT Keys in request do not hash to the same slot", ex.Message, command.Command);
                }

                void GarnetClientSessionCrossslotTest(BaseCommand command)
                {
                    if (!command.IsArrayCommand)
                        return;
                    var client = context.clusterTestUtils.GetGarnetClientSession(requestNodeIndex);
                    var ex = Assert.Throws<Exception>(() => client.ExecuteAsync(command.GetCrossslotRequestWithCommand).GetAwaiter().GetResult(),
                        $"Expected exception was not thrown. Command: {command.Command} \n{ClusterState()}");
                    ClassicAssert.AreEqual("CROSSSLOT Keys in request do not hash to the same slot", ex.Message, command.Command);
                }
            }
        }

        [Test, Order(4), NonParallelizable]
        [Category("SLOT_VERIFY")]
        public void ClusterMOVEDTest()
        {
            foreach (var command in TestCommands)
            {
                var requestNodeIndex = targetIndex;
                var address = "127.0.0.1";
                var port = context.clusterTestUtils.GetPortFromNodeIndex(sourceIndex);

                Initialize(command);

                for (var i = 0; i < iterations; i++)
                    SERedisMOVEDTest(command);

                for (var i = 0; i < iterations; i++)
                    GarnetClientSessionMOVEDTest(command);

                void SERedisMOVEDTest(BaseCommand command)
                {
                    var ex = Assert.Throws<RedisServerException>(() => context.clusterTestUtils.GetServer(requestNodeIndex).Execute(command.Command, command.GetSingleSlotRequest(), CommandFlags.NoRedirect),
                        $"Expected exception was not thrown. Command: {command.Command} \n{ClusterState()}");
                    ClassicAssert.IsTrue(ex.Message.StartsWith("Key has MOVED"), command.Command);
                    var tokens = ex.Message.Split(' ');
                    ClassicAssert.IsTrue(tokens.Length > 10 && tokens[2].Equals("MOVED"), command.Command);

                    var _address = tokens[5].Split(':')[0];
                    var _port = int.Parse(tokens[5].Split(':')[1]);
                    var _slot = int.Parse(tokens[8]);
                    ClassicAssert.AreEqual(address, _address, command.Command);
                    ClassicAssert.AreEqual(port, _port, command.Command);
                    ClassicAssert.AreEqual(command.GetSlot, _slot, command.Command);
                }

                void GarnetClientSessionMOVEDTest(BaseCommand command)
                {
                    var client = context.clusterTestUtils.GetGarnetClientSession(requestNodeIndex);
                    var ex = Assert.Throws<Exception>(() => client.ExecuteAsync(command.GetSingleSlotRequestWithCommand).GetAwaiter().GetResult(),
                        $"Expected exception was not thrown. Command: {command.Command} \n{ClusterState()}");
                    ClassicAssert.AreEqual($"MOVED {command.GetSlot} {address}:{port}", ex.Message, command.Command);
                }
            }
        }

        [Test, Order(5), NonParallelizable]
        [Category("SLOT_VERIFY")]
        public void ClusterASKTest()
        {
            foreach (var command in TestCommands)
            {
                var requestNodeIndex = sourceIndex;
                var address = "127.0.0.1";
                var port = context.clusterTestUtils.GetPortFromNodeIndex(targetIndex);

                Initialize(command);

                ConfigureSlotForMigration();

                try
                {
                    for (var i = 0; i < iterations; i++)
                        SERedisASKTest(command);

                    for (var i = 0; i < iterations; i++)
                        GarnetClientSessionASKTest(command);
                }
                finally
                {
                    ResetSlot();
                    try
                    {
                        var resp = (string)context.clusterTestUtils.GetServer(requestNodeIndex).Execute("DEL", [.. command.GetSingleSlotKeys], CommandFlags.NoRedirect);
                    }
                    catch (Exception ex)
                    {
                        context.logger?.LogError(ex, "Failed executing cleanup {command}", command.Command);
                        Assert.Fail($"Failed executing cleanup. Command: {command.Command} \n{ClusterState()}");
                    }
                }

                void SERedisASKTest(BaseCommand command)
                {
                    var ex = Assert.Throws<RedisConnectionException>(() => context.clusterTestUtils.GetServer(requestNodeIndex).Execute(command.Command, command.GetSingleSlotRequest(), CommandFlags.NoRedirect),
                        $"Expected exception was not thrown. Command: {command.Command} \n{ClusterState()}");
                    var tokens = ex.Message.Split(' ');
                    ClassicAssert.IsTrue(tokens.Length > 10 && tokens[0].Equals("Endpoint"), command.Command + " => " + ex.Message);

                    var _address = tokens[1].Split(':')[0];
                    var _port = int.Parse(tokens[1].Split(':')[1]);
                    var _slot = int.Parse(tokens[4]);
                    ClassicAssert.AreEqual(address, _address, command.Command);
                    ClassicAssert.AreEqual(port, _port, command.Command);
                    ClassicAssert.AreEqual(command.GetSlot, _slot, command.Command);
                }

                void GarnetClientSessionASKTest(BaseCommand command)
                {
                    var client = context.clusterTestUtils.GetGarnetClientSession(requestNodeIndex);
                    var ex = Assert.Throws<Exception>(() => client.ExecuteAsync(command.GetSingleSlotRequestWithCommand).GetAwaiter().GetResult(),
                        $"Expected exception was not thrown. Command: {command.Command} \n{ClusterState()}");
                    ClassicAssert.AreEqual($"ASK {command.GetSlot} {address}:{port}", ex.Message, command.Command);
                }
            }
        }

        [Test, Order(6), NonParallelizable]
        [Category("SLOT_VERIFY")]
        public void ClusterTRYAGAINTest()
        {
            foreach (var command in TestCommands)
            {
                var requestNodeIndex = sourceIndex;

                Initialize(command);

                for (var i = 0; i < iterations; i++)
                    SERedisTRYAGAINTest(command);

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
                            Assert.Fail($"Failed executing setup. Command: {command.Command} \n{ClusterState()}");
                        }
                    }

                    ConfigureSlotForMigration();
                    try
                    {
                        var ex = Assert.Throws<RedisServerException>(() => context.clusterTestUtils.GetServer(requestNodeIndex).Execute(command.Command, command.GetSingleSlotRequest(), CommandFlags.NoRedirect),
                        $"Expected exception was not thrown. Command: {command.Command} \n{ClusterState()}");
                        ClassicAssert.AreEqual("TRYAGAIN Multiple keys request during rehashing of slot", ex.Message, command.Command, $"\n{ClusterState()}");
                    }
                    finally
                    {
                        ResetSlot();
                        try
                        {
                            _ = context.clusterTestUtils.GetServer(requestNodeIndex).Execute("DEL", [.. command.GetSingleSlotKeys], CommandFlags.NoRedirect);
                        }
                        catch (Exception ex)
                        {
                            context.logger?.LogError(ex, "Failed executing cleanup {command}", command.Command);
                            Assert.Fail($"Failed executing cleanup. Command: {command.Command} \n{ClusterState()}");
                        }
                    }
                }
            }
        }

        private void Initialize(BaseCommand cmd)
        {
            var server = context.clusterTestUtils.GetServer(sourceIndex);

            foreach (var initCmd in cmd.Initialize())
            {
                var c = initCmd.Array[initCmd.Offset];
                var rest = initCmd.Array.AsSpan().Slice(initCmd.Offset + 1, initCmd.Count - 1).ToArray();

                _ = server.Execute(c, rest);
            }
        }
    }
}