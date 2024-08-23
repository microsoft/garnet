﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Garnet.common;
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

    [NonParallelizable]
    public class ClusterSlotVerificationTests
    {
        static readonly HashSet<BaseCommand> TestCommands = new(BaseCommandComparer.Instance)
            {
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
                new RENAME(),
                new DEL(),
                new GETDEL(),
                new EXISTS(),
                new PERSIST(),
                new EXPIRE(),
                new TTL(),
                new SDIFFSTORE(),
                new SDIFF(),
                new SMOVE(),
                new SUNIONSTORE(),
                new SUNION(),
                new SINTERSTORE(),
                new SINTER(),
                new LMOVE(),
            };


        ClusterTestContext context;
        readonly int sourceIndex = 0;
        readonly int targetIndex = 1;
        readonly int otherIndex = 2;
        readonly int iterations = 3;

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

            context.CreateInstances(3);
            context.CreateConnection();

            // Assign all slots to node 0
            context.clusterTestUtils.AddSlotsRange(sourceIndex, [(0, 16383)], logger: context.logger);
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

        [Test, Order(1), NonParallelizable]
        [Category("SLOT_VERIFY")]
        [TestCase("GET")]
        [TestCase("SET")]
        [TestCase("MGET")]
        [TestCase("MSET")]
        [TestCase("PFADD")]
        [TestCase("PFCOUNT")]
        [TestCase("PFMERGE")]
        [TestCase("SETBIT")]
        [TestCase("GETBIT")]
        [TestCase("BITCOUNT")]
        [TestCase("BITPOS")]
        [TestCase("BITOP")]
        [TestCase("BITFIELD")]
        [TestCase("BITFIELD_RO")]
        [TestCase("SETRANGE")]
        [TestCase("GETRANGE")]
        [TestCase("INCR")]
        [TestCase("APPEND")]
        [TestCase("STRLEN")]
        [TestCase("RENAME")]
        [TestCase("DEL")]
        [TestCase("GETDEL")]
        [TestCase("EXISTS")]
        [TestCase("PERSIST")]
        [TestCase("EXPIRE")]
        [TestCase("TTL")]
        [TestCase("SDIFFSTORE")]
        [TestCase("SDIFF")]
        [TestCase("SMOVE")]
        [TestCase("SUNIONSTORE")]
        [TestCase("SUNION")]
        [TestCase("SINTERSTORE")]
        [TestCase("SINTER")]
        [TestCase("LMOVE")]
        public void ClusterCLUSTERDOWNTest(string commandName)
        {
            var requestNodeIndex = otherIndex;
            var dummyCommand = new DummyCommand(commandName);
            ClassicAssert.IsTrue(TestCommands.TryGetValue(dummyCommand, out var command), "Command not found");

            for (var i = 0; i < iterations; i++)
                SERedisClusterDown(command);

            for (var i = 0; i < iterations; i++)
                GarnetClientSessionClusterDown(command);

            void SERedisClusterDown(BaseCommand command)
            {
                try
                {
                    _ = context.clusterTestUtils.GetServer(requestNodeIndex).Execute(command.Command, command.GetSingleSlotRequest());
                }
                catch (Exception ex)
                {
                    ClassicAssert.AreEqual("CLUSTERDOWN Hash slot not served", ex.Message, command.Command);
                    return;
                }
                Assert.Fail($"Should not reach here. Command: {command.Command}");
            }

            void GarnetClientSessionClusterDown(BaseCommand command)
            {
                var client = context.clusterTestUtils.GetGarnetClientSession(requestNodeIndex);
                try
                {
                    _ = client.ExecuteAsync(command.GetSingleSlotRequestWithCommand).GetAwaiter().GetResult();
                }
                catch (Exception ex)
                {
                    ClassicAssert.AreEqual("CLUSTERDOWN Hash slot not served", ex.Message, command.Command);
                    return;
                }
                Assert.Fail($"Should not reach here. Command: {command.Command}");
            }
        }

        [Test, Order(2), NonParallelizable]
        [Category("SLOT_VERIFY")]
        [TestCase("GET")]
        [TestCase("SET")]
        [TestCase("MGET")]
        [TestCase("MSET")]
        [TestCase("PFADD")]
        [TestCase("PFCOUNT")]
        [TestCase("PFMERGE")]
        [TestCase("SETBIT")]
        [TestCase("GETBIT")]
        [TestCase("BITCOUNT")]
        [TestCase("BITPOS")]
        [TestCase("BITOP")]
        [TestCase("BITFIELD")]
        [TestCase("BITFIELD_RO")]
        [TestCase("SETRANGE")]
        [TestCase("GETRANGE")]
        [TestCase("INCR")]
        [TestCase("APPEND")]
        [TestCase("STRLEN")]
        [TestCase("RENAME")]
        [TestCase("DEL")]
        [TestCase("GETDEL")]
        [TestCase("EXISTS")]
        [TestCase("PERSIST")]
        [TestCase("EXPIRE")]
        [TestCase("TTL")]
        [TestCase("SDIFFSTORE")]
        [TestCase("SDIFF")]
        [TestCase("SMOVE")]
        [TestCase("SUNIONSTORE")]
        [TestCase("SUNION")]
        [TestCase("SINTERSTORE")]
        [TestCase("SINTER")]
        [TestCase("LMOVE")]
        public void ClusterOKTest(string commandName)
        {
            var requestNodeIndex = sourceIndex;
            var dummyCommand = new DummyCommand(commandName);
            ClassicAssert.IsTrue(TestCommands.TryGetValue(dummyCommand, out var command), "Command not found");

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
                Assert.Fail($"Failed executing cleanup. Command: {command.Command}");
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
                        Assert.Fail($"{ex.Message}. Command: {command.Command}");
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
                        Assert.Fail($"{ex.Message}. Command: {command.Command}");
                }
            }
        }

        [Test, Order(3)]
        [Category("SLOT_VERIFY")]
        [TestCase("GET")]
        [TestCase("SET")]
        [TestCase("MGET")]
        [TestCase("MSET")]
        [TestCase("PFADD")]
        [TestCase("PFCOUNT")]
        [TestCase("PFMERGE")]
        [TestCase("SETBIT")]
        [TestCase("GETBIT")]
        [TestCase("BITCOUNT")]
        [TestCase("BITPOS")]
        [TestCase("BITOP")]
        [TestCase("BITFIELD")]
        [TestCase("BITFIELD_RO")]
        [TestCase("SETRANGE")]
        [TestCase("GETRANGE")]
        [TestCase("INCR")]
        [TestCase("APPEND")]
        [TestCase("STRLEN")]
        [TestCase("RENAME")]
        [TestCase("DEL")]
        [TestCase("GETDEL")]
        [TestCase("EXISTS")]
        [TestCase("PERSIST")]
        [TestCase("EXPIRE")]
        [TestCase("TTL")]
        [TestCase("SDIFFSTORE")]
        [TestCase("SDIFF")]
        [TestCase("SMOVE")]
        [TestCase("SUNIONSTORE")]
        [TestCase("SUNION")]
        [TestCase("SINTERSTORE")]
        [TestCase("SINTER")]
        [TestCase("LMOVE")]
        public void ClusterCROSSSLOTTest(string commandName)
        {
            var requestNodeIndex = sourceIndex;
            var dummyCommand = new DummyCommand(commandName);
            ClassicAssert.IsTrue(TestCommands.TryGetValue(dummyCommand, out var command), "Command not found");

            for (var i = 0; i < iterations; i++)
                SERedisCrossslotTest(command);

            for (var i = 0; i < iterations; i++)
                GarnetClientSessionCrossslotTest(command);

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
                    ClassicAssert.AreEqual("CROSSSLOT Keys in request do not hash to the same slot", ex.Message, command.Command);
                    return;
                }
                Assert.Fail($"Should not reach here. Command: {command.Command}");
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
                    ClassicAssert.AreEqual("CROSSSLOT Keys in request do not hash to the same slot", ex.Message, command.Command);
                    return;
                }
                Assert.Fail($"Should not reach here. Command: {command.Command}");
            }
        }

        [Test, Order(4), NonParallelizable]
        [Category("SLOT_VERIFY")]
        [TestCase("GET")]
        [TestCase("SET")]
        [TestCase("MGET")]
        [TestCase("MSET")]
        [TestCase("PFADD")]
        [TestCase("PFCOUNT")]
        [TestCase("PFMERGE")]
        [TestCase("SETBIT")]
        [TestCase("GETBIT")]
        [TestCase("BITCOUNT")]
        [TestCase("BITPOS")]
        [TestCase("BITOP")]
        [TestCase("BITFIELD")]
        [TestCase("BITFIELD_RO")]
        [TestCase("SETRANGE")]
        [TestCase("GETRANGE")]
        [TestCase("INCR")]
        [TestCase("APPEND")]
        [TestCase("STRLEN")]
        [TestCase("RENAME")]
        [TestCase("DEL")]
        [TestCase("GETDEL")]
        [TestCase("EXISTS")]
        [TestCase("PERSIST")]
        [TestCase("EXPIRE")]
        [TestCase("TTL")]
        [TestCase("SDIFFSTORE")]
        [TestCase("SDIFF")]
        [TestCase("SMOVE")]
        [TestCase("SUNIONSTORE")]
        [TestCase("SUNION")]
        [TestCase("SINTERSTORE")]
        [TestCase("SINTER")]
        [TestCase("LMOVE")]
        public void ClusterMOVEDTest(string commandName)
        {
            var requestNodeIndex = targetIndex;
            var address = "127.0.0.1";
            var port = context.clusterTestUtils.GetPortFromNodeIndex(sourceIndex);
            var dummyCommand = new DummyCommand(commandName);
            ClassicAssert.IsTrue(TestCommands.TryGetValue(dummyCommand, out var command), "Command not found");

            for (var i = 0; i < iterations; i++)
                SERedisMOVEDTest(command);

            for (var i = 0; i < iterations; i++)
                GarnetClientSessionMOVEDTest(command);

            void SERedisMOVEDTest(BaseCommand command)
            {
                try
                {
                    context.clusterTestUtils.GetServer(requestNodeIndex).Execute(command.Command, command.GetSingleSlotRequest(), CommandFlags.NoRedirect);
                }
                catch (Exception ex)
                {
                    ClassicAssert.IsTrue(ex.Message.StartsWith("Key has MOVED"), command.Command);
                    var tokens = ex.Message.Split(' ');
                    ClassicAssert.IsTrue(tokens.Length > 10 && tokens[2].Equals("MOVED"), command.Command);

                    var _address = tokens[5].Split(':')[0];
                    var _port = int.Parse(tokens[5].Split(':')[1]);
                    var _slot = int.Parse(tokens[8]);
                    ClassicAssert.AreEqual(address, _address, command.Command);
                    ClassicAssert.AreEqual(port, _port, command.Command);
                    ClassicAssert.AreEqual(command.GetSlot, _slot, command.Command);
                    return;
                }
                Assert.Fail($"Should not reach here. Command: {command.Command}");
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
                    ClassicAssert.AreEqual($"MOVED {command.GetSlot} {address}:{port}", ex.Message, command.Command);
                    return;
                }
                Assert.Fail($"Should not reach here. Command: {command.Command}");
            }
        }

        [Test, Order(5), NonParallelizable]
        [Category("SLOT_VERIFY")]
        [TestCase("GET")]
        [TestCase("SET")]
        [TestCase("MGET")]
        [TestCase("MSET")]
        [TestCase("PFADD")]
        [TestCase("PFCOUNT")]
        [TestCase("PFMERGE")]
        [TestCase("SETBIT")]
        [TestCase("GETBIT")]
        [TestCase("BITCOUNT")]
        [TestCase("BITPOS")]
        [TestCase("BITOP")]
        [TestCase("BITFIELD")]
        [TestCase("BITFIELD_RO")]
        [TestCase("SETRANGE")]
        [TestCase("GETRANGE")]
        [TestCase("INCR")]
        [TestCase("APPEND")]
        [TestCase("STRLEN")]
        [TestCase("RENAME")]
        [TestCase("DEL")]
        [TestCase("GETDEL")]
        [TestCase("EXISTS")]
        [TestCase("PERSIST")]
        [TestCase("EXPIRE")]
        [TestCase("TTL")]
        [TestCase("SDIFFSTORE")]
        [TestCase("SDIFF")]
        [TestCase("SMOVE")]
        [TestCase("SUNIONSTORE")]
        [TestCase("SUNION")]
        [TestCase("SINTERSTORE")]
        [TestCase("SINTER")]
        [TestCase("LMOVE")]
        public void ClusterASKTest(string commandName)
        {
            var requestNodeIndex = sourceIndex;
            var address = "127.0.0.1";
            var port = context.clusterTestUtils.GetPortFromNodeIndex(targetIndex);
            var dummyCommand = new DummyCommand(commandName);
            ClassicAssert.IsTrue(TestCommands.TryGetValue(dummyCommand, out var command), "Command not found");
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
                    Assert.Fail($"Failed executing cleanup. Command: {command.Command}");
                }
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
                    ClassicAssert.IsTrue(tokens.Length > 10 && tokens[0].Equals("Endpoint"), command.Command);

                    var _address = tokens[1].Split(':')[0];
                    var _port = int.Parse(tokens[1].Split(':')[1]);
                    var _slot = int.Parse(tokens[4]);
                    ClassicAssert.AreEqual(address, _address, command.Command);
                    ClassicAssert.AreEqual(port, _port, command.Command);
                    ClassicAssert.AreEqual(command.GetSlot, _slot, command.Command);
                    return;
                }
                Assert.Fail($"Should not reach here. Command: {command.Command}");
            }

            void GarnetClientSessionASKTest(BaseCommand command)
            {
                var client = context.clusterTestUtils.GetGarnetClientSession(requestNodeIndex);
                try
                {
                    _ = client.ExecuteAsync(command.GetSingleSlotRequestWithCommand).GetAwaiter().GetResult();
                }
                catch (Exception ex)
                {
                    ClassicAssert.AreEqual($"ASK {command.GetSlot} {address}:{port}", ex.Message, command.Command);
                    return;
                }
                Assert.Fail($"Should not reach here. Command: {command.Command}");
            }
        }

        [Test, Order(6), NonParallelizable]
        [Category("SLOT_VERIFY")]
        [TestCase("GET")]
        [TestCase("SET")]
        [TestCase("MGET")]
        [TestCase("MSET")]
        [TestCase("PFADD")]
        [TestCase("PFCOUNT")]
        [TestCase("PFMERGE")]
        [TestCase("SETBIT")]
        [TestCase("GETBIT")]
        [TestCase("BITCOUNT")]
        [TestCase("BITPOS")]
        [TestCase("BITOP")]
        [TestCase("BITFIELD")]
        [TestCase("BITFIELD_RO")]
        [TestCase("SETRANGE")]
        [TestCase("GETRANGE")]
        [TestCase("INCR")]
        [TestCase("APPEND")]
        [TestCase("STRLEN")]
        [TestCase("RENAME")]
        [TestCase("DEL")]
        [TestCase("GETDEL")]
        [TestCase("EXISTS")]
        [TestCase("PERSIST")]
        [TestCase("EXPIRE")]
        [TestCase("TTL")]
        [TestCase("SDIFFSTORE")]
        [TestCase("SDIFF")]
        [TestCase("SMOVE")]
        [TestCase("SUNIONSTORE")]
        [TestCase("SUNION")]
        [TestCase("SINTERSTORE")]
        [TestCase("SINTER")]
        [TestCase("LMOVE")]
        public void ClusterTRYAGAINTest(string commandName)
        {
            var requestNodeIndex = sourceIndex;
            var dummyCommand = new DummyCommand(commandName);
            ClassicAssert.IsTrue(TestCommands.TryGetValue(dummyCommand, out var command), "Command not found");
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
                        Assert.Fail($"Failed executing setup. Command: {command.Command}");
                    }
                }

                ConfigureSlotForMigration();
                try
                {
                    _ = context.clusterTestUtils.GetServer(requestNodeIndex).Execute(command.Command, command.GetSingleSlotRequest(), CommandFlags.NoRedirect);
                }
                catch (Exception ex)
                {
                    ClassicAssert.AreEqual("TRYAGAIN Multiple keys request during rehashing of slot", ex.Message, command.Command);
                    return;
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
                        Assert.Fail($"Failed executing cleanup. Command: {command.Command}");
                    }
                }

                Assert.Fail($"Should not reach here. Command: {command.Command}");
            }
        }
    }
}