// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Garnet.common;

namespace Garnet.server
{
    static class ServerConfig
    {
        public static readonly HashSet<ServerConfigType> DefaultConfigType = Enum.GetValues<ServerConfigType>().
            Where(e => e switch
            {
                ServerConfigType.NONE => false,
                ServerConfigType.ALL => false,
                _ => true
            }).ToHashSet();

        public static unsafe ServerConfigType GetConfig(Span<byte> parameter)
        {
            AsciiUtils.ToUpperInPlace(parameter);
            return parameter switch
            {
                _ when parameter.SequenceEqual("TIMEOUT"u8) => ServerConfigType.TIMEOUT,
                _ when parameter.SequenceEqual("SAVE"u8) => ServerConfigType.SAVE,
                _ when parameter.SequenceEqual("APPENDONLY"u8) => ServerConfigType.APPENDONLY,
                _ when parameter.SequenceEqual("SLAVE-READ-ONLY"u8) => ServerConfigType.SLAVE_READ_ONLY,
                _ when parameter.SequenceEqual("DATABASES"u8) => ServerConfigType.DATABASES,
                _ when parameter.SequenceEqual("CLUSTER-NODE-TIMEOUT"u8) => ServerConfigType.CLUSTER_NODE_TIMEOUT,
                _ when parameter.SequenceEqual("*"u8) => ServerConfigType.ALL,
                _ => ServerConfigType.NONE,
            };
        }
    }

    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        private bool NetworkConfigGet(ReadOnlySpan<byte> bufSpan, int count)
        {
            if (count == 0)
            {
                while (!RespWriteUtils.WriteError($"ERR wrong number of arguments for 'config|get' command", ref dcurr, dend))
                    SendAndReset();

                return true;
            }

            // Extract requested parameters
            HashSet<ServerConfigType> parameters = [];
            var returnAll = false;
            for (var i = 0; i < count; i++)
            {
                var parameter = GetCommand(bufSpan, out bool success2);
                if (!success2) return false;
                var serverConfigType = ServerConfig.GetConfig(parameter);

                if (returnAll) continue;
                if (serverConfigType == ServerConfigType.ALL)
                {
                    parameters = ServerConfig.DefaultConfigType;
                    returnAll = true;
                    continue;
                }

                if (serverConfigType != ServerConfigType.NONE)
                    _ = parameters.Add(serverConfigType);
            }

            // Generate response for matching parameters
            if (parameters.Count > 0)
            {
                while (!RespWriteUtils.WriteArrayLength(parameters.Count * 2, ref dcurr, dend))
                    SendAndReset();

                foreach (var parameter in parameters)
                {
                    var parameterValue = parameter switch
                    {
                        ServerConfigType.TIMEOUT => "$7\r\ntimeout\r\n$1\r\n0\r\n"u8,
                        ServerConfigType.SAVE => "$4\r\nsave\r\n$0\r\n\r\n"u8,
                        ServerConfigType.APPENDONLY => storeWrapper.serverOptions.EnableAOF ? "$10\r\nappendonly\r\n$3\r\nyes\r\n"u8 : "$10\r\nappendonly\r\n$2\r\nno\r\n"u8,
                        ServerConfigType.SLAVE_READ_ONLY => clusterSession == null || clusterSession.ReadWriteSession ? "$15\r\nslave-read-only\r\n$2\r\nno\r\n"u8 : "$15\r\nslave-read-only\r\n$3\r\nyes\r\n"u8,
                        ServerConfigType.DATABASES => storeWrapper.serverOptions.EnableCluster ? "$9\r\ndatabases\r\n$1\r\n1\r\n"u8 : "$9\r\ndatabases\r\n$2\r\n16\r\n"u8,
                        ServerConfigType.CLUSTER_NODE_TIMEOUT => Encoding.ASCII.GetBytes($"$20\r\ncluster-node-timeout\r\n${storeWrapper.serverOptions.ClusterTimeout.ToString().Length}\r\n{storeWrapper.serverOptions.ClusterTimeout}\r\n"),
                        ServerConfigType.NONE => throw new NotImplementedException(),
                        ServerConfigType.ALL => throw new NotImplementedException(),
                        _ => throw new NotImplementedException()
                    };

                    while (!RespWriteUtils.WriteDirect(parameterValue, ref dcurr, dend))
                        SendAndReset();
                }
            }
            else
            {
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_EMPTYLIST, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }
    }
}