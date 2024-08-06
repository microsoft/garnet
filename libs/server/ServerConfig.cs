// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Garnet.common;
using Microsoft.Extensions.Logging;

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
        private bool NetworkCONFIG_GET(int count)
        {
            if (count == 0)
            {
                return AbortWithWrongNumberOfArguments($"{nameof(RespCommand.CONFIG)}|{nameof(CmdStrings.GET)}", count);
            }

            // Extract requested parameters
            HashSet<ServerConfigType> parameters = [];
            var returnAll = false;
            for (var i = 0; i < count; i++)
            {
                var parameter = parseState.GetArgSliceByRef(i).Span;
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

        private bool NetworkCONFIG_REWRITE(int count)
        {
            if (count != 0)
            {
                return AbortWithWrongNumberOfArguments($"{nameof(RespCommand.CONFIG)}|{nameof(CmdStrings.REWRITE)}", count);
            }

            storeWrapper.clusterProvider?.FlushConfig();
            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        private bool NetworkCONFIG_SET(int count)
        {
            if (count == 0 || count % 2 != 0)
            {
                return AbortWithWrongNumberOfArguments($"{nameof(RespCommand.CONFIG)}|{nameof(CmdStrings.SET)}", count);
            }

            string certFileName = null;
            string certPassword = null;
            string clusterUsername = null;
            string clusterPassword = null;
            var unknownOption = false;
            var unknownKey = "";

            for (var c = 0; c < count; c += 2)
            {
                var key = parseState.GetArgSliceByRef(c).ReadOnlySpan;
                var value = parseState.GetArgSliceByRef(c + 1).ReadOnlySpan;

                if (key.SequenceEqual(CmdStrings.CertFileName))
                    certFileName = Encoding.ASCII.GetString(value);
                else if (key.SequenceEqual(CmdStrings.CertPassword))
                    certPassword = Encoding.ASCII.GetString(value);
                else if (key.SequenceEqual(CmdStrings.ClusterUsername))
                    clusterUsername = Encoding.ASCII.GetString(value);
                else if (key.SequenceEqual(CmdStrings.ClusterPassword))
                    clusterPassword = Encoding.ASCII.GetString(value);
                else
                {
                    if (!unknownOption)
                    {
                        unknownOption = true;
                        unknownKey = Encoding.ASCII.GetString(key);
                    }
                }
            }

            string errorMsg = null;
            if (unknownOption)
            {
                errorMsg = string.Format(CmdStrings.GenericErrUnknownOptionConfigSet, unknownKey);
            }
            else
            {
                if (clusterUsername != null || clusterPassword != null)
                {
                    if (clusterUsername == null)
                        logger?.LogWarning("Cluster username is not provided, will use new password with existing username");
                    if (storeWrapper.clusterProvider != null)
                        storeWrapper.clusterProvider?.UpdateClusterAuth(clusterUsername, clusterPassword);
                    else
                    {
                        errorMsg = "ERR Cluster is disabled.";
                    }
                }
                if (certFileName != null || certPassword != null)
                {
                    if (storeWrapper.serverOptions.TlsOptions != null)
                    {
                        if (!storeWrapper.serverOptions.TlsOptions.UpdateCertFile(certFileName, certPassword, out var certErrorMessage))
                        {
                            if (errorMsg == null) errorMsg = "ERR " + certErrorMessage;
                            else errorMsg += " " + certErrorMessage;
                        }
                    }
                    else
                    {
                        if (errorMsg == null) errorMsg = "ERR TLS is disabled.";
                        else errorMsg += " TLS is disabled.";
                    }
                }
            }

            if (errorMsg == null)
            {
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.WriteError(errorMsg, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }
    }
}