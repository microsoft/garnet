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
        public static readonly HashSet<ServerConfigType> DefaultConfigType = [.. Enum.GetValues<ServerConfigType>().
            Where(e => e switch
            {
                ServerConfigType.NONE => false,
                ServerConfigType.ALL => false,
                _ => true
            })];

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
        private bool NetworkCONFIG_GET()
        {
            if (parseState.Count == 0)
            {
                return AbortWithWrongNumberOfArguments($"{nameof(RespCommand.CONFIG)}|{nameof(CmdStrings.GET)}");
            }

            // Extract requested parameters
            HashSet<ServerConfigType> parameters = [];
            var returnAll = false;
            for (var i = 0; i < parseState.Count; i++)
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
                WriteMapLength(parameters.Count);

                foreach (var parameter in parameters)
                {
                    var parameterValue = parameter switch
                    {
                        ServerConfigType.TIMEOUT => "$7\r\ntimeout\r\n$1\r\n0\r\n"u8,
                        ServerConfigType.SAVE => "$4\r\nsave\r\n$0\r\n\r\n"u8,
                        ServerConfigType.APPENDONLY => storeWrapper.serverOptions.EnableAOF ? "$10\r\nappendonly\r\n$3\r\nyes\r\n"u8 : "$10\r\nappendonly\r\n$2\r\nno\r\n"u8,
                        ServerConfigType.SLAVE_READ_ONLY => clusterSession == null || clusterSession.ReadWriteSession ? "$15\r\nslave-read-only\r\n$2\r\nno\r\n"u8 : "$15\r\nslave-read-only\r\n$3\r\nyes\r\n"u8,
                        ServerConfigType.DATABASES => GetDatabases(),
                        ServerConfigType.CLUSTER_NODE_TIMEOUT => Encoding.ASCII.GetBytes($"$20\r\ncluster-node-timeout\r\n${storeWrapper.serverOptions.ClusterTimeout.ToString().Length}\r\n{storeWrapper.serverOptions.ClusterTimeout}\r\n"),
                        ServerConfigType.NONE => throw new NotImplementedException(),
                        ServerConfigType.ALL => throw new NotImplementedException(),
                        _ => throw new NotImplementedException()
                    };

                    ReadOnlySpan<byte> GetDatabases()
                    {
                        var databases = storeWrapper.serverOptions.MaxDatabases.ToString();
                        return Encoding.ASCII.GetBytes($"$9\r\ndatabases\r\n${databases.Length}\r\n{databases}\r\n");
                    }

                    while (!RespWriteUtils.TryWriteDirect(parameterValue, ref dcurr, dend))
                        SendAndReset();
                }
            }
            else
            {
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_EMPTYLIST, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        private bool NetworkCONFIG_REWRITE()
        {
            if (parseState.Count != 0)
                return AbortWithWrongNumberOfArguments($"{nameof(RespCommand.CONFIG)}|{nameof(CmdStrings.REWRITE)}");

            storeWrapper.clusterProvider?.FlushConfig();
            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        private bool NetworkCONFIG_SET()
        {
            if (parseState.Count == 0 || parseState.Count % 2 != 0)
                return AbortWithWrongNumberOfArguments($"{nameof(RespCommand.CONFIG)}|{nameof(CmdStrings.SET)}");

            string certFileName = null;
            string certPassword = null;
            string clusterUsername = null;
            string clusterPassword = null;
            string mainLogMemorySize = null;
            string readCacheMemorySize = null;
            string index = null;

            var unknownOption = false;
            var unknownKey = "";

            for (var c = 0; c < parseState.Count; c += 2)
            {
                var key = parseState.GetArgSliceByRef(c).ReadOnlySpan;
                var value = parseState.GetArgSliceByRef(c + 1).ReadOnlySpan;

                if (key.EqualsLowerCaseSpanIgnoringCase(CmdStrings.MainLogMemory, allowNonAlphabeticChars: false))
                    mainLogMemorySize = Encoding.ASCII.GetString(value);
                else if (key.EqualsLowerCaseSpanIgnoringCase(CmdStrings.ReadCacheMemory, allowNonAlphabeticChars: false))
                    readCacheMemorySize = Encoding.ASCII.GetString(value);
                else if (key.EqualsLowerCaseSpanIgnoringCase(CmdStrings.Index, allowNonAlphabeticChars: false))
                    index = Encoding.ASCII.GetString(value);
                else if (key.EqualsLowerCaseSpanIgnoringCase(CmdStrings.CertFileName, allowNonAlphabeticChars: true))
                    certFileName = Encoding.ASCII.GetString(value);
                else if (key.EqualsLowerCaseSpanIgnoringCase(CmdStrings.CertPassword, allowNonAlphabeticChars: true))
                    certPassword = Encoding.ASCII.GetString(value);
                else if (key.EqualsLowerCaseSpanIgnoringCase(CmdStrings.ClusterUsername, allowNonAlphabeticChars: true))
                    clusterUsername = Encoding.ASCII.GetString(value);
                else if (key.EqualsLowerCaseSpanIgnoringCase(CmdStrings.ClusterPassword, allowNonAlphabeticChars: true))
                    clusterPassword = Encoding.ASCII.GetString(value);
                else if (!unknownOption)
                {
                    unknownOption = true;
                    unknownKey = Encoding.ASCII.GetString(key);
                }
            }

            var sbErrorMsg = new StringBuilder();

            if (unknownOption)
                AppendError(sbErrorMsg, string.Format(CmdStrings.GenericErrUnknownOptionConfigSet, unknownKey));
            else
            {
                if (clusterUsername != null || clusterPassword != null)
                {
                    if (clusterUsername == null)
                        logger?.LogWarning("Cluster username is not provided, will use new password with existing username");
                    if (storeWrapper.clusterProvider != null)
                        storeWrapper.clusterProvider?.UpdateClusterAuth(clusterUsername, clusterPassword);
                    else
                        AppendError(sbErrorMsg, "ERR Cluster is disabled.");
                }

                if (certFileName != null || certPassword != null)
                {
                    if (storeWrapper.serverOptions.TlsOptions != null)
                    {
                        if (!storeWrapper.serverOptions.TlsOptions.UpdateCertFile(certFileName, certPassword, out var certErrorMessage))
                            AppendError(sbErrorMsg, certErrorMessage);
                    }
                    else
                        _ = sbErrorMsg.AppendLine("ERR TLS is disabled.");
                }

                if (mainLogMemorySize != null)
                    HandleMemorySizeChange(mainLogMemorySize, sbErrorMsg, isReadCache: false);
                if (readCacheMemorySize != null)
                    HandleMemorySizeChange(readCacheMemorySize, sbErrorMsg, isReadCache: true);
                if (index != null)
                    HandleIndexSizeChange(index, sbErrorMsg);
            }

            if (sbErrorMsg.Length == 0)
            {
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteError(sbErrorMsg.ToString(), ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        private void HandleMemorySizeChange(string memorySize, StringBuilder sbErrorMsg, bool isReadCache)
        {
            if (!ServerOptions.TryParseSize(memorySize, out var newMemorySize))
            {
                AppendErrorWithTemplate(sbErrorMsg, CmdStrings.GenericErrIncorrectSizeFormat, CmdStrings.MainLogMemory);
                return;
            }

            // Parse the configured memory size
            // If the new memory size is the same as the configured memory size, nothing to do
            var confMemorySize = ServerOptions.ParseSize(storeWrapper.serverOptions.LogMemorySize, out _);
            if (newMemorySize == confMemorySize)
                return;

            // Calculate the buffer size based on the configured memory size
            // If the new memory size is greater than the configured memory size, return an error
            confMemorySize = ServerOptions.NextPowerOf2(confMemorySize);
            if (newMemorySize > confMemorySize)
            {
                AppendErrorWithTemplate(sbErrorMsg, CmdStrings.GenericErrMemorySizeGreaterThanBuffer, CmdStrings.MainLogMemory);
                return;
            }

            // If the size tracker is not running for the specified allocator, return an error
            if (isReadCache)
            {
                if (storeWrapper.sizeTracker?.readCacheTracker is null)
                {
                    AppendErrorWithTemplate(sbErrorMsg, CmdStrings.GenericErrReadCacheMemorySizeTrackerNotRunning, CmdStrings.ReadCacheMemory);
                    return;
                }
            }
            else if (storeWrapper.sizeTracker?.mainLogTracker is null)
            {
                AppendErrorWithTemplate(sbErrorMsg, CmdStrings.GenericErrMainLogMemorySizeTrackerNotRunning, CmdStrings.MainLogMemory);
                return;
            }

            // Set the new target size for the object store size tracker
            if (isReadCache)
                storeWrapper.sizeTracker.ReadCacheTargetSize = newMemorySize;
            else
                storeWrapper.sizeTracker.TargetSize = newMemorySize;
        }

        private void HandleIndexSizeChange(string indexSize, StringBuilder sbErrorMsg)
        {
            if (!ServerOptions.TryParseSize(indexSize, out var newIndexSize))
            {
                AppendErrorWithTemplate(sbErrorMsg, CmdStrings.GenericErrIncorrectSizeFormat, CmdStrings.Index);
                return;
            }

            // Check if the new index size is a power of two. If not - return an error.
            var adjNewIndexSize = ServerOptions.PreviousPowerOf2(newIndexSize);
            if (adjNewIndexSize != newIndexSize)
            {
                AppendErrorWithTemplate(sbErrorMsg, CmdStrings.GenericErrIndexSizePowerOfTwo, CmdStrings.Index);
                return;
            }

            // Check if the index auto-grow task is running. If so - return an error.
            if (storeWrapper.serverOptions.AdjustedIndexMaxCacheLines > 0)
            {
                AppendErrorWithTemplate(sbErrorMsg, CmdStrings.GenericErrIndexSizeAutoGrow, CmdStrings.Index);
                return;
            }

            var currIndexSize = storeWrapper.store.IndexSize;

            // Convert new index size to cache lines
            // If the current index size is the same as the new index size, nothing to do
            adjNewIndexSize /= 64;
            if (currIndexSize == adjNewIndexSize)
                return;

            // If the new index size is smaller than the current index size, return an error
            if (currIndexSize > adjNewIndexSize)
            {
                AppendErrorWithTemplate(sbErrorMsg, CmdStrings.GenericErrIndexSizeSmallerThanCurrent, CmdStrings.Index);
                return;
            }

            // Try to grow the index size by doubling it until it reaches the new size
            for (; currIndexSize < adjNewIndexSize; currIndexSize *= 2)
            {
                if (!storeWrapper.store.GrowIndexAsync().ConfigureAwait(false).GetAwaiter().GetResult())
                {
                    AppendErrorWithTemplate(sbErrorMsg, CmdStrings.GenericErrIndexSizeGrowFailed, CmdStrings.Index);
                    return;
                }
            }
        }

        private static void AppendError(StringBuilder sbErrorMsg, string error)
            => _ = sbErrorMsg.Append($"{(sbErrorMsg.Length == 0 ? error : $"; {error.Substring(4)}")}");

        private static void AppendErrorWithTemplate(StringBuilder sbErrorMsg, string template, ReadOnlySpan<byte> option)
        {
            var error = string.Format(template, Encoding.ASCII.GetString(option));
            AppendError(sbErrorMsg, error);
        }
    }
}