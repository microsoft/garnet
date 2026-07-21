// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    static class ServerConfig
    {
        public static ServerConfigType GetConfig(Span<byte> parameter)
        {
            AsciiUtils.ToUpperInPlace(parameter);
            if (parameter.SequenceEqual("*"u8))
                return ServerConfigType.ALL;

            // slave-read-only is a per-session value (READWRITE/READONLY) and is resolved by the CONFIG GET
            // handler which has the session in scope; it is not part of the runtime config table.
            if (parameter.SequenceEqual("SLAVE-READ-ONLY"u8))
                return ServerConfigType.SLAVE_READ_ONLY;

            // Every other CONFIG parameter (settable and read-only) is resolved through the runtime config table.
            return RuntimeServerConfig.TryGetType(parameter, out var configType) ? configType : ServerConfigType.NONE;
        }
    }

    internal sealed partial class RespServerSession : ServerSessionBase
    {
        private unsafe bool NetworkCONFIG_GET()
        {
            if (parseState.Count == 0)
            {
                return AbortWithWrongNumberOfArguments($"{nameof(RespCommand.CONFIG)}|{nameof(CmdStrings.GET)}");
            }

            // Extract requested parameters. All CONFIG parameters (settable and read-only) are served
            // through the runtime config table.
            List<ServerConfigType> parameters = null;
            var returnAll = false;
            for (var i = 0; i < parseState.Count; i++)
            {
                var parameter = parseState.GetArgSliceByRef(i).Span;
                var serverConfigType = ServerConfig.GetConfig(parameter);

                if (returnAll) continue;
                if (serverConfigType == ServerConfigType.ALL)
                {
                    parameters = [.. RuntimeServerConfig.RuntimeTypes];
                    // slave-read-only is session-scoped and not part of the table, so include it explicitly.
                    parameters.Add(ServerConfigType.SLAVE_READ_ONLY);
                    returnAll = true;
                    continue;
                }

                if (serverConfigType == ServerConfigType.NONE)
                    continue;
                (parameters ??= []).Add(serverConfigType);
            }

            // Generate response for matching parameters
            var totalCount = parameters?.Count ?? 0;
            if (totalCount > 0)
            {
                WriteMapLength(totalCount);

                foreach (var configType in parameters)
                {
                    string name, value;
                    if (configType == ServerConfigType.SLAVE_READ_ONLY)
                    {
                        // Per-session value: a session is read-only only when it is on a replica and has not
                        // opted into writes via READWRITE (see https://redis.io/docs/latest/commands/readwrite/).
                        name = "slave-read-only";
                        value = clusterSession == null || clusterSession.ReadWriteSession ? "no" : "yes";
                    }
                    else
                    {
                        name = RuntimeServerConfig.Name(configType);
                        value = storeWrapper.runtimeConfig.Format(configType);
                    }

                    while (!RespWriteUtils.TryWriteAsciiBulkString(name, ref dcurr, dend))
                        SendAndReset();
                    while (!RespWriteUtils.TryWriteAsciiBulkString(value, ref dcurr, dend))
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

        private unsafe bool NetworkCONFIG_REWRITE()
        {
            if (parseState.Count != 0)
                return AbortWithWrongNumberOfArguments($"{nameof(RespCommand.CONFIG)}|{nameof(CmdStrings.REWRITE)}");

            storeWrapper.clusterProvider?.FlushConfig();
            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        private unsafe bool NetworkCONFIG_SET()
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
            List<(ServerConfigType type, string value)> dynamicSets = null;

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
                else if (RuntimeServerConfig.TryGetType(key, out var runtimeType))
                    (dynamicSets ??= []).Add((runtimeType, Encoding.ASCII.GetString(value)));
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
                {
                    // Must block, we're on the network thread
                    AsyncUtils.BlockingWait(HandleIndexSizeChangeAsync(index, sbErrorMsg));
                }

                if (dynamicSets != null)
                {
                    foreach (var (type, value) in dynamicSets)
                    {
                        var error = storeWrapper.runtimeConfig.TrySet(type, value);
                        if (error != null)
                            AppendError(sbErrorMsg, error);
                    }
                }
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

        private async Task HandleIndexSizeChangeAsync(string indexSize, StringBuilder sbErrorMsg)
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
                if (!AsyncUtils.BlockingWait(storeWrapper.store.GrowIndexAsync()))
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