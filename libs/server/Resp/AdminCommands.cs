// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server.ACL;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Server session for RESP protocol - admin commands are in this file
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        private bool ProcessAdminCommands<TGarnetApi>(RespCommand command, ReadOnlySpan<byte> bufSpan, int count, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            bool errorFlag = false;
            string errorCmd = string.Empty;
            hasAdminCommand = true;

            if (command == RespCommand.AUTH)
            {
                // AUTH [<username>] <password>
                if (count < 1 || count > 2)
                {
                    if (!DrainCommands(bufSpan, count))
                        return false;
                    errorCmd = "auth";
                    var errorMsg = string.Format(CmdStrings.ErrMissingParam, errorCmd);
                    var bresp_ERRMISSINGPARAM = Encoding.ASCII.GetBytes(errorMsg);
                    bresp_ERRMISSINGPARAM.CopyTo(new Span<byte>(dcurr, bresp_ERRMISSINGPARAM.Length));
                    dcurr += bresp_ERRMISSINGPARAM.Length;
                }
                else
                {
                    bool success = true;

                    // Optional Argument: <username>
                    ReadOnlySpan<byte> username = (count > 1) ? GetCommand(bufSpan, out success) : null;

                    // Mandatory Argument: <password>
                    ReadOnlySpan<byte> password = success ? GetCommand(bufSpan, out success) : null;

                    // If any of the parsing failed, exit here
                    if (!success)
                    {
                        return false;
                    }

                    // NOTE: Some authenticators cannot accept username/password pairs
                    if (!_authenticator.CanAuthenticate)
                    {
                        while (!RespWriteUtils.WriteDirect(Encoding.ASCII.GetBytes("-ERR Client sent AUTH, but configured authenticator does not accept passwords\r\n"), ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }
                    else
                    {
                        // XXX: There should be high-level AuthenticatorException
                        if (this.AuthenticateUser(username, password))
                        {
                            while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                                SendAndReset();
                        }
                        else
                        {
                            if (username.IsEmpty)
                            {
                                while (!RespWriteUtils.WriteDirect(Encoding.ASCII.GetBytes("-WRONGPASS Invalid password\r\n"), ref dcurr, dend))
                                    SendAndReset();
                            }
                            else
                            {
                                while (!RespWriteUtils.WriteDirect(Encoding.ASCII.GetBytes("-WRONGPASS Invalid username/password combination\r\n"), ref dcurr, dend))
                                    SendAndReset();
                            }
                        }
                    }
                }
                return true;
            }

            if (!_authenticator.IsAuthenticated)
            {
                // If the current session is unauthenticated, we stop parsing, because no other commands are allowed
                if (!DrainCommands(bufSpan, count))
                    return false;

                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_NOAUTH, ref dcurr, dend))
                    SendAndReset();
            }
            else if (command == RespCommand.CONFIG)
            {
                if (!CheckACLAdminPermissions(bufSpan, count, out bool success))
                {
                    return success;
                }

                var param = GetCommand(bufSpan, out bool success1);
                if (!success1) return false;
                if (param.SequenceEqual(CmdStrings.GET) || param.SequenceEqual(CmdStrings.get))
                {
                    if (count > 2)
                    {
                        if (!DrainCommands(bufSpan, count - 1))
                            return false;
                        errorFlag = true;
                        errorCmd = Encoding.ASCII.GetString(param.ToArray());
                    }
                    else
                    {
                        var key = GetCommand(bufSpan, out bool success2);
                        if (!success2) return false;

                        while (!RespWriteUtils.WriteResponse(CmdStrings.GetConfig(key), ref dcurr, dend))
                            SendAndReset();
                    }
                }
                else if (param.SequenceEqual(CmdStrings.REWRITE) || param.SequenceEqual(CmdStrings.rewrite))
                {
                    storeWrapper.clusterProvider?.FlushConfig();
                    while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
                else if (param.SequenceEqual(CmdStrings.SET) || param.SequenceEqual(CmdStrings.set))
                {
                    string certFileName = null;
                    string certPassword = null;
                    string clusterUsername = null;
                    string clusterPassword = null;
                    bool unknownOption = false;
                    string unknownKey = "";
                    if (count == 1 || count % 2 != 1)
                    {
                        if (!DrainCommands(bufSpan, count - 1))
                            return false;
                        while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_WRONG_ARGUMENTS, ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }
                    else
                    {
                        for (int c = 0; c < (count - 1) / 2; c++)
                        {
                            var key = GetCommand(bufSpan, out bool success2);
                            if (!success2) return false;
                            var value = GetCommand(bufSpan, out bool success3);
                            if (!success3) return false;

                            if (key.SequenceEqual(CmdStrings.CertFileName))
                                certFileName = Encoding.ASCII.GetString(value.ToArray());
                            else if (key.SequenceEqual(CmdStrings.CertPassword))
                                certPassword = Encoding.ASCII.GetString(value.ToArray());
                            else if (key.SequenceEqual(CmdStrings.ClusterUsername))
                                clusterUsername = Encoding.ASCII.GetString(value.ToArray());
                            else if (key.SequenceEqual(CmdStrings.ClusterPassword))
                                clusterPassword = Encoding.ASCII.GetString(value.ToArray());
                            else
                            {
                                if (!unknownOption)
                                {
                                    unknownOption = true;
                                    unknownKey = Encoding.ASCII.GetString(key);
                                }
                            }
                        }
                    }
                    string errorMsg = null;
                    if (unknownOption)
                    {
                        errorMsg = string.Format(CmdStrings.UnknownOption, unknownKey);
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
                                if (errorMsg == null) errorMsg = "Cluster is disabled.";
                                else errorMsg += " " + "Cluster is disabled.";
                            }
                        }
                        if (certFileName != null || certPassword != null)
                        {
                            if (storeWrapper.serverOptions.TlsOptions != null)
                            {
                                if (!storeWrapper.serverOptions.TlsOptions.UpdateCertFile(certFileName, certPassword, out var _errorMsg))
                                {
                                    if (errorMsg == null) errorMsg = _errorMsg;
                                    else errorMsg += " " + _errorMsg;
                                }
                            }
                            else
                            {
                                if (errorMsg == null) errorMsg = "TLS is disabled.";
                                else errorMsg += " " + "TLS is disabled.";
                            }
                        }
                    }
                    if (errorMsg == null)
                    {
                        while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                            SendAndReset();
                    }
                    else
                    {
                        while (!RespWriteUtils.WriteDirect(Encoding.ASCII.GetBytes("-ERR " + errorMsg + "\r\n"), ref dcurr, dend))
                            SendAndReset();
                    }
                }
                else
                {
                    if (!DrainCommands(bufSpan, count - 1))
                        return false;
                    errorFlag = true;
                    errorCmd = command.ToString() + " " + Encoding.ASCII.GetString(param.ToArray());
                }
            }
            else if (command == RespCommand.ECHO)
            {
                if (count != 1)
                {
                    if (!DrainCommands(bufSpan, count))
                        return false;
                    errorFlag = true;
                    errorCmd = "echo";
                }
                else
                {
                    var oldReadHead = readHead;
                    GetCommand(bufSpan, out bool success1);
                    if (!success1) return false;
                    var length = readHead - oldReadHead;
                    while (!RespWriteUtils.WriteDirect(bufSpan.Slice(oldReadHead, length).ToArray(), ref dcurr, dend))
                        SendAndReset();
                }
            }
            else if (command == RespCommand.INFO)
            {
                return ProcessInfoCommand(count);
            }
            else if (command == RespCommand.COMMAND)
            {
                if (count != 0)
                {
                    if (!DrainCommands(bufSpan, count))
                        return false;
                    errorFlag = true;
                    errorCmd = "command";
                }
                else
                {
                    // TODO: include the built-in commands
                    string resultStr = "";
                    int cnt = 0;
                    for (int i = 0; i < storeWrapper.customCommandManager.CommandId; i++)
                    {
                        var cmd = storeWrapper.customCommandManager.commandMap[i];
                        if (cmd != null)
                        {
                            cnt++;
                            resultStr += $"*6\r\n${cmd.nameStr.Length}\r\n{cmd.nameStr}\r\n:{1 + cmd.NumKeys + cmd.NumParams}\r\n*1\r\n+fast\r\n:1\r\n:1\r\n:1\r\n";
                        }
                    }

                    while (!RespWriteUtils.WriteDirect(Encoding.ASCII.GetBytes($"*{cnt}\r\n"), ref dcurr, dend))
                        SendAndReset();
                    while (!RespWriteUtils.WriteDirect(Encoding.ASCII.GetBytes(resultStr), ref dcurr, dend))
                        SendAndReset();
                }
            }
            else if (command == RespCommand.PING)
            {
                if (count == 0)
                {
                    while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_PONG, ref dcurr, dend))
                        SendAndReset();
                }
                else if (count == 1)
                {
                    var oldReadHead = readHead;
                    GetCommand(bufSpan, out bool success1);
                    if (!success1) return false;
                    var length = readHead - oldReadHead;
                    bufSpan.Slice(oldReadHead, length).CopyTo(new Span<byte>(dcurr, length));
                    dcurr += length;
                }
                else
                {
                    if (!DrainCommands(bufSpan, count))
                        return false;
                    errorFlag = true;
                    errorCmd = "ping";
                }
            }
            else if ((command == RespCommand.CLUSTER) || (command == RespCommand.MIGRATE) || (command == RespCommand.FAILOVER) || (command == RespCommand.REPLICAOF) || (command == RespCommand.SECONDARYOF))
            {
                if (clusterSession == null)
                {
                    if (!DrainCommands(bufSpan, count))
                        return false;
                    while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_CLUSTER_DISABLED, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                clusterSession.ProcessClusterCommands(command, bufSpan, count, recvBufferPtr, bytesRead, ref readHead, ref dcurr, ref dend, out bool result);
                return result;
            }
            else if (command == RespCommand.LATENCY)
            {
                return ProcessLatencyCommands(bufSpan, count);
            }
            else if (command == RespCommand.TIME)
            {
                if (count != 0)
                {
                    if (!DrainCommands(bufSpan, count))
                        return false;
                    errorFlag = true;
                    errorCmd = "time";
                }
                else
                {
                    var utcTime = DateTimeOffset.UtcNow;
                    var seconds = utcTime.ToUnixTimeSeconds();
                    var microsecs = utcTime.ToString("ffffff");
                    var response = string.Format("*2\r\n${0}\r\n{1}\r\n${2}\r\n{3}\r\n", seconds.ToString().Length, seconds, microsecs.Length, microsecs);
                    while (!RespWriteUtils.WriteDirect(Encoding.ASCII.GetBytes(response), ref dcurr, dend))
                        SendAndReset();
                }
            }
            else if (command == RespCommand.QUIT)
            {
                if (!DrainCommands(bufSpan, count))
                    return false;
                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
                toDispose = true;
            }
            else if (command == RespCommand.SAVE)
            {
                if (!CheckACLAdminPermissions(bufSpan, count, out bool success))
                {
                    return success;
                }

                if (!DrainCommands(bufSpan, count))
                    return false;

                var resp = CmdStrings.RESP_OK;
                if (!storeWrapper.TakeCheckpoint(false, StoreType.All, logger))
                    resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes("-ERR checkpoint already in progress\r\n"));
                while (!RespWriteUtils.WriteResponse(resp, ref dcurr, dend))
                    SendAndReset();
            }
            else if (command == RespCommand.LASTSAVE)
            {
                bool success;
                if (!CheckACLAdminPermissions(bufSpan, count, out success))
                {
                    return success;
                }

                if (!DrainCommands(bufSpan, count))
                    return false;
                var seconds = storeWrapper.lastSaveTime.ToUnixTimeSeconds();
                while (!RespWriteUtils.WriteInteger(seconds, ref dcurr, dend))
                    SendAndReset();
            }
            else if (command == RespCommand.BGSAVE)
            {
                bool success;
                if (!CheckACLAdminPermissions(bufSpan, count, out success))
                {
                    return success;
                }

                if (!DrainCommands(bufSpan, count))
                    return false;

                success = storeWrapper.TakeCheckpoint(true, StoreType.All, logger);
                if (success)
                {
                    while (!RespWriteUtils.WriteSimpleString(Encoding.ASCII.GetBytes("Background saving started"), ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    var resp = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes("-ERR checkpoint already in progress\r\n"));
                    while (!RespWriteUtils.WriteResponse(resp, ref dcurr, dend))
                        SendAndReset();
                }
            }
            else if (command == RespCommand.COMMITAOF)
            {
                if (!DrainCommands(bufSpan, count))
                    return false;

                CommitAof();
                while (!RespWriteUtils.WriteSimpleString(Encoding.ASCII.GetBytes("AOF file committed"), ref dcurr, dend))
                    SendAndReset();
            }
            else if (command == RespCommand.FLUSHDB)
            {
                bool unsafeTruncateLog = false;
                bool async = false;
                if (count > 0)
                {
                    while (count > 0)
                    {
                        var param = GetCommand(bufSpan, out bool success1);
                        if (!success1) return false;
                        if (Encoding.ASCII.GetString(param).ToUpper() == "UNSAFETRUNCATELOG")
                            unsafeTruncateLog = true;
                        if (Encoding.ASCII.GetString(param).ToUpper() == "ASYNC")
                            async = true;
                        if (Encoding.ASCII.GetString(param).ToUpper() == "SYNC")
                            async = false;
                        count--;
                    }
                }
                if (!DrainCommands(bufSpan, count))
                    return false;

                if (async)
                    Task.Run(() => FlushDB(unsafeTruncateLog)).ConfigureAwait(false);
                else
                    FlushDB(unsafeTruncateLog);

                logger?.LogInformation("Running flushDB " + (async ? "async" : "sync") + (unsafeTruncateLog ? " with unsafetruncatelog." : ""));
                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
            else if (command == RespCommand.FORCEGC)
            {
                var generation = GC.MaxGeneration;
                if (count == 1)
                {
                    var ptr = recvBufferPtr + readHead;
                    if (!RespReadUtils.ReadIntWithLengthHeader(out generation, ref ptr, recvBufferPtr + bytesRead))
                        return false;

                    if (generation < 0 || generation > GC.MaxGeneration)
                    {
                        while (!RespWriteUtils.WriteDirect(Encoding.ASCII.GetBytes("-ERR Invalid GC generation.\r\n"), ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }
                    readHead = (int)(ptr - recvBufferPtr);
                }
                else if (count == 0)
                {
                    if (!DrainCommands(bufSpan, count))
                        return false;
                }
                else
                {
                    errorFlag = true;
                    errorCmd = "forcegc";
                }

                if (!errorFlag)
                {
                    GC.Collect(generation, GCCollectionMode.Forced, true);
                    while (!RespWriteUtils.WriteSimpleString(Encoding.ASCII.GetBytes("GC completed"), ref dcurr, dend))
                        SendAndReset();
                }
            }
            else if (command == RespCommand.MEMORY)
            {
                return NetworkMEMORY(count, recvBufferPtr + readHead, ref storageApi);
            }
            else if (command == RespCommand.MONITOR)
            {
                if (!CheckACLAdminPermissions(bufSpan, count, out bool success))
                {
                    return success;
                }

                return NetworkMONITOR(count, recvBufferPtr + readHead);
            }
            else if (command == RespCommand.ACL)
            {
                return ProcessACLCommands(bufSpan, count);
            }
            else if ((command == RespCommand.REGISTERCS))
            {
                return NetworkREGISTERCS(count, recvBufferPtr + readHead, storeWrapper.customCommandManager);
            }
            else
            {
                // Unknown RESP Command
                if (!DrainCommands(bufSpan, count))
                    return false;
                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_ERR, ref dcurr, dend))
                    SendAndReset();
            }

            if (errorFlag && !string.IsNullOrWhiteSpace(errorCmd))
            {
                var errorMsg = string.Format(CmdStrings.ErrMissingParam, errorCmd);
                while (!RespWriteUtils.WriteDirect(Encoding.ASCII.GetBytes(errorMsg), ref dcurr, dend))
                    SendAndReset();
            }
            return true;
        }

        bool DrainCommands(ReadOnlySpan<byte> bufSpan, int count)
        {
            for (int i = 0; i < count; i++)
            {
                GetCommand(bufSpan, out bool success1);
                if (!success1) return false;
            }
            return true;
        }

        /// <summary>
        /// Performs @admin command group permission checks for the current user and the given command.
        /// (NOTE: This function is temporary until per-command permissions are implemented)
        /// </summary>
        /// <param name="bufSpan">Buffer containing the current command in RESP3 style.</param>
        /// <param name="count">Number of parameters left in the command specification.</param>
        /// <param name="processingCompleted">Indicates whether the command was completely processed, regardless of whether execution was successful or not.</param>
        /// <returns>True if the command execution is allowed to continue, otherwise false.</returns>
        bool CheckACLAdminPermissions(ReadOnlySpan<byte> bufSpan, int count, out bool processingCompleted)
        {
            Debug.Assert(!_authenticator.IsAuthenticated || (_user != null));

            if (!_authenticator.IsAuthenticated || (!_user.CanAccessCategory(CommandCategory.Flag.Admin)))
            {
                if (!DrainCommands(bufSpan, count))
                {
                    processingCompleted = false;
                }
                else
                {
                    while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_NOAUTH, ref dcurr, dend))
                        SendAndReset();
                    processingCompleted = true;
                }
                return false;
            }

            processingCompleted = true;

            return true;
        }

        void CommitAof()
        {
            storeWrapper.appendOnlyFile?.CommitAsync().ConfigureAwait(false).GetAwaiter().GetResult();
        }

        void FlushDB(bool unsafeTruncateLog)
        {
            storeWrapper.store.Log.ShiftBeginAddress(storeWrapper.store.Log.TailAddress, truncateLog: unsafeTruncateLog);
            storeWrapper.objectStore?.Log.ShiftBeginAddress(storeWrapper.objectStore.Log.TailAddress, truncateLog: unsafeTruncateLog);
        }

        private bool NetworkMEMORY<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
              where TGarnetApi : IGarnetApi
        {
            //MEMORY USAGE [key] [SAMPLES count]

            if (!RespReadUtils.ReadStringWithLengthHeader(out var memoryOption, ref ptr, recvBufferPtr + bytesRead))
                return false;

            var status = GarnetStatus.OK;
            long memoryUsage = default;

            if (memoryOption.ToUpperInvariant().Equals("USAGE"))
            {
                // read key
                byte* keyPtr = null;
                int kSize = 0;

                if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref kSize, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (count == 4)
                {
                    // Calculations for nested types do not apply to garnet, but we are parsing the parameter for API compatibility
                    if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var samplesBA, ref ptr, recvBufferPtr + bytesRead))
                        return false;

                    if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var countSamplesBA, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                }

                status = storageApi.MemoryUsageForKey(new(keyPtr, kSize), out memoryUsage);
            }

            if (status == GarnetStatus.OK)
            {
                while (!RespWriteUtils.WriteInteger((int)memoryUsage, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                    SendAndReset();
            }

            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        private bool NetworkMONITOR(int count, byte* ptr)
        {
            //todo:Not supported yet.
            return true;
        }

    }
}