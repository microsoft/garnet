// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Server session for RESP protocol - admin commands are in this file
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        private bool ProcessAdminCommands<TGarnetApi>(RespCommand command, int count, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            bool errorFlag = false;
            string errorCmd = string.Empty;
            hasAdminCommand = true;

            bool success;

            if (command == RespCommand.AUTH)
            {
                // AUTH [<username>] <password>
                if (count < 1 || count > 2)
                {
                    if (!DrainCommands(count))
                        return false;
                    errorCmd = "auth";
                    var errorMsg = string.Format(CmdStrings.GenericErrWrongNumArgs, errorCmd);
                    while (!RespWriteUtils.WriteError(errorMsg, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    success = true;

                    // Optional Argument: <username>
                    ReadOnlySpan<byte> username = (count > 1) ? GetCommand(out success) : null;

                    // Mandatory Argument: <password>
                    ReadOnlySpan<byte> password = success ? GetCommand(out success) : null;

                    // If any of the parsing failed, exit here
                    if (!success)
                    {
                        return false;
                    }

                    // NOTE: Some authenticators cannot accept username/password pairs
                    if (!_authenticator.CanAuthenticate)
                    {
                        while (!RespWriteUtils.WriteError("ERR Client sent AUTH, but configured authenticator does not accept passwords"u8, ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }
                    else
                    {
                        // XXX: There should be high-level AuthenticatorException
                        if (this.AuthenticateUser(username, password))
                        {
                            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                                SendAndReset();
                        }
                        else
                        {
                            if (username.IsEmpty)
                            {
                                while (!RespWriteUtils.WriteError(CmdStrings.RESP_WRONGPASS_INVALID_PASSWORD, ref dcurr, dend))
                                    SendAndReset();
                            }
                            else
                            {
                                while (!RespWriteUtils.WriteError(CmdStrings.RESP_WRONGPASS_INVALID_USERNAME_PASSWORD, ref dcurr, dend))
                                    SendAndReset();
                            }
                        }
                    }
                }
                return true;
            }

            if (_authenticator.CanAuthenticate && !_authenticator.IsAuthenticated)
            {
                // If the current session is unauthenticated, we stop parsing, because no other commands are allowed
                if (!DrainCommands(count))
                    return false;

                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_NOAUTH, ref dcurr, dend))
                    SendAndReset();
            }
            else if (command == RespCommand.CONFIG_GET)
            {
                return NetworkConfigGet(count);
            }
            else if (command == RespCommand.CONFIG_REWRITE)
            {
                if (count != 0)
                {
                    if (!DrainCommands(count))
                        return false;

                    while (!RespWriteUtils.WriteError($"ERR Unknown subcommand or wrong number of arguments for CONFIG REWRITE.", ref dcurr, dend))
                        SendAndReset();

                    return true;
                }

                storeWrapper.clusterProvider?.FlushConfig();
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();

                return true;
            }
            else if (command == RespCommand.CONFIG_SET)
            {
                string certFileName = null;
                string certPassword = null;
                string clusterUsername = null;
                string clusterPassword = null;
                bool unknownOption = false;
                string unknownKey = "";
                if (count == 0 || count % 2 != 0)
                {
                    if (!DrainCommands(count))
                        return false;
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_WRONG_ARGUMENTS, ref dcurr, dend))
                        SendAndReset();

                    return true;
                }
                else
                {
                    for (int c = 0; c < count / 2; c++)
                    {
                        var key = GetCommand(out bool success2);
                        if (!success2) return false;
                        var value = GetCommand(out bool success3);
                        if (!success3) return false;

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
                            if (errorMsg == null) errorMsg = "ERR Cluster is disabled.";
                            else errorMsg += " Cluster is disabled.";
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
            }
            else if (command == RespCommand.ECHO)
            {
                if (count != 1)
                {
                    if (!DrainCommands(count))
                        return false;
                    errorFlag = true;
                    errorCmd = "echo";
                }
                else
                {
                    var oldReadHead = readHead;
                    if (!SkipCommand()) return false;
                    var length = readHead - oldReadHead;
                    WriteDirectLarge(new ReadOnlySpan<byte>(recvBufferPtr + oldReadHead, length));
                }
            }
            else if (command == RespCommand.INFO)
            {
                return ProcessInfoCommand(count);
            }
            else if (command == RespCommand.PING)
            {
                if (count == 0)
                {
                    if (isSubscriptionSession && respProtocolVersion == 2)
                    {
                        while (!RespWriteUtils.WriteDirect(CmdStrings.SUSCRIBE_PONG, ref dcurr, dend))
                            SendAndReset();
                    }
                    else
                    {
                        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_PONG, ref dcurr, dend))
                            SendAndReset();
                    }
                }
                else if (count == 1)
                {
                    var oldReadHead = readHead;
                    if (!SkipCommand()) return false;
                    var length = readHead - oldReadHead;
                    WriteDirectLarge(new ReadOnlySpan<byte>(recvBufferPtr + oldReadHead, length));
                }
                else
                {
                    if (!DrainCommands(count))
                        return false;
                    errorFlag = true;
                    errorCmd = "ping";
                }
            }
            else if (command == RespCommand.HELLO)
            {
                byte? respProtocolVersion = null;
                ReadOnlySpan<byte> authUsername = default, authPassword = default;
                string clientName = null;

                if (count > 0)
                {
                    var ptr = recvBufferPtr + readHead;
                    int localRespProtocolVersion;
                    if (!RespReadUtils.ReadIntWithLengthHeader(out localRespProtocolVersion, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                    readHead = (int)(ptr - recvBufferPtr);

                    respProtocolVersion = (byte)localRespProtocolVersion;
                    count--;
                    while (count > 0)
                    {
                        var param = GetCommand(out bool success1);
                        if (!success1) return false;
                        count--;
                        if (param.EqualsUpperCaseSpanIgnoringCase(CmdStrings.AUTH))
                        {
                            if (count < 2)
                            {
                                if (!DrainCommands(count))
                                    return false;
                                count = 0;
                                errorFlag = true;
                                errorCmd = nameof(RespCommand.HELLO);
                                break;
                            }
                            authUsername = GetCommand(out success1);
                            if (!success1) return false;
                            count--;
                            authPassword = GetCommand(out success1);
                            if (!success1) return false;
                            count--;
                        }
                        else if (param.EqualsUpperCaseSpanIgnoringCase(CmdStrings.SETNAME))
                        {
                            if (count < 1)
                            {
                                if (!DrainCommands(count))
                                    return false;
                                count = 0;
                                errorFlag = true;
                                errorCmd = nameof(RespCommand.HELLO);
                                break;
                            }

                            var arg = GetCommand(out success1);
                            if (!success1) return false;
                            count--;
                            clientName = Encoding.ASCII.GetString(arg);
                        }
                        else
                        {
                            if (!DrainCommands(count))
                                return false;
                            count = 0;
                            errorFlag = true;
                            errorCmd = nameof(RespCommand.HELLO);
                        }
                    }
                }
                if (!errorFlag) ProcessHelloCommand(respProtocolVersion, authUsername, authPassword, clientName);
            }
            else if (command.IsClusterSubCommand() || command == RespCommand.MIGRATE || command == RespCommand.FAILOVER || command == RespCommand.REPLICAOF || command == RespCommand.SECONDARYOF)
            {
                if (clusterSession == null)
                {
                    if (!DrainCommands(count))
                        return false;
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_CLUSTER_DISABLED, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                clusterSession.ProcessClusterCommands(command, count, recvBufferPtr, bytesRead, ref readHead, ref dcurr, ref dend, out bool result);
                return result;
            }
            else if (command == RespCommand.LATENCY_HELP)
            {
                return NetworkLatencyHelp(count);
            }
            else if (command == RespCommand.LATENCY_HISTOGRAM)
            {
                return NetworkLatencyHistogram(count);
            }
            else if (command == RespCommand.LATENCY_RESET)
            {
                return NetworkLatencyReset(count);
            }
            else if (command == RespCommand.TIME)
            {
                if (count != 0)
                {
                    if (!DrainCommands(count))
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
                    while (!RespWriteUtils.WriteAsciiDirect(response, ref dcurr, dend))
                        SendAndReset();
                }
            }
            else if (command == RespCommand.QUIT)
            {
                if (!DrainCommands(count))
                    return false;
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();

                toDispose = true;
            }
            else if (command == RespCommand.SAVE)
            {
                if (!DrainCommands(count))
                    return false;

                if (!storeWrapper.TakeCheckpoint(false, StoreType.All, logger))
                {
                    while (!RespWriteUtils.WriteError("ERR checkpoint already in progress"u8, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
            }
            else if (command == RespCommand.LASTSAVE)
            {
                if (!DrainCommands(count))
                    return false;
                var seconds = storeWrapper.lastSaveTime.ToUnixTimeSeconds();
                while (!RespWriteUtils.WriteInteger(seconds, ref dcurr, dend))
                    SendAndReset();
            }
            else if (command == RespCommand.BGSAVE)
            {
                if (!DrainCommands(count))
                    return false;

                success = storeWrapper.TakeCheckpoint(true, StoreType.All, logger);
                if (success)
                {
                    while (!RespWriteUtils.WriteSimpleString("Background saving started"u8, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.WriteError("ERR checkpoint already in progress"u8, ref dcurr, dend))
                        SendAndReset();
                }
            }
            else if (command == RespCommand.COMMITAOF)
            {
                if (!DrainCommands(count))
                    return false;

                CommitAof();
                while (!RespWriteUtils.WriteSimpleString("AOF file committed"u8, ref dcurr, dend))
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
                        var param = GetCommand(out bool success1);
                        if (!success1) return false;
                        string paramStr = Encoding.ASCII.GetString(param);
                        if (paramStr.Equals("UNSAFETRUNCATELOG", StringComparison.OrdinalIgnoreCase))
                            unsafeTruncateLog = true;
                        else if (paramStr.Equals("ASYNC", StringComparison.OrdinalIgnoreCase))
                            async = true;
                        else if (paramStr.Equals("SYNC", StringComparison.OrdinalIgnoreCase))
                            async = false;
                        count--;
                    }
                }
                if (!DrainCommands(count))
                    return false;

                if (async)
                    Task.Run(() => FlushDB(unsafeTruncateLog)).ConfigureAwait(false);
                else
                    FlushDB(unsafeTruncateLog);

                logger?.LogInformation("Running flushDB " + (async ? "async" : "sync") + (unsafeTruncateLog ? " with unsafetruncatelog." : ""));
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
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
                        while (!RespWriteUtils.WriteError("ERR Invalid GC generation."u8, ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }
                    readHead = (int)(ptr - recvBufferPtr);
                }
                else if (count == 0)
                {
                    if (!DrainCommands(count))
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
                    while (!RespWriteUtils.WriteSimpleString("GC completed"u8, ref dcurr, dend))
                        SendAndReset();
                }
            }
            else if (command == RespCommand.MEMORY_USAGE)
            {
                return NetworkMemoryUsage(count, recvBufferPtr + readHead, ref storageApi);
            }
            else if (command == RespCommand.MONITOR)
            {
                return NetworkMonitor(count, recvBufferPtr + readHead);
            }
            else if (command == RespCommand.ACL_CAT)
            {
                return NetworkAclCat(count);
            }
            else if (command == RespCommand.ACL_DELUSER)
            {
                return NetworkAclDelUser(count);
            }
            else if (command == RespCommand.ACL_LIST)
            {
                return NetworkAclList(count);
            }
            else if (command == RespCommand.ACL_LOAD)
            {
                return NetworkAclLoad(count);
            }
            else if (command == RespCommand.ACL_SETUSER)
            {
                return NetworkAclSetUser(count);
            }
            else if (command == RespCommand.ACL_USERS)
            {
                return NetworkAclUsers(count);
            }
            else if (command == RespCommand.ACL_WHOAMI)
            {
                return NetworkAclWhoAmI(count);
            }
            else if (command == RespCommand.ACL_SAVE)
            {
                return NetworkAclSave(count);
            }
            else if (command == RespCommand.REGISTERCS)
            {
                return NetworkRegisterCs(count, recvBufferPtr + readHead, storeWrapper.customCommandManager);
            }
            else if (command == RespCommand.ASYNC)
            {
                if (respProtocolVersion > 2 && count == 1)
                {
                    var param = GetCommand(out bool success1);
                    if (!success1) return false;
                    if (param.SequenceEqual(CmdStrings.ON) || param.SequenceEqual(CmdStrings.on))
                    {
                        useAsync = true;
                        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                            SendAndReset();
                    }
                    else if (param.SequenceEqual(CmdStrings.OFF) || param.SequenceEqual(CmdStrings.off))
                    {
                        useAsync = false;
                        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                            SendAndReset();
                    }
                    else if (param.SequenceEqual(CmdStrings.BARRIER) || param.SequenceEqual(CmdStrings.barrier))
                    {
                        if (asyncCompleted < asyncStarted)
                        {
                            asyncDone = new(0);
                            if (dcurr > networkSender.GetResponseObjectHead())
                                Send(networkSender.GetResponseObjectHead());
                            try
                            {
                                networkSender.ExitAndReturnResponseObject();
                                while (asyncCompleted < asyncStarted) asyncDone.Wait();
                                asyncDone.Dispose();
                                asyncDone = null;
                            }
                            finally
                            {
                                networkSender.EnterAndGetResponseObject(out dcurr, out dend);
                            }
                        }

                        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                            SendAndReset();
                    }
                    else
                    {
                        if (!DrainCommands(count - 1))
                            return false;
                        errorFlag = true;
                        errorCmd = "ASYNC";
                    }
                }
                else
                {
                    if (!DrainCommands(count))
                        return false;
                    if (respProtocolVersion <= 2)
                    {
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_NOT_SUPPORTED_RESP2, ref dcurr, dend))
                            SendAndReset();
                    }
                    else
                    {
                        errorFlag = true;
                        errorCmd = "ASYNC";
                    }
                }
            }
            else
            {
                if (!DrainCommands(count))
                    return false;
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_UNK_CMD, ref dcurr, dend))
                    SendAndReset();
            }

            if (errorFlag && !string.IsNullOrWhiteSpace(errorCmd))
            {
                var errorMsg = string.Format(CmdStrings.GenericErrWrongNumArgs, errorCmd);
                while (!RespWriteUtils.WriteError(errorMsg, ref dcurr, dend))
                    SendAndReset();
            }
            return true;
        }

        /// <summary>
        /// Process the HELLO command
        /// </summary>
        void ProcessHelloCommand(byte? respProtocolVersion, ReadOnlySpan<byte> username, ReadOnlySpan<byte> password, string clientName)
        {
            if (respProtocolVersion != null)
            {
                if (respProtocolVersion.Value is < 2 or > 3)
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_UNSUPPORTED_PROTOCOL_VERSION, ref dcurr, dend))
                        SendAndReset();
                    return;
                }

                if (respProtocolVersion.Value != this.respProtocolVersion && asyncCompleted < asyncStarted)
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_ASYNC_PROTOCOL_CHANGE, ref dcurr, dend))
                        SendAndReset();
                    return;
                }

                this.respProtocolVersion = respProtocolVersion.Value;
            }

            if (username != default)
            {
                if (!this.AuthenticateUser(username, password))
                {
                    if (username.IsEmpty)
                    {
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_WRONGPASS_INVALID_PASSWORD, ref dcurr, dend))
                            SendAndReset();
                    }
                    else
                    {
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_WRONGPASS_INVALID_USERNAME_PASSWORD, ref dcurr, dend))
                            SendAndReset();
                    }
                    return;
                }
            }

            if (clientName != null)
            {
                this.clientName = clientName;
            }

            (string, string)[] helloResult =
                [
                    ("server", "redis"),
                    ("version", storeWrapper.redisProtocolVersion),
                    ("garnet_version", storeWrapper.version),
                    ("proto", $"{this.respProtocolVersion}"),
                    ("id", "63"),
                    ("mode", storeWrapper.serverOptions.EnableCluster ? "cluster" : "standalone"),
                    ("role", storeWrapper.serverOptions.EnableCluster && storeWrapper.clusterProvider.IsReplica() ? "replica" : "master"),
                ];

            if (this.respProtocolVersion == 2)
            {
                while (!RespWriteUtils.WriteArrayLength(helloResult.Length * 2 + 2, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.WriteMapLength(helloResult.Length + 1, ref dcurr, dend))
                    SendAndReset();
            }
            for (int i = 0; i < helloResult.Length; i++)
            {
                while (!RespWriteUtils.WriteAsciiBulkString(helloResult[i].Item1, ref dcurr, dend))
                    SendAndReset();
                while (!RespWriteUtils.WriteAsciiBulkString(helloResult[i].Item2, ref dcurr, dend))
                    SendAndReset();
            }
            while (!RespWriteUtils.WriteAsciiBulkString("modules", ref dcurr, dend))
                SendAndReset();
            while (!RespWriteUtils.WriteArrayLength(0, ref dcurr, dend))
                SendAndReset();
        }

        /// <summary>
        /// Performs permission checks for the current user and the given command.
        /// (NOTE: This function does not check keyspaces)
        /// </summary>
        /// <param name="cmd">Command be processed</param>
        /// <param name="ptr">Pointer to start of arguments in command buffer</param>
        /// <param name="count">Number of parameters left in the command specification.</param>
        /// <param name="processingCompleted">Indicates whether the command was completely processed, regardless of whether execution was successful or not.</param>
        /// <returns>True if the command execution is allowed to continue, otherwise false.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool CheckACLPermissions(RespCommand cmd, byte* ptr, int count, out bool processingCompleted)
        {
            Debug.Assert(!_authenticator.IsAuthenticated || (_user != null));

            if ((!_authenticator.IsAuthenticated || !_user.CanAccessCommand(cmd)) && !cmd.IsNoAuth())
            {
                processingCompleted = OnACLFailure(this, cmd, count);
                return false;
            }

            processingCompleted = true;

            return true;

            // Failing should be rare, and is not important for performance so hide this behind
            // a method call to keep icache pressure down
            [MethodImpl(MethodImplOptions.NoInlining)]
            static bool OnACLFailure(RespServerSession self, RespCommand cmd, int count)
            {
                // If we're rejecting a command, we may need to cleanup some ambient state too
                if (cmd == RespCommand.CustomCmd)
                {
                    self.currentCustomCommand = null;
                }
                else if (cmd == RespCommand.CustomObjCmd)
                {
                    self.currentCustomObjectCommand = null;
                }
                else if (cmd == RespCommand.CustomTxn)
                {
                    self.currentCustomTransaction = null;
                }

                if (!self.DrainCommands(count))
                {
                    return false;
                }
                else
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_NOAUTH, ref self.dcurr, self.dend))
                        self.SendAndReset();

                    return true;
                }
            }
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

        private bool NetworkMemoryUsage<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
              where TGarnetApi : IGarnetApi
        {
            //MEMORY USAGE [key] [SAMPLES count]

            var status = GarnetStatus.OK;
            long memoryUsage = default;

            if (!RespReadUtils.TrySliceWithLengthHeader(out var keyBytes, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (count == 3)
            {
                // Calculations for nested types do not apply to garnet, but we are parsing the parameter for API compatibility
                if (!RespReadUtils.TrySliceWithLengthHeader(out _ /* samplesBA */, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (!RespReadUtils.TrySliceWithLengthHeader(out _ /* countSamplesBA */, ref ptr, recvBufferPtr + bytesRead))
                    return false;
            }

            status = storageApi.MemoryUsageForKey(ArgSlice.FromPinnedSpan(keyBytes), out memoryUsage);

            if (status == GarnetStatus.OK)
            {
                while (!RespWriteUtils.WriteInteger((int)memoryUsage, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                    SendAndReset();
            }

            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        private bool NetworkMonitor(int count, byte* ptr)
        {
            // TODO: Not supported yet.
            return true;
        }

    }
}