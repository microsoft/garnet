// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server.Custom;
using Garnet.server.Module;
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
                    errorFlag = true;
                    errorCmd = "echo";
                }
                else
                {
                    WriteDirectLarge(new ReadOnlySpan<byte>(recvBufferPtr + readHead, endReadHead - readHead));
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
                    WriteDirectLarge(new ReadOnlySpan<byte>(recvBufferPtr + readHead, endReadHead - readHead));
                }
                else
                {
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
                return NetworkQUIT();
            }
            else if (command == RespCommand.SAVE)
            {
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
                var seconds = storeWrapper.lastSaveTime.ToUnixTimeSeconds();
                while (!RespWriteUtils.WriteInteger(seconds, ref dcurr, dend))
                    SendAndReset();
            }
            else if (command == RespCommand.BGSAVE)
            {
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
                else if (count != 0)
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
                        errorFlag = true;
                        errorCmd = "ASYNC";
                    }
                }
                else
                {
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
            else if (command == RespCommand.MODULE_LOADCS)
            {
                NetworkModuleLoad(storeWrapper.customCommandManager);
            }
            else
            {
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
        /// <returns>True if the command execution is allowed to continue, otherwise false.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool CheckACLPermissions(RespCommand cmd)
        {
            Debug.Assert(!_authenticator.IsAuthenticated || (_user != null));

            if ((!_authenticator.IsAuthenticated || !_user.CanAccessCommand(cmd)) && !cmd.IsNoAuth())
            {
                OnACLFailure(this, cmd);
                return false;
            }
            return true;

            // Failing should be rare, and is not important for performance so hide this behind
            // a method call to keep icache pressure down
            [MethodImpl(MethodImplOptions.NoInlining)]
            static void OnACLFailure(RespServerSession self, RespCommand cmd)
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
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_NOAUTH, ref self.dcurr, self.dend))
                    self.SendAndReset();
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

        private bool LoadAssemblies(IEnumerable<string> binaryPaths, out IEnumerable<Assembly> loadedAssemblies, out ReadOnlySpan<byte> errorMessage)
        {
            loadedAssemblies = null;
            errorMessage = default;

            // Get all binary file paths from inputs binary paths
            if (!FileUtils.TryGetFiles(binaryPaths, out var files, out _, [".dll", ".exe"],
                    SearchOption.AllDirectories))
            {
                errorMessage = CmdStrings.RESP_ERR_GENERIC_GETTING_BINARY_FILES;
                return false;
            }

            // Check that all binary files are contained in allowed binary paths
            var binaryFiles = files.ToArray();
            if (binaryFiles.Any(f =>
                    storeWrapper.serverOptions.ExtensionBinPaths.All(p => !FileUtils.IsFileInDirectory(f, p))))
            {
                errorMessage = CmdStrings.RESP_ERR_GENERIC_BINARY_FILES_NOT_IN_ALLOWED_PATHS;
                return false;
            }

            // Get all assemblies from binary files
            if (!FileUtils.TryLoadAssemblies(binaryFiles, out loadedAssemblies, out _))
            {
                errorMessage = CmdStrings.RESP_ERR_GENERIC_LOADING_ASSEMBLIES;
                return false;
            }

            // If necessary, check that all assemblies are digitally signed
            if (!storeWrapper.serverOptions.ExtensionAllowUnsignedAssemblies)
            {
                foreach (var loadedAssembly in loadedAssemblies)
                {
                    var publicKey = loadedAssembly.GetName().GetPublicKey();
                    if (publicKey == null || publicKey.Length == 0)
                    {
                        errorMessage = CmdStrings.RESP_ERR_GENERIC_ASSEMBLY_NOT_SIGNED;
                        return false;
                    }
                }
            }

            return true;
        }

        /// <summary>
        /// Register all custom commands / transactions
        /// </summary>
        /// <param name="binaryPaths">Binary paths from which to load assemblies</param>
        /// <param name="cmdInfoPath">Path of JSON file containing RespCommandsInfo for custom commands</param>
        /// <param name="classNameToRegisterArgs">Mapping between class names to register and arguments required for registration</param>
        /// <param name="customCommandManager">CustomCommandManager instance used to register commands</param>
        /// <param name="errorMessage">If method returned false, contains ASCII encoded generic error string; otherwise <c>default</c></param>
        /// <returns>A boolean value indicating whether registration of the custom commands was successful.</returns>
        private bool TryRegisterCustomCommands(
            IEnumerable<string> binaryPaths,
            string cmdInfoPath,
            Dictionary<string, List<RegisterArgsBase>> classNameToRegisterArgs,
            CustomCommandManager customCommandManager,
            out ReadOnlySpan<byte> errorMessage)
        {
            errorMessage = default;
            var classInstances = new Dictionary<string, object>();
            IReadOnlyDictionary<string, RespCommandsInfo> cmdNameToInfo = new Dictionary<string, RespCommandsInfo>();

            if (cmdInfoPath != null)
            {
                // Check command info path, if specified
                if (!File.Exists(cmdInfoPath))
                {
                    errorMessage = CmdStrings.RESP_ERR_GENERIC_GETTING_CMD_INFO_FILE;
                    return false;
                }

                // Check command info path is in allowed paths
                if (storeWrapper.serverOptions.ExtensionBinPaths.All(p => !FileUtils.IsFileInDirectory(cmdInfoPath, p)))
                {
                    errorMessage = CmdStrings.RESP_ERR_GENERIC_CMD_INFO_FILE_NOT_IN_ALLOWED_PATHS;
                    return false;
                }

                var streamProvider = StreamProviderFactory.GetStreamProvider(FileLocationType.Local);
                var commandsInfoProvider = RespCommandsInfoProviderFactory.GetRespCommandsInfoProvider();

                var importSucceeded = commandsInfoProvider.TryImportRespCommandsInfo(cmdInfoPath,
                    streamProvider, out cmdNameToInfo, logger);

                if (!importSucceeded)
                {
                    errorMessage = CmdStrings.RESP_ERR_GENERIC_MALFORMED_COMMAND_INFO_JSON;
                    return false;
                }
            }

            if (!LoadAssemblies(binaryPaths, out var loadedAssemblies, out errorMessage))
                return false;

            foreach (var c in classNameToRegisterArgs.Keys)
            {
                classInstances.TryAdd(c, null);
            }

            // Get types from loaded assemblies
            var loadedTypes = loadedAssemblies
                .SelectMany(a => a.GetTypes())
                .Where(t => classInstances.ContainsKey(t.Name)).ToArray();

            // Check that all types implement one of the supported custom command base classes
            var supportedCustomCommandTypes = RegisterCustomCommandProviderBase.SupportedCustomCommandBaseTypesLazy.Value;
            if (loadedTypes.Any(t => !supportedCustomCommandTypes.Any(st => st.IsAssignableFrom(t))))
            {
                errorMessage = CmdStrings.RESP_ERR_GENERIC_REGISTERCS_UNSUPPORTED_CLASS;
                return false;
            }

            // Check that all types have empty constructors
            if (loadedTypes.Any(t => t.GetConstructor(Type.EmptyTypes) == null))
            {
                errorMessage = CmdStrings.RESP_ERR_GENERIC_INSTANTIATING_CLASS;
                return false;
            }

            // Instantiate types
            foreach (var type in loadedTypes)
            {
                var instance = Activator.CreateInstance(type);
                classInstances[type.Name] = instance;
            }

            // If any class specified in the arguments was not instantiated, return an error
            if (classNameToRegisterArgs.Keys.Any(c => classInstances[c] == null))
            {
                errorMessage = CmdStrings.RESP_ERR_GENERIC_INSTANTIATING_CLASS;
                return false;
            }

            // Register each command / transaction using its specified class instance
            var registerApis = new List<IRegisterCustomCommandProvider>();
            foreach (var classNameToArgs in classNameToRegisterArgs)
            {
                foreach (var args in classNameToArgs.Value)
                {
                    // Add command info to register arguments, if exists
                    if (cmdNameToInfo.ContainsKey(args.Name))
                    {
                        args.CommandInfo = cmdNameToInfo[args.Name];
                    }

                    var registerApi =
                        RegisterCustomCommandProviderFactory.GetRegisterCustomCommandProvider(classInstances[classNameToArgs.Key], args);

                    if (registerApi == null)
                    {
                        errorMessage = CmdStrings.RESP_ERR_GENERIC_REGISTERCS_UNSUPPORTED_CLASS;
                        return false;
                    }

                    registerApis.Add(registerApi);
                }
            }

            foreach (var registerApi in registerApis)
            {
                registerApi.Register(customCommandManager);
            }

            return true;

            // If any assembly was not loaded correctly, return an error

            // If any directory was not enumerated correctly, return an error
        }

        /// <summary>
        /// REGISTERCS - Registers one or more custom commands / transactions
        /// </summary>
        private bool NetworkRegisterCs(int count, byte* ptr, CustomCommandManager customCommandManager)
        {
            var leftTokens = count;
            var readPathsOnly = false;
            var optionalParamsRead = 0;

            var binaryPaths = new HashSet<string>();
            string cmdInfoPath = default;

            // Custom class name to arguments read from each sub-command
            var classNameToRegisterArgs = new Dictionary<string, List<RegisterArgsBase>>();

            ReadOnlySpan<byte> errorMsg = null;

            if (leftTokens < 6)
                errorMsg = CmdStrings.RESP_ERR_GENERIC_MALFORMED_REGISTERCS_COMMAND;

            // Parse the REGISTERCS command - list of registration sub-commands
            // followed by an optional path to JSON file containing an array of RespCommandsInfo objects,
            // followed by a list of paths to binary files / folders
            // Syntax - REGISTERCS cmdType name numParams className [expTicks] [cmdType name numParams className [expTicks] ...]
            // [INFO path] SRC path [path ...]
            RegisterArgsBase args = null;

            while (leftTokens > 0)
            {
                // Read first token of current sub-command or path
                if (!RespReadUtils.TrySliceWithLengthHeader(out var tokenSpan, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                leftTokens--;

                // Check if first token defines the start of a new sub-command (cmdType) or a path
                if (!readPathsOnly && (tokenSpan.SequenceEqual(CmdStrings.READ) ||
                                       tokenSpan.SequenceEqual(CmdStrings.read)))
                {
                    args = new RegisterCmdArgs { CommandType = CommandType.Read };
                }
                else if (!readPathsOnly && (tokenSpan.SequenceEqual(CmdStrings.READMODIFYWRITE) ||
                                            tokenSpan.SequenceEqual(CmdStrings.readmodifywrite) ||
                                            tokenSpan.SequenceEqual(CmdStrings.RMW) ||
                                            tokenSpan.SequenceEqual(CmdStrings.rmw)))
                {
                    args = new RegisterCmdArgs { CommandType = CommandType.ReadModifyWrite };
                }
                else if (!readPathsOnly && (tokenSpan.SequenceEqual(CmdStrings.TRANSACTION) ||
                                            tokenSpan.SequenceEqual(CmdStrings.transaction) ||
                                            tokenSpan.SequenceEqual(CmdStrings.TXN) ||
                                            tokenSpan.SequenceEqual(CmdStrings.txn)))
                {
                    args = new RegisterTxnArgs();
                }
                else if (tokenSpan.SequenceEqual(CmdStrings.INFO) ||
                         tokenSpan.SequenceEqual(CmdStrings.info))
                {
                    // If first token is not a cmdType and no other sub-command is previously defined, command is malformed
                    if (classNameToRegisterArgs.Count == 0 || leftTokens == 0)
                    {
                        errorMsg = CmdStrings.RESP_ERR_GENERIC_MALFORMED_REGISTERCS_COMMAND;
                        break;
                    }

                    if (!RespReadUtils.ReadStringWithLengthHeader(out cmdInfoPath, ref ptr, recvBufferPtr + bytesRead))
                        return false;

                    leftTokens--;
                    continue;
                }
                else if (readPathsOnly || (tokenSpan.SequenceEqual(CmdStrings.SRC) ||
                                           tokenSpan.SequenceEqual(CmdStrings.src)))
                {
                    // If first token is not a cmdType and no other sub-command is previously defined, command is malformed
                    if (classNameToRegisterArgs.Count == 0)
                    {
                        errorMsg = CmdStrings.RESP_ERR_GENERIC_MALFORMED_REGISTERCS_COMMAND;
                        break;
                    }

                    // Read only binary paths from this point forth
                    if (readPathsOnly)
                    {
                        var path = Encoding.ASCII.GetString(tokenSpan);
                        binaryPaths.Add(path);
                    }

                    readPathsOnly = true;

                    continue;
                }
                else
                {
                    // Check optional parameters for previous sub-command
                    if (optionalParamsRead == 0 && args is RegisterCmdArgs cmdArgs)
                    {
                        var expTicks = NumUtils.BytesToLong(tokenSpan);
                        cmdArgs.ExpirationTicks = expTicks;
                        optionalParamsRead++;
                        continue;
                    }

                    // Unexpected token
                    errorMsg = CmdStrings.RESP_ERR_GENERIC_MALFORMED_REGISTERCS_COMMAND;
                    break;
                }

                optionalParamsRead = 0;

                // At this point we expect at least 6 remaining tokens -
                // 3 more tokens for command definition + 2 for source definition
                if (leftTokens < 5)
                {
                    errorMsg = CmdStrings.RESP_ERR_GENERIC_MALFORMED_REGISTERCS_COMMAND;
                    break;
                }

                // Start reading the sub-command arguments
                // Read custom command name
                if (!RespReadUtils.ReadStringWithLengthHeader(out var name, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                leftTokens--;
                args.Name = name;

                // Read custom command number of parameters
                if (!RespReadUtils.ReadIntWithLengthHeader(out var numParams, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                leftTokens--;
                args.NumParams = numParams;

                // Read custom command class name
                if (!RespReadUtils.ReadStringWithLengthHeader(out var className, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                leftTokens--;

                // Add sub-command arguments
                if (!classNameToRegisterArgs.ContainsKey(className))
                    classNameToRegisterArgs.Add(className, new List<RegisterArgsBase>());

                classNameToRegisterArgs[className].Add(args);
            }

            // If ended before reading command, drain tokens not read from pipe
            while (leftTokens > 0)
            {
                if (!RespReadUtils.TrySliceWithLengthHeader(out _, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                leftTokens--;
            }

            // If no error is found, continue to try register custom commands in the server
            if (errorMsg == null &&
                TryRegisterCustomCommands(binaryPaths, cmdInfoPath, classNameToRegisterArgs, customCommandManager, out errorMsg))
            {
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.WriteError(errorMsg, ref dcurr, dend))
                    SendAndReset();
            }

            readHead = (int)(ptr - recvBufferPtr);

            return true;
        }

        private void NetworkModuleLoad(CustomCommandManager customCommandManager)
        {
            if (parseState.count < 1) // At least module path is required
            {
                AbortWithWrongNumberOfArguments("MODULE LOADCS", parseState.count);
                return;
            }

            // Read path to module file
            var modulePath = parseState.GetArgSliceByRef(0).ToString();

            // Read module args
            var moduleArgs = new string[parseState.count - 1];
            for (var i = 0; i < moduleArgs.Length; i++)
                moduleArgs[i] = parseState.GetArgSliceByRef(i + 1).ToString();

            if (LoadAssemblies([modulePath], out var loadedAssemblies, out var errorMsg))
            {
                Debug.Assert(loadedAssemblies != null);
                var assembliesList = loadedAssemblies.ToList();
                Debug.Assert(assembliesList.Count == 1, "Only one assembly per module load");

                if (ModuleRegistrar.Instance.LoadModule(customCommandManager, assembliesList[0], moduleArgs, logger, out errorMsg))
                {
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
            }

            if (errorMsg != default)
            {
                while (!RespWriteUtils.WriteError(errorMsg, ref dcurr, dend))
                    SendAndReset();
            }
        }

    }
}