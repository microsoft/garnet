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
using Garnet.common;
using Garnet.server.Custom;
using Garnet.server.Module;

namespace Garnet.server
{
    /// <summary>
    /// Server session for RESP protocol - admin commands are in this file
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        private bool ProcessAdminCommands(RespCommand command, int count)
        {
            hasAdminCommand = true;

            if (_authenticator.CanAuthenticate && !_authenticator.IsAuthenticated)
            {
                // If the current session is unauthenticated, we stop parsing, because no other commands are allowed
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_NOAUTH, ref dcurr, dend))
                    SendAndReset();

                return true;
            }

            var cmdFound = true;
            _ = command switch
            {
                RespCommand.CONFIG_GET => NetworkCONFIG_GET(count),
                RespCommand.CONFIG_REWRITE => NetworkCONFIG_REWRITE(count),
                RespCommand.CONFIG_SET => NetworkCONFIG_SET(count),
                RespCommand.FAILOVER or
                RespCommand.REPLICAOF or
                RespCommand.SECONDARYOF => NetworkProcessClusterCommand(command, count),
                RespCommand.LATENCY_HELP => NetworkLatencyHelp(count),
                RespCommand.LATENCY_HISTOGRAM => NetworkLatencyHistogram(count),
                RespCommand.LATENCY_RESET => NetworkLatencyReset(count),
                RespCommand.SAVE => NetworkSAVE(count),
                RespCommand.LASTSAVE => NetworkLASTSAVE(count),
                RespCommand.BGSAVE => NetworkBGSAVE(count),
                RespCommand.COMMITAOF => NetworkCOMMITAOF(count),
                RespCommand.FORCEGC => NetworkFORCEGC(count),
                RespCommand.MONITOR => NetworkMonitor(count),
                RespCommand.ACL_DELUSER => NetworkAclDelUser(count),
                RespCommand.ACL_LIST => NetworkAclList(count),
                RespCommand.ACL_LOAD => NetworkAclLoad(count),
                RespCommand.ACL_SETUSER => NetworkAclSetUser(count),
                RespCommand.ACL_USERS => NetworkAclUsers(count),
                RespCommand.ACL_SAVE => NetworkAclSave(count),
                RespCommand.REGISTERCS => NetworkRegisterCs(count, storeWrapper.customCommandManager),
                RespCommand.MODULE_LOADCS => NetworkModuleLoad(count, storeWrapper.customCommandManager),
                _ => cmdFound = false
            };

            if (cmdFound) return true;

            if (command.IsClusterSubCommand())
            {
                NetworkProcessClusterCommand(command, count);
                return true;
            }

            while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_UNK_CMD, ref dcurr, dend))
                SendAndReset();

            return true;
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

        private bool NetworkMonitor(int count)
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
        private bool NetworkRegisterCs(int count, CustomCommandManager customCommandManager)
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
                var token = parseState.GetArgSliceByRef(count - leftTokens);
                leftTokens--;

                // Check if first token defines the start of a new sub-command (cmdType) or a path
                if (!readPathsOnly && token.ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.READ))
                {
                    args = new RegisterCmdArgs { CommandType = CommandType.Read };
                }
                else if (!readPathsOnly && (token.ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.READMODIFYWRITE) ||
                                            token.ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.RMW)))
                {
                    args = new RegisterCmdArgs { CommandType = CommandType.ReadModifyWrite };
                }
                else if (!readPathsOnly && (token.ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.TRANSACTION) ||
                                            token.ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.TXN)))
                {
                    args = new RegisterTxnArgs();
                }
                else if (token.ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.INFO))
                {
                    // If first token is not a cmdType and no other sub-command is previously defined, command is malformed
                    if (classNameToRegisterArgs.Count == 0 || leftTokens == 0)
                    {
                        errorMsg = CmdStrings.RESP_ERR_GENERIC_MALFORMED_REGISTERCS_COMMAND;
                        break;
                    }

                    cmdInfoPath = parseState.GetString(count - leftTokens);
                    leftTokens--;
                    continue;
                }
                else if (readPathsOnly || token.ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.SRC))
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
                        var path = Encoding.ASCII.GetString(token.Span);
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
                        var expTicks = NumUtils.BytesToLong(token.Span);
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
                args.Name = parseState.GetString(count - leftTokens);
                leftTokens--;

                // Read custom command number of parameters
                if (!parseState.TryGetInt(count - leftTokens, out var numParams))
                {
                    errorMsg = CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER;
                    break;
                }
                args.NumParams = numParams;
                leftTokens--;

                // Read custom command class name
                var className = parseState.GetString(count - leftTokens);
                leftTokens--;

                // Add sub-command arguments
                if (!classNameToRegisterArgs.ContainsKey(className))
                    classNameToRegisterArgs.Add(className, new List<RegisterArgsBase>());

                classNameToRegisterArgs[className].Add(args);
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

            return true;
        }

        private bool NetworkModuleLoad(int count, CustomCommandManager customCommandManager)
        {
            if (count < 1) // At least module path is required
            {
                AbortWithWrongNumberOfArguments($"{RespCommand.MODULE}|{Encoding.ASCII.GetString(CmdStrings.LOADCS)}", count);
                return true;
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

            return true;
        }

        private bool NetworkCOMMITAOF(int count)
        {
            if (count != 0)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.COMMITAOF), count);
            }

            CommitAof();
            while (!RespWriteUtils.WriteSimpleString("AOF file committed"u8, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        private bool NetworkFORCEGC(int count)
        {
            if (count > 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.FORCEGC), count);
            }

            var generation = GC.MaxGeneration;
            if (count == 1)
            {
                generation = parseState.GetInt(0);
                
                if (generation < 0 || generation > GC.MaxGeneration)
                {
                    while (!RespWriteUtils.WriteError("ERR Invalid GC generation."u8, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }
            }

            GC.Collect(generation, GCCollectionMode.Forced, true);
            while (!RespWriteUtils.WriteSimpleString("GC completed"u8, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        private bool NetworkProcessClusterCommand(RespCommand command, int count)
        {
            if (clusterSession == null)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_CLUSTER_DISABLED, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            clusterSession.ProcessClusterCommands(command, count, recvBufferPtr, bytesRead, ref readHead, ref dcurr, ref dend, out var result);
            return result;
        }

        private bool NetworkSAVE(int count)
        {
            if (count != 0)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.SAVE), count);
            }

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

            return true;
        }

        private bool NetworkLASTSAVE(int count)
        {
            if (count != 0)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.SAVE), count);
            }

            var seconds = storeWrapper.lastSaveTime.ToUnixTimeSeconds();
            while (!RespWriteUtils.WriteInteger(seconds, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        private bool NetworkBGSAVE(int count)
        {
            if (count > 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.BGSAVE), count);
            }

            var success = storeWrapper.TakeCheckpoint(true, StoreType.All, logger);
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

            return true;
        }
    }
}