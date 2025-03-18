// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using Garnet.common;
using Garnet.server.Auth.Settings;
using Garnet.server.Custom;

namespace Garnet.server
{
    /// <summary>
    /// Server session for RESP protocol - admin commands are in this file
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        internal int noScriptStart;
        internal ulong[] noScriptBitmap;

        private void ProcessAdminCommands<TGarnetApi>(RespCommand command, ref TGarnetApi storageApi) where TGarnetApi : IGarnetApi
        {
            /*
             * WARNING: Here is safe to add @slow commands (check how containsSlowCommand is used).
             */
            containsSlowCommand = true;
            if (_authenticator.CanAuthenticate && !_authenticator.IsAuthenticated)
            {
                // If the current session is unauthenticated, we stop parsing, because no other commands are allowed
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_NOAUTH, ref dcurr, dend))
                    SendAndReset();
            }

            var cmdFound = true;
            _ = command switch
            {
                RespCommand.CONFIG_GET => NetworkCONFIG_GET(),
                RespCommand.CONFIG_REWRITE => NetworkCONFIG_REWRITE(),
                RespCommand.CONFIG_SET => NetworkCONFIG_SET(),
                RespCommand.FAILOVER or
                RespCommand.REPLICAOF or
                RespCommand.MIGRATE or
                RespCommand.SECONDARYOF => NetworkProcessClusterCommand(command),
                RespCommand.LATENCY_HELP => NetworkLatencyHelp(),
                RespCommand.LATENCY_HISTOGRAM => NetworkLatencyHistogram(),
                RespCommand.LATENCY_RESET => NetworkLatencyReset(),
                RespCommand.SLOWLOG_HELP => NetworkSlowLogHelp(),
                RespCommand.SLOWLOG_GET => NetworkSlowLogGet(),
                RespCommand.SLOWLOG_LEN => NetworkSlowLogLen(),
                RespCommand.SLOWLOG_RESET => NetworkSlowLogReset(),
                RespCommand.ROLE => NetworkROLE(),
                RespCommand.SAVE => NetworkSAVE(),
                RespCommand.LASTSAVE => NetworkLASTSAVE(),
                RespCommand.BGSAVE => NetworkBGSAVE(),
                RespCommand.COMMITAOF => NetworkCOMMITAOF(),
                RespCommand.FORCEGC => NetworkFORCEGC(),
                RespCommand.HCOLLECT => NetworkHCOLLECT(ref storageApi),
                RespCommand.MONITOR => NetworkMonitor(),
                RespCommand.ACL_DELUSER => NetworkAclDelUser(),
                RespCommand.ACL_GETUSER => NetworkAclGetUser(),
                RespCommand.ACL_LIST => NetworkAclList(),
                RespCommand.ACL_LOAD => NetworkAclLoad(),
                RespCommand.ACL_SETUSER => NetworkAclSetUser(),
                RespCommand.ACL_USERS => NetworkAclUsers(),
                RespCommand.ACL_SAVE => NetworkAclSave(),
                RespCommand.DEBUG => NetworkDebug(),
                RespCommand.REGISTERCS => NetworkRegisterCs(storeWrapper.customCommandManager),
                RespCommand.MODULE_LOADCS => NetworkModuleLoad(storeWrapper.customCommandManager),
                RespCommand.PURGEBP => NetworkPurgeBP(),
                _ => cmdFound = false
            };

            if (cmdFound) return;

            if (command.IsClusterSubCommand())
            {
                NetworkProcessClusterCommand(command);
                return;
            }

            while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_UNK_CMD, ref dcurr, dend))
                SendAndReset();
        }

        /// <summary>
        /// For sessions that are hosting scripting calls, checks that the parsed command is runnable.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool CheckScriptPermissions(RespCommand cmd)
        {
            if (noScriptBitmap != null)
            {
                var ix = (int)cmd - noScriptStart;
                var ulongIndex = ix / sizeof(ulong);
                if (ulongIndex >= 0 && ulongIndex < noScriptBitmap.Length)
                {
                    var bitmapIndex = ix % sizeof(ulong);
                    var mask = 1UL << bitmapIndex;

                    if ((noScriptBitmap[ulongIndex] & mask) != 0)
                    {
                        OnACLOrNoScriptFailure(this, cmd);
                        return false;
                    }
                }
            }

            return true;
        }

        /// <summary>
        /// Performs permission checks for the current user and the given command.
        /// (NOTE: This function does not check keyspaces)
        /// </summary>
        /// <param name="cmd">Command be processed</param>
        /// <returns>True if the command execution is allowed to continue, otherwise false.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool CheckACLPermissions(RespCommand cmd)
        {
            Debug.Assert(!_authenticator.IsAuthenticated || (_userHandle != null));

            // Authentication and authorization checks must be performed against the effective user.
            if ((!_authenticator.IsAuthenticated || !_userHandle.User.CanAccessCommand(cmd)) && !cmd.IsNoAuth())
            {
                OnACLOrNoScriptFailure(this, cmd);
                return false;
            }
            return true;
        }

        /// <summary>
        /// Handle ACL or NoScript failures.
        /// 
        /// Failing should be rare, and is not important for performance so hide this behind
        /// a method call to keep icache pressure down
        /// </summary>
        /// <param name="self"></param>
        /// <param name="cmd"></param>
        [MethodImpl(MethodImplOptions.NoInlining)]
        static void OnACLOrNoScriptFailure(RespServerSession self, RespCommand cmd)
        {
            // If we're rejecting a command, we may need to cleanup some ambient state too
            if (cmd == RespCommand.CustomRawStringCmd)
            {
                self.currentCustomRawStringCommand = null;
            }
            else if (cmd == RespCommand.CustomObjCmd)
            {
                self.currentCustomObjectCommand = null;
            }
            else if (cmd == RespCommand.CustomTxn)
            {
                self.currentCustomTransaction = null;
            }
            else if (cmd == RespCommand.CustomProcedure)
            {
                self.currentCustomProcedure = null;
            }
        }

        void CommitAof()
        {
            storeWrapper.appendOnlyFile?.CommitAsync().ConfigureAwait(false).GetAwaiter().GetResult();
        }

        private bool NetworkMonitor()
        {
            if (parseState.Count != 0)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.MONITOR));
            }

            while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_UNK_CMD, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        private bool TryImportCommandsData<TData>(string cmdDataPath, out IReadOnlyDictionary<string, TData> cmdNameToData, out ReadOnlySpan<byte> errorMessage) where TData : class, IRespCommandData<TData>
        {
            cmdNameToData = default;
            errorMessage = default;

            // Check command info path, if specified
            if (!File.Exists(cmdDataPath))
            {
                errorMessage = CmdStrings.RESP_ERR_GENERIC_GETTING_CMD_INFO_FILE;
                return false;
            }

            // Check command info path is in allowed paths
            if (storeWrapper.serverOptions.ExtensionBinPaths.All(p => !FileUtils.IsFileInDirectory(cmdDataPath, p)))
            {
                errorMessage = CmdStrings.RESP_ERR_GENERIC_CMD_INFO_FILE_NOT_IN_ALLOWED_PATHS;
                return false;
            }

            var streamProvider = StreamProviderFactory.GetStreamProvider(FileLocationType.Local);
            var commandsInfoProvider =
                RespCommandsDataProviderFactory.GetRespCommandsDataProvider<TData>();

            var importSucceeded = commandsInfoProvider.TryImportRespCommandsData(cmdDataPath,
                streamProvider, out cmdNameToData, logger);

            if (!importSucceeded)
            {
                errorMessage = CmdStrings.RESP_ERR_GENERIC_MALFORMED_COMMAND_INFO_JSON;
                return false;
            }

            return true;
        }

        /// <summary>
        /// Register all custom commands / transactions
        /// </summary>
        /// <param name="binaryPaths">Binary paths from which to load assemblies</param>
        /// <param name="cmdInfoPath">Path of JSON file containing RespCommandsInfo for custom commands</param>
        /// <param name="cmdDocsPath">Path of JSON file containing RespCommandDocs for custom commands</param>
        /// <param name="classNameToRegisterArgs">Mapping between class names to register and arguments required for registration</param>
        /// <param name="customCommandManager">CustomCommandManager instance used to register commands</param>
        /// <param name="errorMessage">If method returned false, contains ASCII encoded generic error string; otherwise <c>default</c></param>
        /// <returns>A boolean value indicating whether registration of the custom commands was successful.</returns>
        private bool TryRegisterCustomCommands(
            IEnumerable<string> binaryPaths,
            string cmdInfoPath,
            string cmdDocsPath,
            Dictionary<string, List<RegisterArgsBase>> classNameToRegisterArgs,
            CustomCommandManager customCommandManager,
            out ReadOnlySpan<byte> errorMessage)
        {
            errorMessage = default;
            var classInstances = new Dictionary<string, object>();

            IReadOnlyDictionary<string, RespCommandsInfo> cmdNameToInfo = default;
            IReadOnlyDictionary<string, RespCommandDocs> cmdNameToDocs = default;

            if (cmdInfoPath != null)
            {
                if (!TryImportCommandsData(cmdInfoPath, out cmdNameToInfo, out errorMessage))
                    return false;
            }

            if (cmdDocsPath != null)
            {
                if (!TryImportCommandsData(cmdDocsPath, out cmdNameToDocs, out errorMessage))
                    return false;
            }

            if (!ModuleUtils.LoadAssemblies(binaryPaths, storeWrapper.serverOptions.ExtensionBinPaths,
                storeWrapper.serverOptions.ExtensionAllowUnsignedAssemblies, out var loadedAssemblies, out errorMessage))
                return false;

            foreach (var c in classNameToRegisterArgs.Keys)
            {
                classInstances.TryAdd(c, null);
            }

            // Also add custom object command class names to instantiate
            var objectCommandArgs = classNameToRegisterArgs.Values
                .SelectMany(cmdArgsList => cmdArgsList)
                .OfType<RegisterCmdArgs>()
                .Where(args => !string.IsNullOrEmpty(args.ObjectCommandName))
                .ToList();

            objectCommandArgs.ForEach(objCmdArgs => classInstances.TryAdd(objCmdArgs.ObjectCommandName, null));

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

            objectCommandArgs.ForEach(objCmdArgs => objCmdArgs.ObjectCommand = (CustomObjectFunctions)classInstances[objCmdArgs.ObjectCommandName]);

            // Register each command / transaction using its specified class instance
            var registerApis = new List<IRegisterCustomCommandProvider>();
            foreach (var classNameToArgs in classNameToRegisterArgs)
            {
                foreach (var args in classNameToArgs.Value)
                {
                    // Add command info to register arguments, if exists
                    if (cmdNameToInfo != null && cmdNameToInfo.TryGetValue(args.Name, out var cmdInfo))
                    {
                        args.CommandInfo = cmdInfo;
                    }

                    // Add command docs to register arguments, if exists
                    if (cmdNameToDocs != null && cmdNameToDocs.TryGetValue(args.Name, out var cmdDocs))
                    {
                        args.CommandDocs = cmdDocs;
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
        private bool NetworkRegisterCs(CustomCommandManager customCommandManager)
        {
            var readPathsOnly = false;
            var optionalParamsRead = 0;

            var binaryPaths = new HashSet<string>();
            string cmdInfoPath = default;
            string cmdDocsPath = default;

            // Custom class name to arguments read from each sub-command
            var classNameToRegisterArgs = new Dictionary<string, List<RegisterArgsBase>>();

            ReadOnlySpan<byte> errorMsg = null;

            if (parseState.Count < 6)
                errorMsg = CmdStrings.RESP_ERR_GENERIC_MALFORMED_REGISTERCS_COMMAND;

            // Parse the REGISTERCS command - list of registration sub-commands
            // followed by an optional path to JSON file containing an array of RespCommandsInfo objects,
            // followed by an optional path to JSON file containing an array of RespCommandsDocs objects,
            // followed by a list of paths to binary files / folders
            // Syntax - REGISTERCS cmdType name numParams className [expTicks] [objCmdName] [cmdType name numParams className [expTicks] [objCmdName]...]
            // [INFO path] [DOCS path] SRC path [path ...]
            RegisterArgsBase args = null;

            var tokenIdx = 0;
            while (tokenIdx < parseState.Count)
            {
                // Read first token of current sub-command or path
                var token = parseState.GetArgSliceByRef(tokenIdx++).ReadOnlySpan;

                // Check if first token defines the start of a new sub-command (cmdType) or a path
                if (!readPathsOnly && token.EqualsUpperCaseSpanIgnoringCase(CmdStrings.READ))
                {
                    args = new RegisterCmdArgs { CommandType = CommandType.Read };
                }
                else if (!readPathsOnly && (token.EqualsUpperCaseSpanIgnoringCase(CmdStrings.READMODIFYWRITE) ||
                                            token.EqualsUpperCaseSpanIgnoringCase(CmdStrings.RMW)))
                {
                    args = new RegisterCmdArgs { CommandType = CommandType.ReadModifyWrite };
                }
                else if (!readPathsOnly && (token.EqualsUpperCaseSpanIgnoringCase(CmdStrings.TRANSACTION) ||
                                            token.EqualsUpperCaseSpanIgnoringCase(CmdStrings.TXN)))
                {
                    args = new RegisterTxnArgs();
                }
                else if (token.EqualsUpperCaseSpanIgnoringCase(CmdStrings.INFO))
                {
                    // If first token is not a cmdType and no other sub-command is previously defined, command is malformed
                    if (classNameToRegisterArgs.Count == 0 || tokenIdx == parseState.Count)
                    {
                        errorMsg = CmdStrings.RESP_ERR_GENERIC_MALFORMED_REGISTERCS_COMMAND;
                        break;
                    }

                    cmdInfoPath = parseState.GetString(tokenIdx++);
                    continue;
                }
                else if (token.EqualsUpperCaseSpanIgnoringCase(CmdStrings.DOCS))
                {
                    // If first token is not a cmdType and no other sub-command is previously defined, command is malformed
                    if (classNameToRegisterArgs.Count == 0 || tokenIdx == parseState.Count)
                    {
                        errorMsg = CmdStrings.RESP_ERR_GENERIC_MALFORMED_REGISTERCS_COMMAND;
                        break;
                    }

                    cmdDocsPath = parseState.GetString(tokenIdx++);
                    continue;
                }
                else if (readPathsOnly || token.EqualsUpperCaseSpanIgnoringCase(CmdStrings.SRC))
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
                        var path = Encoding.ASCII.GetString(token);
                        binaryPaths.Add(path);
                    }

                    readPathsOnly = true;

                    continue;
                }
                else
                {
                    // Check optional parameters for previous sub-command
                    if (optionalParamsRead < 2 && args is RegisterCmdArgs cmdArgs)
                    {
                        if (NumUtils.TryReadInt64(token, out var expTicks))
                        {
                            cmdArgs.ExpirationTicks = expTicks;
                            optionalParamsRead++;
                            continue;
                        }
                        else // Treat the argument as custom object command name
                        {
                            cmdArgs.ObjectCommandName = Encoding.ASCII.GetString(token);
                            optionalParamsRead++;
                            continue;
                        }
                    }

                    // Unexpected token
                    errorMsg = CmdStrings.RESP_ERR_GENERIC_MALFORMED_REGISTERCS_COMMAND;
                    break;
                }

                optionalParamsRead = 0;

                // At this point we expect at least 6 remaining tokens -
                // 3 more tokens for command definition + 2 for source definition
                if (parseState.Count - tokenIdx < 5)
                {
                    errorMsg = CmdStrings.RESP_ERR_GENERIC_MALFORMED_REGISTERCS_COMMAND;
                    break;
                }

                // Start reading the sub-command arguments
                // Read custom command name
                args.Name = parseState.GetString(tokenIdx++);

                // Read custom command number of parameters
                if (!parseState.TryGetInt(tokenIdx++, out var numParams))
                {
                    errorMsg = CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER;
                    break;
                }
                args.NumParams = numParams;

                // Read custom command class name
                var className = parseState.GetString(tokenIdx++);

                // Add sub-command arguments
                if (!classNameToRegisterArgs.ContainsKey(className))
                    classNameToRegisterArgs.Add(className, new List<RegisterArgsBase>());

                classNameToRegisterArgs[className].Add(args);
            }

            // If no error is found, continue to try register custom commands in the server
            if (errorMsg.IsEmpty &&
                TryRegisterCustomCommands(binaryPaths, cmdInfoPath, cmdDocsPath, classNameToRegisterArgs, customCommandManager, out errorMsg))
            {
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteError(errorMsg, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        private bool NetworkModuleLoad(CustomCommandManager customCommandManager)
        {
            if (parseState.Count < 1) // At least module path is required
            {
                AbortWithWrongNumberOfArguments($"{RespCommand.MODULE}|{Encoding.ASCII.GetString(CmdStrings.LOADCS)}");
                return true;
            }

            // Read path to module file
            var modulePath = parseState.GetArgSliceByRef(0).ToString();

            // Read module args
            var moduleArgs = new string[parseState.Count - 1];
            for (var i = 0; i < moduleArgs.Length; i++)
                moduleArgs[i] = parseState.GetArgSliceByRef(i + 1).ToString();

            var errorMsg = ReadOnlySpan<byte>.Empty;

            var binPath = Path.GetDirectoryName(modulePath);
            var moduleFileName = Path.GetFileName(modulePath);

            // Load dependencies from the module path
            if (Directory.Exists(binPath) && !ModuleUtils.LoadAssemblies([binPath],
                    storeWrapper.serverOptions.ExtensionBinPaths,
                    storeWrapper.serverOptions.ExtensionAllowUnsignedAssemblies, out _, out errorMsg, [moduleFileName],
                    SearchOption.TopDirectoryOnly, true))
            {
                if (!errorMsg.IsEmpty)
                {
                    while (!RespWriteUtils.TryWriteError(errorMsg, ref dcurr, dend))
                        SendAndReset();
                }

                return true;
            }

            // Load the module path
            if (ModuleUtils.LoadAssemblies([modulePath], storeWrapper.serverOptions.ExtensionBinPaths,
                storeWrapper.serverOptions.ExtensionAllowUnsignedAssemblies, out var loadedAssemblies, out errorMsg))
            {
                Debug.Assert(loadedAssemblies != null);
                var assembliesList = loadedAssemblies.ToList();
                Debug.Assert(assembliesList.Count == 1, "Only one assembly per module load");

                if (ModuleRegistrar.Instance.LoadModule(customCommandManager, assembliesList[0], moduleArgs, logger, out errorMsg))
                {
                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
            }

            if (!errorMsg.IsEmpty)
            {
                while (!RespWriteUtils.TryWriteError(errorMsg, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        private bool NetworkCOMMITAOF()
        {
            if (parseState.Count != 0)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.COMMITAOF));
            }

            CommitAof();
            while (!RespWriteUtils.TryWriteSimpleString("AOF file committed"u8, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        private bool NetworkFORCEGC()
        {
            if (parseState.Count > 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.FORCEGC));
            }

            var generation = GC.MaxGeneration;
            if (parseState.Count == 1)
            {
                if (!parseState.TryGetInt(0, out generation) || generation < 0 || generation > GC.MaxGeneration)
                {
                    while (!RespWriteUtils.TryWriteError("ERR Invalid GC generation."u8, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }
            }

            GC.Collect(generation, GCCollectionMode.Forced, true);
            while (!RespWriteUtils.TryWriteSimpleString("GC completed"u8, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        private bool NetworkHCOLLECT<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.HCOLLECT));
            }

            var keys = parseState.Parameters;

            var header = new RespInputHeader(GarnetObjectType.Hash) { HashOp = HashOperation.HCOLLECT };
            var input = new ObjectInput(header);

            var status = storageApi.HashCollect(keys, ref input);

            switch (status)
            {
                case GarnetStatus.OK:
                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                    break;
                default:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_HCOLLECT_ALREADY_IN_PROGRESS, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            return true;
        }

        private bool NetworkProcessClusterCommand(RespCommand command)
        {
            if (clusterSession == null)
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_CLUSTER_DISABLED, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            clusterSession.ProcessClusterCommands(command, ref parseState, ref dcurr, ref dend);
            return true;
        }

        private bool NetworkDebug()
        {
            if (parseState.Count == 0)
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_NUMBER_OF_ARGUMENTS, ref dcurr, dend))
                    SendAndReset();

                return true;
            }

            if (
                    (storeWrapper.serverOptions.EnableDebugCommand == ConnectionProtectionOption.No)
                 || (
                        (storeWrapper.serverOptions.EnableDebugCommand == ConnectionProtectionOption.Local)
                      && !networkSender.IsLocalConnection()
                    )
               )
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_DEUBG_DISALLOWED, ref dcurr, dend))
                    SendAndReset();

                return true;
            }

            var command = parseState.GetArgSliceByRef(0).ReadOnlySpan;

            if (command.EqualsUpperCaseSpanIgnoringCase(CmdStrings.PANIC))
                // Throwing an exception is intentional and desirable for this command.
                throw new GarnetException(Microsoft.Extensions.Logging.LogLevel.Debug, panic: true);

            if (command.EqualsUpperCaseSpanIgnoringCase(CmdStrings.ERROR))
            {
                if (parseState.Count != 2)
                {
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_NUMBER_OF_ARGUMENTS, ref dcurr, dend))
                        SendAndReset();

                    return true;
                }

                while (!RespWriteUtils.TryWriteError(parseState.GetString(1), ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (command.EqualsUpperCaseSpanIgnoringCase(CmdStrings.HELP))
            {
                var help = new List<string>()
                {
                    "DEBUG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                    "PANIC",
                    "\tCrash the server simulating a panic.",
                    "ERROR <string>",
                    "\tReturn a Redis protocol error with <string> as message. Useful for clients",
                    "\tunit tests to simulate Redis errors.",
                    "HELP",
                    "\tPrints this help"
                };

                while (!RespWriteUtils.TryWriteArrayLength(help.Count, ref dcurr, dend))
                    SendAndReset();

                foreach (var line in help)
                {
                    while (!RespWriteUtils.TryWriteSimpleString(line, ref dcurr, dend))
                        SendAndReset();
                }

                return true;
            }

            var error = string.Format(CmdStrings.GenericErrUnknownSubCommand, parseState.GetString(0), nameof(RespCommand.DEBUG));
            while (!RespWriteUtils.TryWriteError(error, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        private bool NetworkROLE()
        {
            if (parseState.Count != 0)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.ROLE));
            }

            if (!storeWrapper.serverOptions.EnableCluster)
            {
                while (!RespWriteUtils.TryWriteArrayLength(3, ref dcurr, dend))
                    SendAndReset();

                while (!RespWriteUtils.TryWriteAsciiBulkString("master", ref dcurr, dend))
                    SendAndReset();

                while (!RespWriteUtils.TryWriteInt32(0, ref dcurr, dend))
                    SendAndReset();

                while (!RespWriteUtils.TryWriteEmptyArray(ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                if (storeWrapper.clusterProvider.IsPrimary())
                {
                    var (replication_offset, replicaInfo) = storeWrapper.clusterProvider.GetPrimaryInfo();

                    while (!RespWriteUtils.TryWriteArrayLength(3, ref dcurr, dend))
                        SendAndReset();

                    while (!RespWriteUtils.TryWriteAsciiBulkString("master", ref dcurr, dend))
                        SendAndReset();

                    while (!RespWriteUtils.TryWriteInt64(replication_offset, ref dcurr, dend))
                        SendAndReset();

                    while (!RespWriteUtils.TryWriteArrayLength(replicaInfo.Count, ref dcurr, dend))
                        SendAndReset();

                    foreach (var replice in replicaInfo)
                    {
                        while (!RespWriteUtils.TryWriteArrayLength(3, ref dcurr, dend))
                            SendAndReset();
                        while (!RespWriteUtils.TryWriteAsciiBulkString(replice.address, ref dcurr, dend))
                            SendAndReset();
                        while (!RespWriteUtils.TryWriteInt32(replice.port, ref dcurr, dend))
                            SendAndReset();
                        while (!RespWriteUtils.TryWriteInt64(replice.replication_offset, ref dcurr, dend))
                            SendAndReset();
                    }
                }
                else
                {
                    var role = storeWrapper.clusterProvider.GetReplicaInfo();

                    while (!RespWriteUtils.TryWriteArrayLength(5, ref dcurr, dend))
                        SendAndReset();

                    while (!RespWriteUtils.TryWriteAsciiBulkString("slave", ref dcurr, dend))
                        SendAndReset();

                    while (!RespWriteUtils.TryWriteAsciiBulkString(role.address, ref dcurr, dend))
                        SendAndReset();

                    while (!RespWriteUtils.TryWriteInt32(role.port, ref dcurr, dend))
                        SendAndReset();

                    while (!RespWriteUtils.TryWriteAsciiBulkString(role.replication_state, ref dcurr, dend))
                        SendAndReset();

                    while (!RespWriteUtils.TryWriteInt64(role.replication_offset, ref dcurr, dend))
                        SendAndReset();
                }
            }

            return true;
        }

        private bool NetworkSAVE()
        {
            if (parseState.Count != 0)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.SAVE));
            }

            if (!storeWrapper.TakeCheckpoint(false, logger))
            {
                while (!RespWriteUtils.TryWriteError("ERR checkpoint already in progress"u8, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        private bool NetworkLASTSAVE()
        {
            if (parseState.Count != 0)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.SAVE));
            }

            var seconds = storeWrapper.lastSaveTime.ToUnixTimeSeconds();
            while (!RespWriteUtils.TryWriteInt64(seconds, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        private bool NetworkBGSAVE()
        {
            if (parseState.Count > 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.BGSAVE));
            }

            var success = storeWrapper.TakeCheckpoint(true, logger);
            if (success)
            {
                while (!RespWriteUtils.TryWriteSimpleString("Background saving started"u8, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteError("ERR checkpoint already in progress"u8, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }
    }
}