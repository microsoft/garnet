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
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Server session for RESP protocol - array commands are in this file
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// MGET
        /// </summary>
        private bool NetworkMGET<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (storeWrapper.serverOptions.EnableScatterGatherGet)
                return NetworkMGET_SG(ref storageApi);

            SpanByte input = default;

            if (NetworkArraySlotVerify(interleavedKeys: false, readOnly: true))
                return true;

            while (!RespWriteUtils.WriteArrayLength(parseState.count, ref dcurr, dend))
                SendAndReset();

            for (int c = 0; c < parseState.count; c++)
            {
                var key = parseState.GetArgSliceByRef(c).SpanByte;
                var o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
                var status = storageApi.GET(ref key, ref input, ref o);

                switch (status)
                {
                    case GarnetStatus.OK:
                        if (!o.IsSpanByte)
                            SendAndReset(o.Memory, o.Length);
                        else
                            dcurr += o.Length;
                        break;
                    case GarnetStatus.NOTFOUND:
                        Debug.Assert(o.IsSpanByte);
                        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                            SendAndReset();
                        break;
                }
            }
            return true;
        }

        /// <summary>
        /// MGET - scatter gather version
        /// </summary>
        private bool NetworkMGET_SG<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetAdvancedApi
        {
            SpanByte input = default;
            long ctx = default;

            if (NetworkArraySlotVerify(interleavedKeys: false, readOnly: true))
            {
                return true;
            }

            int firstPending = -1;
            (GarnetStatus, SpanByteAndMemory)[] outputArr = null;

            // Write array length header
            while (!RespWriteUtils.WriteArrayLength(parseState.count, ref dcurr, dend))
                SendAndReset();

            SpanByteAndMemory o = new(dcurr, (int)(dend - dcurr));
            for (int c = 0; c < parseState.count; c++)
            {
                var key = parseState.GetArgSliceByRef(c).SpanByte;

                // Store index in context, since completions are not in order
                ctx = c;

                var status = storageApi.GET_WithPending(ref key, ref input, ref o, ctx, out bool isPending);

                if (isPending)
                {
                    if (firstPending == -1)
                    {
                        outputArr = new (GarnetStatus, SpanByteAndMemory)[parseState.count];
                        firstPending = c;
                    }
                    outputArr[c] = (status, default);
                    o = new SpanByteAndMemory();
                }
                else
                {
                    if (status == GarnetStatus.OK)
                    {
                        if (firstPending == -1)
                        {
                            // Found in memory without IO, and no earlier pending, so we can add directly to the output
                            if (!o.IsSpanByte)
                                SendAndReset(o.Memory, o.Length);
                            else
                                dcurr += o.Length;
                            o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
                        }
                        else
                        {
                            outputArr[c] = (status, o);
                            o = new SpanByteAndMemory();
                        }
                    }
                    else
                    {
                        if (firstPending == -1)
                        {
                            // Realized not-found without IO, and no earlier pending, so we can add directly to the output
                            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                                SendAndReset();
                            o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
                        }
                        else
                        {
                            outputArr[c] = (status, o);
                            o = new SpanByteAndMemory();
                        }
                    }
                }
            }

            if (firstPending != -1)
            {
                // First complete all pending ops
                storageApi.GET_CompletePending(outputArr, true);

                // Write the outputs to network buffer
                for (int i = firstPending; i < parseState.count; i++)
                {
                    var status = outputArr[i].Item1;
                    var output = outputArr[i].Item2;
                    if (status == GarnetStatus.OK)
                    {
                        if (!output.IsSpanByte)
                            SendAndReset(output.Memory, output.Length);
                        else
                            dcurr += output.Length;
                    }
                    else
                    {
                        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                            SendAndReset();
                    }
                }
            }
            return true;
        }

        private bool LoadAssemblies(IEnumerable<string> binaryPaths, out Assembly[] loadedAssemblies, out ReadOnlySpan<byte> errorMessage)
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
            if (!FileUtils.TryLoadAssemblies(binaryFiles, out var assemblies, out _))
            {
                errorMessage = CmdStrings.RESP_ERR_GENERIC_LOADING_ASSEMBLIES;
                return false;
            }

            loadedAssemblies = assemblies.ToArray();

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

        /// <summary>
        /// MSET
        /// </summary>
        private bool NetworkMSET<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            Debug.Assert(parseState.count % 2 == 0);

            if (NetworkArraySlotVerify(interleavedKeys: true, readOnly: false))
            {
                return true;
            }

            for (int c = 0; c < parseState.count; c += 2)
            {
                var key = parseState.GetArgSliceByRef(c).SpanByte;
                var val = parseState.GetArgSliceByRef(c + 1).SpanByte;
                _ = storageApi.SET(ref key, ref val);
            }
            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        /// <summary>
        /// MSETNX
        /// </summary>
        private bool NetworkMSETNX<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (NetworkArraySlotVerify(interleavedKeys: true, readOnly: false))
            {
                return true;
            }

            byte* hPtr = stackalloc byte[RespInputHeader.Size];

            bool anyValuesSet = false;
            for (int c = 0; c < parseState.count; c += 2)
            {
                var key = parseState.GetArgSliceByRef(c).SpanByte;
                var val = parseState.GetArgSliceByRef(c + 1).SpanByte;

                // We have to access the raw pointer in order to inject the input header
                byte* valPtr = val.ToPointer();
                int vsize = val.Length;

                valPtr -= sizeof(int);

                int saveV = *(int*)valPtr;
                *(int*)valPtr = vsize;

                // Make space for RespCommand in input
                valPtr -= RespInputHeader.Size;

                // Save state of memory to override with header
                Buffer.MemoryCopy(valPtr, hPtr, RespInputHeader.Size, RespInputHeader.Size);

                *(int*)valPtr = RespInputHeader.Size + vsize;
                ((RespInputHeader*)(valPtr + sizeof(int)))->cmd = RespCommand.SETEXNX;
                ((RespInputHeader*)(valPtr + sizeof(int)))->flags = 0;

                var status = storageApi.SET_Conditional(ref key, ref Unsafe.AsRef<SpanByte>(valPtr));

                // Status tells us whether an old image was found during RMW or not
                // For a "set if not exists", NOTFOUND means that the operation succeeded
                if (status == GarnetStatus.NOTFOUND)
                    anyValuesSet = true;

                // Put things back in place so that network buffer is not clobbered
                Buffer.MemoryCopy(hPtr, valPtr, RespInputHeader.Size, RespInputHeader.Size);
                valPtr += RespInputHeader.Size;
                *(int*)valPtr = saveV;
            }

            while (!RespWriteUtils.WriteInteger(anyValuesSet ? 1 : 0, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        private bool NetworkDEL<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (NetworkArraySlotVerify(interleavedKeys: false, readOnly: false))
            {
                return true;
            }

            int keysDeleted = 0;
            for (int c = 0; c < parseState.count; c++)
            {
                var key = parseState.GetArgSliceByRef(c).SpanByte;
                var status = storageApi.DELETE(ref key, StoreType.All);

                // This is only an approximate count because the deletion of a key on disk is performed as a blind tombstone append
                if (status == GarnetStatus.OK)
                    keysDeleted++;
            }

            while (!RespWriteUtils.WriteInteger(keysDeleted, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        /// <summary>
        /// SELECT
        /// </summary>
        /// <param name="ptr"></param>
        /// <returns></returns>
        private bool NetworkSELECT(byte* ptr)
        {
            // Read index
            if (!RespReadUtils.ReadIntWithLengthHeader(out var result, ref ptr, recvBufferPtr + bytesRead))
                return false;

            readHead = (int)(ptr - recvBufferPtr);

            if (storeWrapper.serverOptions.EnableCluster)
            {
                // Cluster mode does not allow DBID
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_SELECT_CLUSTER_MODE, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {

                if (result == 0)
                {
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_SELECT_INVALID_INDEX, ref dcurr, dend))
                        SendAndReset();
                }
            }
            return true;
        }

        private bool NetworkDBSIZE<TGarnetApi>(byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            while (!RespWriteUtils.WriteInteger(storageApi.GetDbSize(), ref dcurr, dend))
                SendAndReset();
            return true;
        }

        private bool NetworkKEYS<TGarnetApi>(byte* ptr, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            byte* pattern = null;
            int psize = 0;

            // Read pattern for keys filter
            if (!RespReadUtils.ReadPtrWithLengthHeader(ref pattern, ref psize, ref ptr, recvBufferPtr + bytesRead))
                return false;

            var patternAS = new ArgSlice(pattern, psize);

            var keys = storageApi.GetDbKeys(patternAS);

            if (keys.Count > 0)
            {
                // Write size of the array
                while (!RespWriteUtils.WriteArrayLength(keys.Count, ref dcurr, dend))
                    SendAndReset();

                // Write the keys matching the pattern
                foreach (var item in keys)
                {
                    while (!RespWriteUtils.WriteBulkString(item, ref dcurr, dend))
                        SendAndReset();
                }
            }
            else
            {
                while (!RespWriteUtils.WriteEmptyArray(ref dcurr, dend))
                    SendAndReset();
            }
            // Advance pointers
            readHead = (int)(ptr - recvBufferPtr);

            return true;
        }

        private bool NetworkSCAN<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
              where TGarnetApi : IGarnetApi
        {
            if (count < 1)
                return AbortWithWrongNumberOfArguments("SCAN", count);

            // Scan cursor [MATCH pattern] [COUNT count] [TYPE type]
            if (!RespReadUtils.ReadLongWithLengthHeader(out var cursorFromInput, ref ptr, recvBufferPtr + bytesRead))
            {
                return false;
            }

            int leftTokens = count - 1;

            ReadOnlySpan<byte> pattern = "*"u8;
            var allKeys = true;
            long countValue = 10;
            ReadOnlySpan<byte> typeParameterValue = default;

            while (leftTokens > 0)
            {
                if (!RespReadUtils.TrySliceWithLengthHeader(out var parameterWord, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (parameterWord.SequenceEqual(CmdStrings.MATCH) || parameterWord.SequenceEqual(CmdStrings.match))
                {
                    // Read pattern for keys filter
                    if (!RespReadUtils.TrySliceWithLengthHeader(out pattern, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                    allKeys = pattern.Length == 1 && pattern[0] == '*';
                    leftTokens--;
                }
                else if (parameterWord.SequenceEqual(CmdStrings.COUNT) || parameterWord.SequenceEqual(CmdStrings.count))
                {
                    if (!RespReadUtils.ReadLongWithLengthHeader(out countValue, ref ptr, recvBufferPtr + bytesRead))
                    {
                        return false;
                    }
                    leftTokens--;
                }
                else if (parameterWord.SequenceEqual(CmdStrings.TYPE) || parameterWord.SequenceEqual(CmdStrings.type))
                {
                    if (!RespReadUtils.TrySliceWithLengthHeader(out typeParameterValue, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                    leftTokens--;
                }
                leftTokens--;
            }

            var patternArgSlice = ArgSlice.FromPinnedSpan(pattern);

            storageApi.DbScan(patternArgSlice, allKeys, cursorFromInput, out var cursor, out var keys, typeParameterValue != default ? long.MaxValue : countValue, typeParameterValue);

            // Prepare values for output
            if (keys.Count == 0)
            {
                while (!RespWriteUtils.WriteArrayLength(2, ref dcurr, dend))
                    SendAndReset();

                // Number of keys "0"
                while (!RespWriteUtils.WriteLongAsSimpleString(0, ref dcurr, dend))
                    SendAndReset();

                // Empty array
                while (!RespWriteUtils.WriteEmptyArray(ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                // The response is two elements: the value of the cursor and the array of keys found matching the pattern
                while (!RespWriteUtils.WriteArrayLength(2, ref dcurr, dend))
                    SendAndReset();

                if (keys.Count > 0)
                    WriteOutputForScan(cursor, keys, ref dcurr, dend);
            }

            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        private bool NetworkTYPE<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
              where TGarnetApi : IGarnetApi
        {
            if (count != 1)
                return AbortWithWrongNumberOfArguments("TYPE", count);

            // TYPE key
            byte* keyPtr = null;
            int kSize = 0;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref kSize, ref ptr, recvBufferPtr + bytesRead))
                return false;

            var status = storageApi.GetKeyType(new(keyPtr, kSize), out string typeName);

            if (status == GarnetStatus.OK)
            {
                while (!RespWriteUtils.WriteSimpleString(typeName, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.WriteSimpleString("none"u8, ref dcurr, dend))
                    SendAndReset();
            }

            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        private bool NetworkMODULE<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi, CustomCommandManager customCommandManager)
            where TGarnetApi : IGarnetApi
        {
            if (count < 1) // At least one subcommand is required
                return AbortWithWrongNumberOfArguments("MODULE", count);

            // Read sub-command
            if (!RespReadUtils.TrySliceWithLengthHeader(out var subCommand, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (subCommand.SequenceEqual(CmdStrings.LOAD) || subCommand.SequenceEqual(CmdStrings.load))
            {
                if (count < 2) // At least module path is required
                    return AbortWithWrongNumberOfArguments("MODULE LOAD", count);

                // Read path to module file
                if (!RespReadUtils.TrySliceWithLengthHeader(out var modulePath, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                // Read module args
                var leftTokens = count - 2;
                List<string> moduleArgs = [];
                while (leftTokens > 0)
                {
                    if (!RespReadUtils.ReadStringWithLengthHeader(out var arg, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                    leftTokens--;

                    moduleArgs.Add(arg);
                }

                if (LoadAssemblies([Encoding.ASCII.GetString(modulePath)], out var loadedAssemblies, out var errorMsg))
                {
                    Debug.Assert(loadedAssemblies != null && loadedAssemblies.Length == 1, "Only one assembly per module load");
                    var loadedAssembly = loadedAssemblies[0];
                    if (ModuleRegistrar.Instance.LoadModule(customCommandManager, loadedAssembly, moduleArgs, logger, out errorMsg))
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
            else
            {
                // TODO: pending implementation for other module commands support.
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_UNK_CMD, ref dcurr, dend))
                    SendAndReset();
            }

            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary>
        /// Writes the keys in the cursor to the output
        /// </summary>
        /// <param name="cursorValue">Cursor value to write to the output</param>
        /// <param name="keys">Keys to write to the output</param>
        /// <param name="curr">Pointer to output buffer</param>
        /// <param name="end">End of the output buffer</param>
        private void WriteOutputForScan(long cursorValue, List<byte[]> keys, ref byte* curr, byte* end)
        {
            // The output is an array of two elements: cursor value and an array of keys
            // Note the cursor value should be formatted as a simple string ('+')
            while (!RespWriteUtils.WriteLongAsSimpleString(cursorValue, ref curr, end))
                SendAndReset();

            if (keys.Count == 0)
            {
                // Cursor value
                while (!RespWriteUtils.WriteLongAsSimpleString(0, ref curr, end))
                    SendAndReset();

                // Empty array
                while (!RespWriteUtils.WriteEmptyArray(ref curr, end))
                    SendAndReset();

                return;
            }

            // Write size of the array
            while (!RespWriteUtils.WriteArrayLength(keys.Count, ref curr, end))
                SendAndReset();

            // Write the keys matching the pattern
            for (int i = 0; i < keys.Count; i++)
            {
                while (!RespWriteUtils.WriteBulkString(keys[i], ref curr, end))
                    SendAndReset();
            }
        }
    }
}