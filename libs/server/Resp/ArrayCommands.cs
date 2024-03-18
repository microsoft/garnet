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
using Garnet.server.Custom;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Server session for RESP protocol - array commands are in this file
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        int opsDone = 0;
        int keysDeleted = 0;

        /// <summary>
        /// MGET
        /// </summary>
        private bool NetworkMGET<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (storeWrapper.serverOptions.EnableScatterGatherGet)
                return NetworkMGET_SG(count, ptr, ref storageApi);

            ptr += 10;
            SpanByte input = default;

            if (NetworkArraySlotVerify(count - 1, ptr, interleavedKeys: false, readOnly: true, out bool retVal))
                return retVal;

            for (int c = 0; c < count - 1; c++)
            {
                byte* keyPtr = null;
                int ksize = 0;

                if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                keyPtr -= sizeof(int);

                if (c < opsDone)
                    continue;

                if (opsDone == 0)
                {
                    while (!RespWriteUtils.WriteArrayLength(count - 1, ref dcurr, dend))
                        SendAndReset();
                }
                opsDone++;

                int save = *(int*)keyPtr;
                *(int*)keyPtr = ksize;

                var o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
                var status = storageApi.GET(ref Unsafe.AsRef<SpanByte>(keyPtr), ref input, ref o);

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
                        while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                            SendAndReset();
                        break;
                }

                *(int*)keyPtr = save;
            }

            opsDone = 0;
            readHead = (int)(ptr - recvBufferPtr);

            return true;
        }

        /// <summary>
        /// MGET - scatter gather version
        /// </summary>
        private bool NetworkMGET_SG<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetAdvancedApi
        {
            ptr += 10;
            SpanByte input = default;
            long ctx = default;

            if (NetworkArraySlotVerify(count - 1, ptr, interleavedKeys: false, readOnly: true, out bool retVal))
            {
                return retVal;
            }

            int firstPending = -1;
            (GarnetStatus, SpanByteAndMemory)[] outputArr = null;
            SpanByteAndMemory o = new(dcurr, (int)(dend - dcurr));

            for (int c = 0; c < count - 1; c++)
            {
                byte* keyPtr = null;
                int ksize = 0;

                if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                    break;

                if (c < opsDone)
                    continue;

                if (opsDone == 0)
                {
                    while (!RespWriteUtils.WriteArrayLength(count - 1, ref dcurr, dend))
                        SendAndReset();
                    o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
                }
                opsDone++;

                // Store length header for the key
                keyPtr -= sizeof(int);
                int save = *(int*)keyPtr;
                *(int*)keyPtr = ksize;

                // Store index in context, since completions are not in order
                ctx = c;

                var status = storageApi.GET_WithPending(ref Unsafe.AsRef<SpanByte>(keyPtr), ref input, ref o, ctx, out bool isPending);

                *(int*)keyPtr = save;

                if (isPending)
                {
                    if (firstPending == -1)
                    {
                        outputArr = new (GarnetStatus, SpanByteAndMemory)[count - 1];
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
                            while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
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
                for (int i = firstPending; i < opsDone; i++)
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
                        while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                            SendAndReset();
                    }
                }
            }

            if (opsDone < count - 1)
                return false;

            opsDone = 0;
            readHead = (int)(ptr - recvBufferPtr);

            return true;
        }

        /// <summary>
        /// Register all custom commands / transactions
        /// </summary>
        /// <param name="binaryPaths">Binary paths from which to load assemblies</param>
        /// <param name="classNameToRegisterArgs">Mapping between class names to register and arguments required for registration</param>
        /// <param name="customCommandManager">CustomCommandManager instance used to register commands</param>
        /// <returns>Response</returns>
        private ReadOnlySpan<byte> RegisterCustomCommands(IEnumerable<string> binaryPaths, Dictionary<string, List<RegisterArgsBase>> classNameToRegisterArgs, CustomCommandManager customCommandManager)
        {
            var classInstances = new Dictionary<string, object>();
            // Get all binary file paths from inputs binary paths
            if (!FileUtils.TryGetFiles(binaryPaths, out var files, out _, new[] { ".dll", ".exe" },
                    SearchOption.AllDirectories)) return CmdStrings.RESP_ERROR_GETTING_BINARY_FILES;

            // Check that all binary files are contained in allowed binary paths
            var binaryFiles = files.ToArray();
            if (binaryFiles.Any(f =>
                    storeWrapper.serverOptions.ExtensionBinPaths.All(p => !FileUtils.IsFileInDirectory(f, p))))
            {
                return CmdStrings.RESP_ERROR_BINARY_FILES_NOT_IN_ALLOWED_PATHS;
            }

            // Get all assemblies from binary files
            if (!FileUtils.TryLoadAssemblies(binaryFiles, out var assemblies, out _))
                return CmdStrings.RESP_ERROR_LOADING_ASSEMBLIES;

            var loadedAssemblies = assemblies.ToArray();

            // If necessary, check that all assemblies are digitally signed
            if (!storeWrapper.serverOptions.ExtensionAllowUnsignedAssemblies)
            {
                foreach (var loadedAssembly in loadedAssemblies)
                {
                    var publicKey = loadedAssembly.GetName().GetPublicKey();
                    if (publicKey == null || publicKey.Length == 0)
                        return CmdStrings.RESP_ERROR_ASSEMBLY_NOT_SIGNED;
                }
            }

            foreach (var c in classNameToRegisterArgs.Keys)
            {
                if (classInstances.ContainsKey(c)) continue;
                classInstances.Add(c, null);
            }

            // Get types from loaded assemblies
            var loadedTypes = loadedAssemblies.SelectMany(a => a.GetTypes()).Where(t =>
                classInstances.ContainsKey(t.Name)).ToArray();

            // Check that all types implement one of the supported custom command base classes
            var supportedCustomCommandTypes = RegisterCustomCommandProviderBase.SupportedCustomCommandBaseTypesLazy.Value;
            if (loadedTypes.Any(t => !supportedCustomCommandTypes.Any(st => st.IsAssignableFrom(t))))
            {
                return CmdStrings.RESP_ERROR_REGISTERCS_UNSUPPORTED_CLASS;
            }

            // Check that all types have empty constructors
            if (loadedTypes.Any(t => t.GetConstructor(Type.EmptyTypes) == null))
            {
                return CmdStrings.RESP_ERROR_INSTANTIATING_CLASS;
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
                return CmdStrings.RESP_ERROR_INSTANTIATING_CLASS;
            }

            // Register each command / transaction using its specified class instance
            var registerApis = new List<IRegisterCustomCommandProvider>();
            foreach (var classNameToArgs in classNameToRegisterArgs)
            {
                foreach (var args in classNameToArgs.Value)
                {
                    var registerApi =
                        RegisterCustomCommandProviderFactory.GetRegisterCustomCommandProvider(classInstances[classNameToArgs.Key], args);

                    if (registerApi == null)
                        return CmdStrings.RESP_ERROR_REGISTERCS_UNSUPPORTED_CLASS;

                    registerApis.Add(registerApi);
                }
            }

            foreach (var registerApi in registerApis)
            {
                registerApi.Register(customCommandManager);
            }

            return CmdStrings.RESP_OK;

            // If any assembly was not loaded correctly, return an error

            // If any directory was not enumerated correctly, return an error
        }

        /// <summary>
        /// REGISTERCS - Registers one or more custom commands / transactions
        /// </summary>
        private bool NetworkREGISTERCS(int count, byte* ptr, CustomCommandManager customCommandManager)
        {
            var leftTokens = count;
            var readPathsOnly = false;

            var binaryPaths = new HashSet<string>();

            // Custom class name to arguments read from each sub-command
            var classNameToRegisterArgs = new Dictionary<string, List<RegisterArgsBase>>();

            ReadOnlySpan<byte> response = null;

            if (leftTokens == 0)
                response = CmdStrings.RESP_MALFORMED_REGISTERCS_COMMAND;

            // Parse the REGISTERCS command - list of registration sub-commands followed by a list of paths to binary files / folders
            // Syntax - REGISTERCS cmdType name numParams className [expTicks] [cmdType name numParams className [expTicks] ...] SRC path [path ...]
            RegisterArgsBase args = null;

            while (leftTokens > 0)
            {
                byte* firstTokenPtr = null;
                int firstTokenSize = 0;

                // Read first token of current sub-command or path
                if (!RespReadUtils.ReadPtrWithLengthHeader(ref firstTokenPtr, ref firstTokenSize, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                leftTokens--;

                // Check if first token defines the start of a new sub-command (cmdType) or a path
                var tokenSpan = new ReadOnlySpan<byte>(firstTokenPtr, firstTokenSize);
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
                else if (readPathsOnly || (tokenSpan.SequenceEqual(CmdStrings.SRC) ||
                                           tokenSpan.SequenceEqual(CmdStrings.src)))
                {
                    // If first token is not a cmdType and no other sub-command is previously defined, command is malformed
                    if (classNameToRegisterArgs.Count == 0)
                    {
                        response = CmdStrings.RESP_MALFORMED_REGISTERCS_COMMAND;
                        break;
                    }

                    // Read only binary paths from this point forth
                    if (readPathsOnly)
                    {
                        var path = Encoding.ASCII.GetString(firstTokenPtr, firstTokenSize);
                        binaryPaths.Add(path);
                    }

                    readPathsOnly = true;

                    continue;
                }
                else
                {
                    // Check optional parameters for previous sub-command
                    if (args is RegisterCmdArgs cmdArgs)
                    {
                        var expTicks = NumUtils.BytesToLong(tokenSpan);
                        cmdArgs.ExpirationTicks = expTicks;
                        continue;
                    }

                    // Unexpected token
                    response = CmdStrings.RESP_MALFORMED_REGISTERCS_COMMAND;
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
                byte* firstTokenPtr = null;
                int firstTokenSize = 0;

                if (!RespReadUtils.ReadPtrWithLengthHeader(ref firstTokenPtr, ref firstTokenSize, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                leftTokens--;
            }

            // If no error is found, continue to register commands in the server
            if (response == null)
            {
                response = this.RegisterCustomCommands(binaryPaths, classNameToRegisterArgs, customCommandManager);
            }

            while (!RespWriteUtils.WriteResponse(response, ref dcurr, dend))
                SendAndReset();

            readHead = (int)(ptr - recvBufferPtr);

            return true;
        }

        /// <summary>
        /// MSET
        /// </summary>
        private bool NetworkMSET<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            ptr += 10;

            if (NetworkArraySlotVerify((count - 1) / 2, ptr, interleavedKeys: true, readOnly: false, out bool retVal))
            {
                return retVal;
            }

            for (int c = 0; c < (count - 1) / 2; c++)
            {
                byte* keyPtr = null, valPtr = null;
                int ksize = 0, vsize = 0;

                if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                keyPtr -= sizeof(int);

                if (!RespReadUtils.ReadPtrWithLengthHeader(ref valPtr, ref vsize, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                valPtr -= sizeof(int);

                if (c < opsDone)
                    continue;

                opsDone++;

                int saveK = *(int*)keyPtr;
                *(int*)keyPtr = ksize;

                int saveV = *(int*)valPtr;
                *(int*)valPtr = vsize;

                var status = storageApi.SET(ref Unsafe.AsRef<SpanByte>(keyPtr), ref Unsafe.AsRef<SpanByte>(valPtr));
                Debug.Assert(status == GarnetStatus.OK);

                *(int*)keyPtr = saveK;
                *(int*)valPtr = saveV;

                readHead = (int)(ptr - recvBufferPtr);
            }
            opsDone = 0;
            while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        /// <summary>
        /// MSETNX
        /// </summary>
        private bool NetworkMSETNX<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            ptr += 12;

            if (NetworkArraySlotVerify((count - 1) / 2, ptr, interleavedKeys: true, readOnly: false, out bool retVal))
            {
                return retVal;
            }

            byte* hPtr = stackalloc byte[RespInputHeader.Size];

            bool anyValuesSet = false;
            for (int c = 0; c < (count - 1) / 2; c++)
            {
                byte* keyPtr = null, valPtr = null;
                int ksize = 0, vsize = 0;

                if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                keyPtr -= sizeof(int);

                if (!RespReadUtils.ReadPtrWithLengthHeader(ref valPtr, ref vsize, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                valPtr -= sizeof(int);

                if (c < opsDone)
                    continue;

                opsDone++;

                int saveK = *(int*)keyPtr;
                *(int*)keyPtr = ksize;

                int saveV = *(int*)valPtr;
                *(int*)valPtr = vsize;

                // Make space for RespCommand in input
                valPtr -= RespInputHeader.Size;

                // Save state of memory to override with header
                Buffer.MemoryCopy(valPtr, hPtr, RespInputHeader.Size, RespInputHeader.Size);

                *(int*)valPtr = RespInputHeader.Size + vsize;
                ((RespInputHeader*)(valPtr + sizeof(int)))->cmd = RespCommand.SETEXNX;
                ((RespInputHeader*)(valPtr + sizeof(int)))->flags = 0;

                var status = storageApi.SET_Conditional(ref Unsafe.AsRef<SpanByte>(keyPtr), ref Unsafe.AsRef<SpanByte>(valPtr));

                // Status tells us whether an old image was found during RMW or not
                // For a "set if not exists", NOTFOUND means that the operation succeeded
                if (status == GarnetStatus.NOTFOUND)
                    anyValuesSet = true;

                // Put things back in place (in case message is partitioned)
                *(int*)keyPtr = saveK;
                Buffer.MemoryCopy(hPtr, valPtr, RespInputHeader.Size, RespInputHeader.Size);
                valPtr += RespInputHeader.Size;
                *(int*)valPtr = saveV;

                readHead = (int)(ptr - recvBufferPtr);
            }

            opsDone = 0;

            while (!RespWriteUtils.WriteInteger(anyValuesSet ? 1 : 0, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        private bool NetworkDEL<TGarnetApi>(int count, byte* ptr, bool unlink, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            ptr += unlink ? 12 : 9;

            if (NetworkArraySlotVerify(count - 1, ptr, interleavedKeys: false, readOnly: false, out bool retVal))
            {
                return retVal;
            }

            for (int c = 0; c < count - 1; c++)
            {
                byte* keyPtr = null;
                int ksize = 0;

                if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                var key = new Span<byte>(keyPtr, ksize);
                keyPtr -= sizeof(int);

                if (c < opsDone)
                    continue;

                // Update processed count
                opsDone++;

                // Save ptr data
                int save = *(int*)keyPtr;
                *(int*)keyPtr = ksize;

                var status = storageApi.DELETE(ref Unsafe.AsRef<SpanByte>(keyPtr), StoreType.All);

                // Restore ptr data
                *(int*)keyPtr = save;

                // This is only an approximate count because the deletion of a key on disk is performed as a blind tombstone append
                if (status == GarnetStatus.OK)
                    keysDeleted++;
                readHead = (int)(ptr - recvBufferPtr);
            }

            while (!RespWriteUtils.WriteInteger(keysDeleted, ref dcurr, dend))
                SendAndReset();

            opsDone = 0;
            keysDeleted = 0;
            return true;
        }

        /// <summary>
        /// SELECT
        /// </summary>
        /// <param name="ptr"></param>
        /// <returns></returns>
        private bool NetworkSELECT(byte* ptr)
        {
            ptr += 12;

            // Read index
            if (!RespReadUtils.ReadStringWithLengthHeader(out var result, ref ptr, recvBufferPtr + bytesRead))
                return false;

            readHead = (int)(ptr - recvBufferPtr);

            if (string.Equals(result, "0"))
            {
                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.WriteResponse(Encoding.ASCII.GetBytes("-ERR invalid database index.\r\n"), ref dcurr, dend))
                    SendAndReset();
            }
            return true;
        }

        private bool NetworkDBSIZE<TGarnetApi>(byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            //$6\r\nDBSIZE\r\n
            ptr += 12;
            readHead = (int)(ptr - recvBufferPtr);
            while (!RespWriteUtils.WriteInteger(storageApi.GetDbSize(), ref dcurr, dend))
                SendAndReset();
            return true;
        }

        private bool NetworkKEYS<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            byte* pattern = null;
            int psize = 0;
            ptr += 10;

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
            ptr += 10;
            // Scan cursor [MATCH pattern] [COUNT count] [TYPE type]
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var cursorParameterByte, ref ptr, recvBufferPtr + bytesRead))
                return false;

            int leftTokens = count - 2;

            int psize = 1;
            byte* pattern = stackalloc byte[psize];
            *pattern = (byte)'*';
            var allKeys = true;

            long countValue = 10;

            int parameterLength = 0;
            byte* parameterWord = null;

            Span<byte> typeParameterValue = default;

            while (leftTokens > 0)
            {
                if (!RespReadUtils.ReadPtrWithLengthHeader(ref parameterWord, ref parameterLength, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                var parameterSB = new ReadOnlySpan<byte>(parameterWord, parameterLength);

                if (parameterSB.SequenceEqual(CmdStrings.MATCH) || parameterSB.SequenceEqual(CmdStrings.match))
                {
                    // Read pattern for keys filter
                    if (!RespReadUtils.ReadPtrWithLengthHeader(ref pattern, ref psize, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                    allKeys = psize == 1 && *pattern == '*';
                    leftTokens--;
                }
                else if (parameterSB.SequenceEqual(CmdStrings.COUNT) || parameterSB.SequenceEqual(CmdStrings.count))
                {
                    if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var countParameterValue, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                    long.TryParse(Encoding.ASCII.GetString(countParameterValue), out countValue);
                    leftTokens--;
                }
                else if (parameterSB.SequenceEqual(CmdStrings.TYPE) || parameterSB.SequenceEqual(CmdStrings.type))
                {
                    if (!RespReadUtils.ReadSpanByteWithLengthHeader(ref typeParameterValue, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                    leftTokens--;
                }
                leftTokens--;
            }

            long cursorFromInput = 0;
            long.TryParse(Encoding.ASCII.GetString(cursorParameterByte), out cursorFromInput);

            var patternAS = new ArgSlice(pattern, psize);

            long cursor = 0;
            storageApi.DbScan(patternAS, allKeys, cursorFromInput, out cursor, out var keys, typeParameterValue != default ? long.MaxValue : countValue, typeParameterValue);

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

        private bool NetworkTYPE<TGarnetApi>(byte* ptr, ref TGarnetApi storageApi)
              where TGarnetApi : IGarnetApi
        {
            ptr += 10;

            // TYPE key
            byte* keyPtr = null;
            int kSize = 0;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref kSize, ref ptr, recvBufferPtr + bytesRead))
                return false;

            var status = storageApi.GetKeyType(new(keyPtr, kSize), out string typeName);

            if (status == GarnetStatus.OK)
            {
                while (!RespWriteUtils.WriteSimpleString(Encoding.ASCII.GetBytes(typeName), ref dcurr, dend))
                    SendAndReset();
            }

            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        private bool NetworkMODULE<TGarnetApi>(byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            ptr += 12;

            // MODULE nameofmodule
            if (!RespReadUtils.ReadByteArrayWithLengthHeader(out var nameofmodule, ref ptr, recvBufferPtr + bytesRead))
                return false;

            // TODO: pending implementation for module support.
            while (!RespWriteUtils.WriteEmptyArray(ref dcurr, dend))
                SendAndReset();

            // Advance pointers
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