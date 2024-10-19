// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;

namespace Garnet.server
{
    /// <summary>
    /// Custom command manager
    /// </summary>
    public class CustomCommandManager
    {
        internal static readonly ushort StartOffset = (ushort)(RespCommandExtensions.LastValidCommand + 1);
        internal static readonly int MaxRegistrations = ushort.MaxValue - StartOffset;

        internal readonly CustomRawStringCommand[] rawStringCommandMap;
        internal readonly CustomObjectCommandWrapper[] objectCommandMap;
        internal readonly CustomTransaction[] transactionProcMap;
        internal readonly CustomProcedureWrapper[] customProcedureMap;
        internal int RawStringCommandId = 0;
        internal int ObjectTypeId = 0;
        internal int TransactionProcId = 0;
        internal int CustomProcedureId = 0;

        internal int CustomCommandsInfoCount => CustomCommandsInfo.Count;
        internal readonly Dictionary<string, RespCommandsInfo> CustomCommandsInfo = new(StringComparer.OrdinalIgnoreCase);
        internal readonly Dictionary<string, RespCommandDocs> CustomCommandsDocs = new(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Create new custom command manager
        /// </summary>
        public CustomCommandManager()
        {
            rawStringCommandMap = new CustomRawStringCommand[MaxRegistrations];
            objectCommandMap = new CustomObjectCommandWrapper[MaxRegistrations];
            transactionProcMap = new CustomTransaction[MaxRegistrations]; // can increase up to byte.MaxValue
            customProcedureMap = new CustomProcedureWrapper[MaxRegistrations];
        }

        internal int Register(string name, CommandType type, CustomRawStringFunctions customFunctions, RespCommandsInfo commandInfo, RespCommandDocs commandDocs, long expirationTicks)
        {
            int id = Interlocked.Increment(ref RawStringCommandId) - 1;
            if (id >= MaxRegistrations)
                throw new Exception("Out of registration space");

            rawStringCommandMap[id] = new CustomRawStringCommand(name, (ushort)id, type, customFunctions, expirationTicks);
            if (commandInfo != null) CustomCommandsInfo.Add(name, commandInfo);
            if (commandDocs != null) CustomCommandsDocs.Add(name, commandDocs);
            return id;
        }

        internal int Register(string name, Func<CustomTransactionProcedure> proc, RespCommandsInfo commandInfo = null, RespCommandDocs commandDocs = null)
        {
            int id = Interlocked.Increment(ref TransactionProcId) - 1;
            if (id >= MaxRegistrations)
                throw new Exception("Out of registration space");

            transactionProcMap[id] = new CustomTransaction(name, (byte)id, proc);
            if (commandInfo != null) CustomCommandsInfo.Add(name, commandInfo);
            if (commandDocs != null) CustomCommandsDocs.Add(name, commandDocs);
            return id;
        }

        internal int RegisterType(CustomObjectFactory factory)
        {
            for (int i = 0; i < ObjectTypeId; i++)
                if (objectCommandMap[i].factory == factory)
                    throw new Exception($"Type already registered with ID {i}");

            int type;
            do
            {
                type = Interlocked.Increment(ref ObjectTypeId) - 1;
                if (type >= MaxRegistrations)
                    throw new Exception("Out of registration space");
            } while (objectCommandMap[type] != null);

            objectCommandMap[type] = new CustomObjectCommandWrapper((byte)type, factory);

            return type;
        }

        internal void RegisterType(int objectTypeId, CustomObjectFactory factory)
        {
            if (objectTypeId >= MaxRegistrations)
                throw new Exception("Type is outside registration space");

            if (ObjectTypeId <= objectTypeId) ObjectTypeId = objectTypeId + 1;
            for (int i = 0; i < ObjectTypeId; i++)
                if (objectCommandMap[i].factory == factory)
                    throw new Exception($"Type already registered with ID {i}");

            objectCommandMap[objectTypeId] = new CustomObjectCommandWrapper((byte)objectTypeId, factory);
        }

        internal (int objectTypeId, int subCommand) Register(string name, CommandType commandType, CustomObjectFactory factory, RespCommandsInfo commandInfo, RespCommandDocs commandDocs)
        {
            int objectTypeId = -1;
            for (int i = 0; i < ObjectTypeId; i++)
            {
                if (objectCommandMap[i].factory == factory) { objectTypeId = i; break; }
            }

            if (objectTypeId == -1)
            {
                objectTypeId = Interlocked.Increment(ref ObjectTypeId) - 1;
                if (objectTypeId >= MaxRegistrations)
                    throw new Exception("Out of registration space");
                objectCommandMap[objectTypeId] = new CustomObjectCommandWrapper((byte)objectTypeId, factory);
            }

            var wrapper = objectCommandMap[objectTypeId];

            int subCommand = Interlocked.Increment(ref wrapper.CommandId) - 1;
            if (subCommand >= byte.MaxValue)
                throw new Exception("Out of registration space");
            wrapper.commandMap[subCommand] = new CustomObjectCommand(name, (byte)objectTypeId, (byte)subCommand, commandType, wrapper.factory);

            if (commandInfo != null) CustomCommandsInfo.Add(name, commandInfo);
            if (commandDocs != null) CustomCommandsDocs.Add(name, commandDocs);

            return (objectTypeId, subCommand);
        }

        internal (int objectTypeId, int subCommand) Register(string name, CommandType commandType, CustomObjectFactory factory, CustomObjectFunctions customObjectFunctions, RespCommandsInfo commandInfo, RespCommandDocs commandDocs)
        {
            var objectTypeId = -1;
            for (var i = 0; i < ObjectTypeId; i++)
            {
                if (objectCommandMap[i].factory == factory) { objectTypeId = i; break; }
            }

            if (objectTypeId == -1)
            {
                objectTypeId = Interlocked.Increment(ref ObjectTypeId) - 1;
                if (objectTypeId >= MaxRegistrations)
                    throw new Exception("Out of registration space");
                objectCommandMap[objectTypeId] = new CustomObjectCommandWrapper((byte)objectTypeId, factory);
            }

            var wrapper = objectCommandMap[objectTypeId];

            int subCommand = Interlocked.Increment(ref wrapper.CommandId) - 1;
            if (subCommand >= byte.MaxValue)
                throw new Exception("Out of registration space");
            wrapper.commandMap[subCommand] = new CustomObjectCommand(name, (byte)objectTypeId, (byte)subCommand, commandType, wrapper.factory, customObjectFunctions);

            if (commandInfo != null) CustomCommandsInfo.Add(name, commandInfo);
            if (commandDocs != null) CustomCommandsDocs.Add(name, commandDocs);

            return (objectTypeId, subCommand);
        }

        /// <summary>
        /// Register custom command
        /// </summary>
        /// <param name="name"></param>
        /// <param name="customProcedure"></param>
        /// <param name="commandInfo"></param>
        /// <param name="commandDocs"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        internal int Register(string name, CustomProcedure customProcedure, RespCommandsInfo commandInfo = null, RespCommandDocs commandDocs = null)
        {
            int id = Interlocked.Increment(ref CustomProcedureId) - 1;
            if (id >= MaxRegistrations)
                throw new Exception("Out of registration space");

            customProcedureMap[id] = new CustomProcedureWrapper(name, (byte)id, customProcedure);
            if (commandInfo != null) CustomCommandsInfo.Add(name, commandInfo);
            if (commandDocs != null) CustomCommandsDocs.Add(name, commandDocs);
            return id;
        }

        internal bool Match(ReadOnlySpan<byte> command, out CustomRawStringCommand cmd)
        {
            for (int i = 0; i < RawStringCommandId; i++)
            {
                cmd = rawStringCommandMap[i];
                if (cmd != null && command.SequenceEqual(new ReadOnlySpan<byte>(cmd.name)))
                    return true;
            }
            cmd = null;
            return false;
        }

        internal bool Match(ReadOnlySpan<byte> command, out CustomTransaction cmd)
        {
            for (int i = 0; i < TransactionProcId; i++)
            {
                cmd = transactionProcMap[i];
                if (cmd != null && command.SequenceEqual(new ReadOnlySpan<byte>(cmd.name)))
                    return true;
            }
            cmd = null;
            return false;
        }

        internal bool Match(ReadOnlySpan<byte> command, out CustomObjectCommand cmd)
        {
            for (int i = 0; i < ObjectTypeId; i++)
            {
                var wrapper = objectCommandMap[i];
                if (wrapper != null)
                {
                    for (int j = 0; j < wrapper.CommandId; j++)
                    {
                        cmd = wrapper.commandMap[j];
                        if (cmd != null && command.SequenceEqual(new ReadOnlySpan<byte>(cmd.name)))
                            return true;
                    }
                }
                else break;
            }
            cmd = null;
            return false;
        }

        internal bool Match(ReadOnlySpan<byte> command, out CustomProcedureWrapper cmd)
        {
            for (int i = 0; i < CustomProcedureId; i++)
            {
                cmd = customProcedureMap[i];
                if (cmd != null && command.SequenceEqual(new ReadOnlySpan<byte>(cmd.Name)))
                    return true;
            }
            cmd = null;
            return false;
        }

        internal bool TryGetCustomCommandInfo(string cmdName, out RespCommandsInfo respCommandsInfo)
        {
            return this.CustomCommandsInfo.TryGetValue(cmdName, out respCommandsInfo);
        }

        internal bool TryGetCustomCommandDocs(string cmdName, out RespCommandDocs respCommandsDocs)
        {
            return this.CustomCommandsDocs.TryGetValue(cmdName, out respCommandsDocs);
        }
    }
}