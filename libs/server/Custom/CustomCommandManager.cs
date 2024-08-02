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
        internal const byte StartOffset = 200;
        internal const int MaxRegistrations = byte.MaxValue - StartOffset;

        internal readonly CustomRawStringCommand[] rawStringCommandMap;
        internal readonly CustomObjectCommandWrapper[] objectCommandMap;
        internal readonly CustomTransaction[] transactionProcMap;
        internal readonly CustomProcedureWrapper[] customProcedureMap;
        internal int RawStringCommandId = 0;
        internal int ObjectTypeId = 0;
        internal int TransactionProcId = 0;
        internal int CustomProcedureId = 0;

        internal int CustomCommandsInfoCount => this.customCommandsInfo.Count;
        internal IEnumerable<RespCommandsInfo> CustomCommandsInfo => this.customCommandsInfo.Values;

        private readonly Dictionary<string, RespCommandsInfo> customCommandsInfo = new(StringComparer.OrdinalIgnoreCase);

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

        internal int Register(string name, int numParams, CommandType type, CustomRawStringFunctions customFunctions, RespCommandsInfo commandInfo, long expirationTicks)
        {
            int id = Interlocked.Increment(ref RawStringCommandId) - 1;
            if (id >= MaxRegistrations)
                throw new Exception("Out of registration space");

            rawStringCommandMap[id] = new CustomRawStringCommand(name, (byte)id, 1, numParams, type, customFunctions, expirationTicks);
            if (commandInfo != null) customCommandsInfo.Add(name, commandInfo);
            return id;
        }

        internal int Register(string name, int numParams, Func<CustomTransactionProcedure> proc, RespCommandsInfo commandInfo = null)
        {
            int id = Interlocked.Increment(ref TransactionProcId) - 1;
            if (id >= MaxRegistrations)
                throw new Exception("Out of registration space");

            transactionProcMap[id] = new CustomTransaction(name, (byte)id, numParams, proc);
            if (commandInfo != null) customCommandsInfo.Add(name, commandInfo);
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

        internal int Register(string name, int numParams, CommandType commandType, int objectTypeId, RespCommandsInfo commandInfo)
        {
            var wrapper = objectCommandMap[objectTypeId];

            int subCommand = Interlocked.Increment(ref wrapper.CommandId) - 1;
            if (subCommand >= byte.MaxValue)
                throw new Exception("Out of registration space");

            wrapper.commandMap[subCommand] = new CustomObjectCommand(name, (byte)objectTypeId, (byte)subCommand, 1, numParams, commandType, wrapper.factory);
            if (commandInfo != null) customCommandsInfo.Add(name, commandInfo);

            return subCommand;
        }

        internal (int objectTypeId, int subCommand) Register(string name, int numParams, CommandType commandType, CustomObjectFactory factory, RespCommandsInfo commandInfo)
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
            wrapper.commandMap[subCommand] = new CustomObjectCommand(name, (byte)objectTypeId, (byte)subCommand, 1, numParams, commandType, wrapper.factory);

            if (commandInfo != null) customCommandsInfo.Add(name, commandInfo);

            return (objectTypeId, subCommand);
        }

        internal (int objectTypeId, int subCommand) Register(string name, int numParams, CommandType commandType, CustomObjectFactory factory, CustomObjectFunctions customObjectFunctions, RespCommandsInfo commandInfo)
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
            wrapper.commandMap[subCommand] = new CustomObjectCommand(name, (byte)objectTypeId, (byte)subCommand, 1, numParams, commandType, wrapper.factory, customObjectFunctions);

            if (commandInfo != null) customCommandsInfo.Add(name, commandInfo);

            return (objectTypeId, subCommand);
        }

        /// <summary>
        /// Register custom command
        /// </summary>
        /// <param name="name"></param>
        /// <param name="customProcedure"></param>
        /// <param name="commandInfo"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        internal int Register(string name, CustomProcedure customProcedure, RespCommandsInfo commandInfo = null)
        {
            int id = Interlocked.Increment(ref CustomProcedureId) - 1;
            if (id >= MaxRegistrations)
                throw new Exception("Out of registration space");

            customProcedureMap[id] = new CustomProcedureWrapper(name, (byte)id, customProcedure);
            if (commandInfo != null) customCommandsInfo.Add(name, commandInfo);
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
            respCommandsInfo = default;
            if (!this.customCommandsInfo.ContainsKey(cmdName)) return false;

            respCommandsInfo = this.customCommandsInfo[cmdName];
            return true;
        }
    }
}