// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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

        internal readonly CustomCommand[] commandMap;
        internal readonly CustomObjectCommandWrapper[] objectCommandMap;
        internal readonly CustomTransaction[] transactionProcMap;
        internal int CommandId = 0;
        internal int ObjectTypeId = 0;
        internal int TransactionProcId = 0;

        /// <summary>
        /// Create new custom command manager
        /// </summary>
        public CustomCommandManager()
        {
            commandMap = new CustomCommand[MaxRegistrations];
            objectCommandMap = new CustomObjectCommandWrapper[MaxRegistrations];
            transactionProcMap = new CustomTransaction[MaxRegistrations]; // can increase up to byte.MaxValue
        }

        internal int Register(string name, int numParams, CommandType type, CustomRawStringFunctions customFunctions, long expirationTicks)
        {
            int id = Interlocked.Increment(ref CommandId) - 1;
            if (id >= MaxRegistrations)
                throw new Exception("Out of registration space");

            commandMap[id] = new CustomCommand(name, (byte)id, 1, numParams, type, customFunctions, expirationTicks);
            return id;
        }

        internal int Register(string name, int numParams, Func<CustomTransactionProcedure> proc)
        {
            int id = Interlocked.Increment(ref TransactionProcId) - 1;
            if (id >= MaxRegistrations)
                throw new Exception("Out of registration space");

            transactionProcMap[id] = new CustomTransaction(name, (byte)id, numParams, proc);
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

        internal int Register(string name, int numParams, CommandType commandType, int objectTypeId)
        {
            var wrapper = objectCommandMap[objectTypeId];

            int subCommand = Interlocked.Increment(ref wrapper.CommandId) - 1;
            if (subCommand >= byte.MaxValue)
                throw new Exception("Out of registration space");
            wrapper.commandMap[subCommand] = new CustomObjectCommand(name, (byte)objectTypeId, (byte)subCommand, 1, numParams, commandType, wrapper.factory);

            return subCommand;
        }

        internal (int objectTypeId, int subCommand) Register(string name, int numParams, CommandType commandType, CustomObjectFactory factory)
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

            return (objectTypeId, subCommand);
        }

        internal bool Match(ReadOnlySpan<byte> command, out CustomCommand cmd)
        {
            for (int i = 0; i < CommandId; i++)
            {
                cmd = commandMap[i];
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
    }
}