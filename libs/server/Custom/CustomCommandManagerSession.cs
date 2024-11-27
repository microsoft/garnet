// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// Server session for RESP protocol - basic commands are in this file
    /// </summary>
    internal sealed class CustomCommandManagerSession
    {
        readonly CustomCommandManager customCommandManager;

        // These session specific arrays are indexed by the same ID as the arrays in CustomCommandManager
        readonly ExtensibleMap<CustomTransactionProcedureWithArity> sessionTransactionProcMap;
        readonly ExtensibleMap<CustomProcedure> sessionCustomProcMap;

        public CustomCommandManagerSession(CustomCommandManager customCommandManager)
        {
            this.customCommandManager = customCommandManager;
            sessionTransactionProcMap = new ExtensibleMap<CustomTransactionProcedureWithArity>(
                CustomCommandManager.MinMapSize, byte.MaxValue, 0);
            sessionCustomProcMap = new ExtensibleMap<CustomProcedure>(CustomCommandManager.MinMapSize,
                byte.MaxValue, 0);
        }

        public CustomProcedure GetCustomProcedure(int id, RespServerSession respServerSession)
        {
            var index = sessionCustomProcMap.GetIndexFromId(id);

            if (sessionCustomProcMap[index] == null)
            {
                var entry = customCommandManager.GetCustomProcedure(id) ?? throw new GarnetException($"Custom procedure {id} not found");
                sessionCustomProcMap[index] = entry.CustomProcedureFactory();
                sessionCustomProcMap[index].respServerSession = respServerSession;
            }

            return sessionCustomProcMap[index];
        }

        public CustomTransactionProcedure GetCustomTransactionProcedure(int id, RespServerSession respServerSession, TransactionManager txnManager, ScratchBufferManager scratchBufferManager, out int arity)
        {
            var index = sessionCustomProcMap.GetIndexFromId(id);

            if (sessionTransactionProcMap[index]?.Procedure == null)
            {
                var entry = customCommandManager.GetCustomTransactionProcedure(id) ?? throw new GarnetException($"Transaction procedure {id} not found");
                _ = customCommandManager.CustomCommandsInfo.TryGetValue(entry.NameStr, out var cmdInfo);
                arity = cmdInfo?.Arity ?? 0;
                return GetCustomTransactionProcedureAndSetArity(entry, respServerSession, txnManager, scratchBufferManager, cmdInfo?.Arity ?? 0);
            }

            arity = sessionTransactionProcMap[index].Arity;
            return sessionTransactionProcMap[index].Procedure;
        }

        private CustomTransactionProcedure GetCustomTransactionProcedureAndSetArity(CustomTransaction entry, RespServerSession respServerSession, TransactionManager txnManager, ScratchBufferManager scratchBufferManager, int arity)
        {
            int id = entry.id;
            var index = sessionCustomProcMap.GetIndexFromId(id);

            sessionTransactionProcMap[index] = new CustomTransactionProcedureWithArity(entry.proc(), arity)
            {
                Procedure =
                {
                    txnManager = txnManager,
                    scratchBufferManager = scratchBufferManager,
                    respServerSession = respServerSession
                }
            };

            return sessionTransactionProcMap[index].Procedure;
        }

        private class CustomTransactionProcedureWithArity
        {
            public CustomTransactionProcedure Procedure { get; set; }

            public int Arity { get; set; }

            public CustomTransactionProcedureWithArity(CustomTransactionProcedure procedure, int arity)
            {
                this.Procedure = procedure;
                this.Arity = arity;
            }
        }
    }
}