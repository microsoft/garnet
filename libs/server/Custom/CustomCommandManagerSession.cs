// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

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
        readonly (CustomTransactionProcedure, int)[] sessionTransactionProcMap;
        readonly CustomProcedure[] sessionCustomProcMap;


        public CustomCommandManagerSession(CustomCommandManager customCommandManager)
        {
            this.customCommandManager = customCommandManager;
            sessionTransactionProcMap = new (CustomTransactionProcedure, int)[CustomCommandManager.MaxRegistrations];
            sessionCustomProcMap = new CustomProcedure[CustomCommandManager.MaxRegistrations];
        }

        public CustomProcedure GetCustomProcedure(int id, RespServerSession respServerSession)
        {
            if (sessionCustomProcMap[id] == null)
            {
                var entry = customCommandManager.customProcedureMap[id] ?? throw new GarnetException($"Custom procedure {id} not found");
                sessionCustomProcMap[id] = entry.CustomProcedureFactory();
                sessionCustomProcMap[id].respServerSession = respServerSession;
            }

            return sessionCustomProcMap[id];
        }

        public (CustomTransactionProcedure, int) GetCustomTransactionProcedure(int id, RespServerSession respServerSession, TransactionManager txnManager, ScratchBufferManager scratchBufferManager)
        {
            if (sessionTransactionProcMap[id].Item1 == null)
            {
                var entry = customCommandManager.transactionProcMap[id] ?? throw new GarnetException($"Transaction procedure {id} not found");
                _ = customCommandManager.CustomCommandsInfo.TryGetValue(entry.NameStr, out var cmdInfo);
                return GetCustomTransactionProcedure(entry, respServerSession, txnManager, scratchBufferManager, cmdInfo?.Arity ?? 0);
            }
            return sessionTransactionProcMap[id];
        }

        public (CustomTransactionProcedure, int) GetCustomTransactionProcedure(CustomTransaction entry, RespServerSession respServerSession, TransactionManager txnManager, ScratchBufferManager scratchBufferManager, int arity)
        {
            int id = entry.id;
            if (sessionTransactionProcMap[id].Item1 == null)
            {
                sessionTransactionProcMap[id].Item1 = entry.proc();
                sessionTransactionProcMap[id].Item2 = arity;

                sessionTransactionProcMap[id].Item1.txnManager = txnManager;
                sessionTransactionProcMap[id].Item1.scratchBufferManager = scratchBufferManager;
                sessionTransactionProcMap[id].Item1.respServerSession = respServerSession;
            }
            return sessionTransactionProcMap[id];
        }
    }
}