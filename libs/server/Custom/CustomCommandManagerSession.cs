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
        public readonly (CustomTransactionProcedure, int)[] sessionTransactionProcMap;

        public CustomCommandManagerSession(CustomCommandManager customCommandManager)
        {
            this.customCommandManager = customCommandManager;
            sessionTransactionProcMap = new (CustomTransactionProcedure, int)[CustomCommandManager.MaxRegistrations];
        }

        public (CustomTransactionProcedure, int) GetCustomTransactionProcedure(int id, TransactionManager txnManager, ScratchBufferManager scratchBufferManager)
        {
            if (sessionTransactionProcMap[id].Item1 == null)
            {
                var entry = customCommandManager.transactionProcMap[id] ?? throw new GarnetException($"Transaction procedure {id} not found");
                return GetCustomTransactionProcedure(entry, txnManager, scratchBufferManager);
            }
            return sessionTransactionProcMap[id];
        }

        public (CustomTransactionProcedure, int) GetCustomTransactionProcedure(CustomTransaction entry, TransactionManager txnManager, ScratchBufferManager scratchBufferManager)
        {
            int id = entry.id;
            if (sessionTransactionProcMap[id].Item1 == null)
            {
                sessionTransactionProcMap[id].Item1 = entry.proc();
                sessionTransactionProcMap[id].Item2 = entry.NumParams;

                sessionTransactionProcMap[id].Item1.txnManager = txnManager;
                sessionTransactionProcMap[id].Item1.scratchBufferManager = scratchBufferManager;
            }
            return sessionTransactionProcMap[id];
        }
    }
}