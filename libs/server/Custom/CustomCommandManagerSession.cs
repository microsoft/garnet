﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
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
        ExpandableMap<CustomTransactionProcedureWithArity> sessionTransactionProcMap;
        ExpandableMap<CustomProcedure> sessionCustomProcMap;

        public CustomCommandManagerSession(CustomCommandManager customCommandManager)
        {
            this.customCommandManager = customCommandManager;
            sessionTransactionProcMap = new ExpandableMap<CustomTransactionProcedureWithArity>(CustomCommandManager.MinMapSize, 0, byte.MaxValue);
            sessionCustomProcMap = new ExpandableMap<CustomProcedure>(CustomCommandManager.MinMapSize, 0, byte.MaxValue);
        }

        public CustomProcedure GetCustomProcedure(int id, RespServerSession respServerSession)
        {
            if (!sessionCustomProcMap.TryGetValue(id, out var customProc))
            {
                if (!customCommandManager.TryGetCustomProcedure(id, out var entry))
                    throw new GarnetException($"Custom procedure {id} not found");

                customProc = entry.CustomProcedureFactory();
                customProc.respServerSession = respServerSession;
                var setSuccessful = sessionCustomProcMap.TrySetValue(id, ref customProc);
                Debug.Assert(setSuccessful);
            }

            return customProc;
        }

        public CustomTransactionProcedure GetCustomTransactionProcedure(int id, RespServerSession respServerSession, TransactionManager txnManager, ScratchBufferManager scratchBufferManager, out int arity)
        {
            if (sessionTransactionProcMap.Exists(id))
            {
                ref var customTranProc = ref sessionTransactionProcMap.GetValueByRef(id);
                if (customTranProc.Procedure != null)
                {
                    arity = customTranProc.Arity;
                    return customTranProc.Procedure;
                }
            }

            if (!customCommandManager.TryGetCustomTransactionProcedure(id, out var entry))
                throw new GarnetException($"Transaction procedure {id} not found");
            _ = customCommandManager.customCommandsInfo.TryGetValue(entry.NameStr, out var cmdInfo);
            arity = cmdInfo?.Arity ?? 0;
            return GetCustomTransactionProcedureAndSetArity(entry, respServerSession, txnManager, scratchBufferManager, cmdInfo?.Arity ?? 0);
        }

        public RespCommand GetCustomRespCommand(int id)
            => customCommandManager.GetCustomRespCommand(id);

        public GarnetObjectType GetCustomGarnetObjectType(int id)
            => customCommandManager.GetCustomGarnetObjectType(id);

        private CustomTransactionProcedure GetCustomTransactionProcedureAndSetArity(CustomTransaction entry, RespServerSession respServerSession, TransactionManager txnManager, ScratchBufferManager scratchBufferManager, int arity)
        {
            int id = entry.id;

            var customTranProc = new CustomTransactionProcedureWithArity(entry.proc(), arity)
            {
                Procedure =
                {
                    txnManager = txnManager,
                    scratchBufferManager = scratchBufferManager,
                    respServerSession = respServerSession
                }
            };
            var setSuccessful = sessionTransactionProcMap.TrySetValue(id, ref customTranProc);
            Debug.Assert(setSuccessful);

            return customTranProc.Procedure;
        }

        private struct CustomTransactionProcedureWithArity
        {
            public CustomTransactionProcedure Procedure { get; }

            public int Arity { get; }

            public CustomTransactionProcedureWithArity(CustomTransactionProcedure procedure, int arity)
            {
                this.Procedure = procedure;
                this.Arity = arity;
            }
        }
    }
}