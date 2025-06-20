// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// Session-specific access to custom command data held by CustomCommandManager
    /// For both custom procedures and custom transactions, this class maintains a cached instance of the transaction / procedure that can be called directly per-session
    /// This class also maintains a cache of custom command info and docs to avoid the cost of synchronization when calling CustomCommandManager
    /// </summary>
    internal sealed class CustomCommandManagerSession
    {
        // Initial size of expandable maps
        private static readonly int MinMapSize = 8;

        // The instance of the CustomCommandManager that this session is associated with
        readonly CustomCommandManager customCommandManager;

        // These session specific maps are indexed by the same ID as the arrays in CustomCommandManager
        // Maps between the transaction ID and a tuple of the per-session transaction procedure and its arity
        ExpandableMap<(CustomTransactionProcedure, int)> sessionTransactionProcMap;
        // Maps between the custom procedure ID and the per-session custom procedure instance
        ExpandableMap<CustomProcedure> sessionCustomProcMap;

        public CustomCommandManagerSession(CustomCommandManager customCommandManager)
        {
            this.customCommandManager = customCommandManager;

            sessionTransactionProcMap = new ExpandableMap<(CustomTransactionProcedure, int)>(MinMapSize, 0, byte.MaxValue);
            sessionCustomProcMap = new ExpandableMap<CustomProcedure>(MinMapSize, 0, byte.MaxValue);
        }

        /// <summary>
        /// Get a custom procedure by ID
        /// </summary>
        /// <param name="id">The procedure ID</param>
        /// <param name="respServerSession">The current session</param>
        /// <returns>The per-session instance of the procedure</returns>
        /// <exception cref="GarnetException"></exception>
        public CustomProcedure GetCustomProcedure(int id, RespServerSession respServerSession)
        {
            // Check if we already have a cached entry of the per-session custom procedure instance
            if (!sessionCustomProcMap.TryGetValue(id, out var customProc))
            {
                // If not, get the custom procedure from the CustomCommandManager
                if (!customCommandManager.TryGetCustomProcedure(id, out var entry))
                    throw new GarnetException($"Custom procedure {id} not found");

                // Create the session-specific instance and add it to the cache
                customProc = entry.CustomProcedureFactory();
                customProc.respServerSession = respServerSession;
                sessionCustomProcMap.TrySetValue(id, customProc);
            }

            return customProc;
        }

        /// <summary>
        /// Get a custom transaction procedure by ID
        /// </summary>
        /// <param name="id">The transaction ID</param>
        /// <param name="respServerSession">The current session</param>
        /// <param name="txnManager">txnManager</param>
        /// <param name="scratchBufferAllocator">scratchBufferAllocator</param>
        /// <param name="arity">The arity of the transaction</param>
        /// <returns>The per-session instance of the transaction</returns>
        /// <exception cref="GarnetException"></exception>
        public CustomTransactionProcedure GetCustomTransactionProcedure(int id, RespServerSession respServerSession,
            TransactionManager txnManager, ScratchBufferAllocator scratchBufferAllocator, out int arity)
        {
            // Check if we already have a cached entry of the per-session custom transaction instance
            if (sessionTransactionProcMap.TryGetValue(id, out var tranToArity))
            {
                if (tranToArity.Item1 != null)
                {
                    arity = tranToArity.Item2;
                    return tranToArity.Item1;
                }
            }

            if (!customCommandManager.TryGetCustomTransactionProcedure(id, out var entry))
                throw new GarnetException($"Transaction procedure {id} not found");

            // Create the session-specific instance and add it to the cache
            var customTranProc = entry.proc();
            customTranProc.txnManager = txnManager;
            customTranProc.scratchBufferAllocator = scratchBufferAllocator;
            customTranProc.respServerSession = respServerSession;

            arity = entry.arity;
            tranToArity = new ValueTuple<CustomTransactionProcedure, int>(customTranProc, arity);
            sessionTransactionProcMap.TrySetValueByRef(id, ref tranToArity);

            return customTranProc;
        }

        /// <summary>
        /// Get a custom raw-string command by name
        /// </summary>
        /// <param name="command">The command name to match</param>
        /// <param name="cmd">The matching command</param>
        /// <returns>True if command name matched an existing command</returns>
        public bool Match(ReadOnlySpan<byte> command, out CustomRawStringCommand cmd)
            => customCommandManager.Match(command, out cmd);

        /// <summary>
        /// Get a custom transaction by name
        /// </summary>
        /// <param name="command">The transaction name to match</param>
        /// <param name="cmd">The matching transaction</param>
        /// <returns>True if transaction name matched an existing transaction</returns>
        public bool Match(ReadOnlySpan<byte> command, out CustomTransaction cmd)
            => customCommandManager.Match(command, out cmd);

        /// <summary>
        /// Get a custom object command by name
        /// </summary>
        /// <param name="command">The command name to match</param>
        /// <param name="cmd">The matching command</param>
        /// <returns>True if command name matched an existing command</returns>
        public bool Match(ReadOnlySpan<byte> command, out CustomObjectCommand cmd)
            => customCommandManager.Match(command, out cmd);

        /// <summary>
        /// Get a custom procedure by name
        /// </summary>
        /// <param name="command">The procedure name to match</param>
        /// <param name="cmd">The matching procedure</param>
        /// <returns>True if procedure name matched an existing procedure</returns>
        public bool Match(ReadOnlySpan<byte> command, out CustomProcedureWrapper cmd)
            => customCommandManager.Match(command, out cmd);

        /// <summary>
        /// Get custom command info by name
        /// </summary>
        /// <param name="cmdName">The command name</param>
        /// <param name="respCommandsInfo">The matching command info</param>
        /// <returns>True if command info was found</returns>
        public bool TryGetCustomCommandInfo(string cmdName, out RespCommandsInfo respCommandsInfo)
            => customCommandManager.TryGetCustomCommandInfo(cmdName, out respCommandsInfo);

        /// <summary>
        /// Get custom command docs by name
        /// </summary>
        /// <param name="cmdName">The command name</param>
        /// <param name="respCommandsDocs">The matching command docs</param>
        /// <returns>True if command docs was found</returns>
        public bool TryGetCustomCommandDocs(string cmdName, out RespCommandDocs respCommandsDocs)
            => customCommandManager.TryGetCustomCommandDocs(cmdName, out respCommandsDocs);

        /// <summary>
        /// Get all custom command infos
        /// </summary>
        /// <returns>Map between custom command name and custom command info</returns>
        internal IReadOnlyDictionary<string, RespCommandsInfo> GetAllCustomCommandsInfos()
            => customCommandManager.GetAllCustomCommandsInfos();

        /// <summary>
        /// Get all custom command docs
        /// </summary>
        /// <returns>Map between custom command name and custom command docs</returns>
        internal IReadOnlyDictionary<string, RespCommandDocs> GetAllCustomCommandsDocs()
            => customCommandManager.GetAllCustomCommandsDocs();

        /// <summary>
        /// Get count of all custom command infos
        /// </summary>
        /// <returns>Count</returns>
        internal int GetCustomCommandInfoCount() => customCommandManager.GetCustomCommandInfoCount();

        /// <summary>
        /// Get RespCommand enum by command ID
        /// </summary>
        /// <param name="id">Command ID</param>
        /// <returns>Matching RespCommand</returns>
        public RespCommand GetCustomRespCommand(int id)
            => customCommandManager.GetCustomRespCommand(id);

        /// <summary>
        /// Get GarnetObjectType enum by object type ID
        /// </summary>
        /// <param name="id">Object type ID</param>
        /// <returns>Matching GarnetObjectType</returns>
        public GarnetObjectType GetCustomGarnetObjectType(int id)
            => customCommandManager.GetCustomGarnetObjectType(id);
    }
}