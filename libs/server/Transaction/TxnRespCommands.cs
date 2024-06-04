// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Server session for RESP protocol - Transaction commands are in this file
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// MULTI
        /// </summary>
        private bool NetworkMULTI(byte* ptr)
        {
            if (txnManager.state != TxnState.None)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_NESTED_MULTI, ref dcurr, dend))
                    SendAndReset();
                txnManager.Abort();
                return true;
            }
            txnManager.txnStartHead = readHead;
            txnManager.state = TxnState.Started;
            txnManager.operationCntTxn = 0;
            //Keep track of ptr for key verification when cluster mode is enabled
            txnManager.saveKeyRecvBufferPtr = recvBufferPtr;

            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        private bool NetworkEXEC(byte* ptr)
        {
            // pass over the EXEC in buffer during execution
            if (txnManager.state == TxnState.Running)
            {
                txnManager.Commit();
                return true;

            }
            // Abort and reset the transaction 
            else if (txnManager.state == TxnState.Aborted)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_EXEC_ABORT, ref dcurr, dend))
                    SendAndReset();
                txnManager.Reset(false);
                return true;
            }
            // start running transaction and setting readHead to first operation
            else if (txnManager.state == TxnState.Started)
            {
                var _origReadHead = readHead;
                readHead = txnManager.txnStartHead;

                txnManager.GetKeysForValidation(recvBufferPtr, out var keys, out int keyCount, out bool readOnly);
                if (NetworkKeyArraySlotVerify(ref keys, readOnly, keyCount))
                {
                    logger?.LogWarning("Failed CheckClusterTxnKeys");
                    txnManager.Reset(false);
                    txnManager.watchContainer.Reset();
                    readHead = _origReadHead;
                    return true;
                }

                bool startTxn = txnManager.Run();

                if (startTxn)
                {
                    while (!RespWriteUtils.WriteArrayLength(txnManager.operationCntTxn, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    readHead = _origReadHead;
                    while (!RespWriteUtils.WriteNull(ref dcurr, dend))
                        SendAndReset();
                }

                return true;
            }
            // EXEC without MULTI command
            while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_EXEC_WO_MULTI, ref dcurr, dend))
                SendAndReset();
            return true;

        }

        /// <summary>
        /// Skip the commands, first phase of the transactions processing.
        /// </summary>
        private bool NetworkSKIP(RespCommand cmd, int count)
        {
            ReadOnlySpan<byte> bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);

            // Retrieve the meta-data for the command to do basic sanity checking for command arguments
            if (!RespCommandsInfo.TryGetRespCommandInfo(cmd, out var commandInfo, txnOnly: true, logger))
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_UNK_CMD, ref dcurr, dend))
                    SendAndReset();
                txnManager.Abort();
                if (!DrainCommands(bufSpan, count))
                    return false;
                return true;
            }

            // Check if input is valid and abort if necessary
            // NOTE: Negative arity means it's an expected minimum of args. Positive means exact.
            var arity = commandInfo.Arity > 0 ? commandInfo.Arity - 1 : commandInfo.Arity + 1;
            bool invalidNumArgs = arity > 0 ? count != (arity) : count < -arity;

            // Watch not allowed during TXN
            bool isWatch = commandInfo.Command == RespCommand.WATCH || commandInfo.Command == RespCommand.WATCH_MS || commandInfo.Command == RespCommand.WATCH_OS;

            if (invalidNumArgs || isWatch)
            {
                if (isWatch)
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_WATCH_IN_MULTI, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    string err = string.Format(CmdStrings.GenericErrWrongNumArgs, commandInfo.Name);
                    while (!RespWriteUtils.WriteError(err, ref dcurr, dend))
                        SendAndReset();
                    txnManager.Abort();
                }

                if (!DrainCommands(bufSpan, count))
                    return false;

                return true;
            }

            // Get and add keys to txn key list
            int skipped = txnManager.GetKeys(cmd, count, out ReadOnlySpan<byte> error);

            if (skipped < 0)
            {
                // We ran out of data in network buffer, let caller handler it
                if (skipped == -2) return false;

                // We found an unsupported command, abort
                while (!RespWriteUtils.WriteError(error, ref dcurr, dend))
                    SendAndReset();

                txnManager.Abort();

                if (!DrainCommands(bufSpan, count))
                    return false;

                return true;
            }

            // Consume the remaining arguments in the input
            for (int i = skipped; i < count; i++)
            {
                GetCommand(bufSpan, out bool success);

                if (!success)
                    return false;
            }

            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_QUEUED, ref dcurr, dend))
                SendAndReset();

            txnManager.operationCntTxn++;
            return true;
        }

        /// <summary>
        /// DISCARD
        /// </summary>
        private bool NetworkDISCARD()
        {
            if (txnManager.state == TxnState.None)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_DISCARD_WO_MULTI, ref dcurr, dend))
                    SendAndReset();
                return true;
            }
            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
            txnManager.Reset(false);
            return true;
        }

        /// <summary>
        /// Common implementation of various WATCH commands and subcommands.
        /// </summary>
        /// <param name="count">Remaining keys in the command buffer.</param>
        /// <param name="type">Store type that's bein gwatch</param>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool CommonWATCH(int count, StoreType type)
        {
            // Have to provide at least one key
            if (count == 0)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.GenericErrWrongNumArgs, ref dcurr, dend))
                    SendAndReset();

                return true;
            }

            List<ArgSlice> keys = new();

            for (int c = 0; c < count; c++)
            {
                var nextKey = GetCommandAsArgSlice(out bool success);
                if (!success) return false;

                keys.Add(nextKey);
            }

            foreach (var toWatch in keys)
            {
                txnManager.Watch(toWatch, type);
            }

            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// WATCH MS key [key ..]
        /// </summary>
        private bool NetworkWATCH_MS(int count)
        => CommonWATCH(count, StoreType.Main);

        /// <summary>
        /// WATCH OS key [key ..]
        /// </summary>
        private bool NetworkWATCH_OS(int count)
        => CommonWATCH(count, StoreType.Object);

        /// <summary>
        /// Watch key [key ...]
        /// </summary>
        private bool NetworkWATCH(int count)
        => CommonWATCH(count, StoreType.All);

        /// <summary>
        /// UNWATCH
        /// </summary>
        private bool NetworkUNWATCH()
        {
            if (txnManager.state == TxnState.None)
            {
                txnManager.watchContainer.Reset();
            }
            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        private bool NetworkRUNTXPFast(byte* ptr)
        {
            int count = *(ptr - 16 + 1) - '0';
            return NetworkRUNTXP(count, ptr);
        }

        private bool NetworkRUNTXP(int count, byte* ptr)
        {
            if (!RespReadUtils.ReadIntWithLengthHeader(out int txid, ref ptr, recvBufferPtr + bytesRead))
                return false;

            byte* start = ptr;

            // Verify all args available
            for (int i = 0; i < count - 1; i++)
            {
                byte* result = default;
                int len = 0;
                if (!RespReadUtils.ReadPtrWithLengthHeader(ref result, ref len, ref ptr, recvBufferPtr + bytesRead))
                    return false;
            }

            // Shift read head
            readHead = (int)(ptr - recvBufferPtr);


            var (proc, numParams) = customCommandManagerSession.GetCustomTransactionProcedure(txid, txnManager, scratchBufferManager);
            if (count - 1 == numParams)
            {
                TryTransactionProc((byte)txid, start, ptr, proc);
            }
            else
            {
                while (!RespWriteUtils.WriteError($"ERR Invalid number of parameters to stored proc {txid}, expected {numParams}, actual {count - 1}", ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            return true;
        }
    }
}