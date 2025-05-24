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
        private bool NetworkMULTI()
        {
            if (txnManager.state != TxnState.None)
            {
                WriteError(CmdStrings.RESP_ERR_GENERIC_NESTED_MULTI);
                txnManager.Abort();
                return true;
            }
            txnManager.txnStartHead = readHead;
            txnManager.state = TxnState.Started;
            txnManager.operationCntTxn = 0;
            //Keep track of ptr for key verification when cluster mode is enabled
            txnManager.saveKeyRecvBufferPtr = recvBufferPtr;

            WriteOK();
            return true;
        }

        private bool NetworkEXEC()
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
                WriteError(CmdStrings.RESP_ERR_EXEC_ABORT);
                txnManager.Reset(false);
                return true;
            }
            // start running transaction and setting readHead to first operation
            else if (txnManager.state == TxnState.Started)
            {
                var _origReadHead = endReadHead;
                endReadHead = txnManager.txnStartHead;

                txnManager.GetKeysForValidation(recvBufferPtr, out var keys, out int keyCount, out bool readOnly);
                if (NetworkKeyArraySlotVerify(keys, readOnly, keyCount))
                {
                    logger?.LogWarning("Failed CheckClusterTxnKeys");
                    txnManager.Reset(false);
                    txnManager.watchContainer.Reset();
                    endReadHead = _origReadHead;
                    return true;
                }

                bool startTxn = txnManager.Run();

                if (startTxn)
                {
                    WriteArrayLength(txnManager.operationCntTxn);
                }
                else
                {
                    endReadHead = _origReadHead;
                    while (!RespWriteUtils.TryWriteNullArray(ref dcurr, dend))
                        SendAndReset();
                }

                return true;
            }

            // EXEC without MULTI command
            WriteError(CmdStrings.RESP_ERR_GENERIC_EXEC_WO_MULTI);
            return true;

        }

        /// <summary>
        /// Skip the commands, first phase of the transactions processing.
        /// </summary>
        private bool NetworkSKIP(RespCommand cmd)
        {
            // Retrieve the meta-data for the command to do basic sanity checking for command arguments
            // Normalize will turn internal "not-real commands" such as SETEXNX, and SETEXXX to the command info parent
            cmd = cmd.NormalizeForACLs();
            if (!RespCommandsInfo.TryGetRespCommandInfo(cmd, out var commandInfo, txnOnly: true, logger))
            {
                WriteError(CmdStrings.RESP_ERR_GENERIC_UNK_CMD);
                txnManager.Abort();
                return true;
            }

            // Check if input is valid and abort if necessary
            // NOTE: Negative arity means it's an expected minimum of args. Positive means exact.
            int count = parseState.Count;
            var arity = commandInfo.Arity > 0 ? commandInfo.Arity - 1 : commandInfo.Arity + 1;
            bool invalidNumArgs = arity > 0 ? count != (arity) : count < -arity;

            // Watch not allowed during TXN
            bool isWatch = commandInfo.Command == RespCommand.WATCH || commandInfo.Command == RespCommand.WATCHMS || commandInfo.Command == RespCommand.WATCHOS;

            // todo: Remove once this is supported by enabling transactions across databases
            // SELECT / SWAPDB currently not allowed during TXN
            var isMultiDbCommand =
                commandInfo.Command == RespCommand.SELECT || commandInfo.Command == RespCommand.SWAPDB;

            if (invalidNumArgs || isWatch || isMultiDbCommand)
            {
                if (isWatch)
                {
                    WriteError(CmdStrings.RESP_ERR_GENERIC_WATCH_IN_MULTI);
                    return true;
                }

                if (invalidNumArgs)
                {
                    var err = string.Format(CmdStrings.GenericErrWrongNumArgs, commandInfo.Name);
                    WriteError(err);
                    txnManager.Abort();
                    return true;
                }

                var errMsg = ReadOnlySpan<byte>.Empty;
                var abort = false;
                switch (commandInfo.Command)
                {
                    case RespCommand.SWAPDB:
                        errMsg = CmdStrings.RESP_ERR_SWAPDB_IN_TXN_UNSUPPORTED;
                        abort = true;
                        break;
                    case RespCommand.SELECT:
                        if (parseState.TryGetInt(0, out var index) && index != activeDbId)
                        {
                            errMsg = CmdStrings.RESP_ERR_SELECT_IN_TXN_UNSUPPORTED;
                            abort = true;
                        }
                        break;
                }

                if (abort)
                {
                    WriteError(errMsg);
                    txnManager.Abort();
                    return true;
                }
            }

            // Get and add keys to txn key list
            int skipped = txnManager.GetKeys(cmd, count, out ReadOnlySpan<byte> error);

            if (skipped < 0)
            {
                // We ran out of data in network buffer, let caller handler it
                if (skipped == -2) return false;

                // We found an unsupported command, abort
                WriteError(error);

                txnManager.Abort();

                return true;
            }

            WriteDirect(CmdStrings.RESP_QUEUED);

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
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_DISCARD_WO_MULTI);
            }

            WriteOK();
            txnManager.Reset(false);
            return true;
        }

        /// <summary>
        /// Common implementation of various WATCH commands and subcommands.
        /// </summary>
        /// <param name="type">Store type that's bein gwatch</param>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool CommonWATCH(StoreType type)
        {
            var count = parseState.Count;
            // Have to provide at least one key
            if (count == 0)
            {
                return AbortWithErrorMessage(CmdStrings.GenericErrWrongNumArgs);
            }

            List<ArgSlice> keys = [];

            for (var c = 0; c < count; c++)
            {
                var nextKey = parseState.GetArgSliceByRef(c);
                keys.Add(nextKey);
            }

            foreach (var toWatch in keys)
            {
                txnManager.Watch(toWatch, type);
            }

            WriteOK();

            return true;
        }

        /// <summary>
        /// WATCH MS key [key ..]
        /// </summary>
        private bool NetworkWATCH_MS()
        => CommonWATCH(StoreType.Main);

        /// <summary>
        /// WATCH OS key [key ..]
        /// </summary>
        private bool NetworkWATCH_OS()
        => CommonWATCH(StoreType.Object);

        /// <summary>
        /// Watch key [key ...]
        /// </summary>
        private bool NetworkWATCH()
        => CommonWATCH(StoreType.All);

        /// <summary>
        /// UNWATCH
        /// </summary>
        private bool NetworkUNWATCH()
        {
            if (txnManager.state == TxnState.None)
            {
                txnManager.watchContainer.Reset();
            }
            
            WriteOK();
            return true;
        }

        private bool NetworkRUNTXPFast(byte* ptr)
        {
            int count = *(ptr - 16 + 1) - '0';
            return NetworkRUNTXP();
        }

        private bool NetworkRUNTXP()
        {
            var count = parseState.Count;
            if (count < 1)
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.RUNTXP));

            if (!parseState.TryGetInt(0, out var txId))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
            }

            CustomTransactionProcedure proc;
            int arity;

            try
            {
                proc = customCommandManagerSession.GetCustomTransactionProcedure(txId, this, txnManager, scratchBufferManager, out arity);
            }
            catch (Exception e)
            {
                logger?.LogError(e, "Getting customer transaction in RUNTXP failed");
                WriteError(CmdStrings.RESP_ERR_NO_TRANSACTION_PROCEDURE);

                return true;
            }

            if ((arity > 0 && count != arity) || (arity < 0 && count < -arity))
            {
                var expectedParams = arity > 0 ? arity - 1 : -arity - 1;
                WriteError(string.Format(CmdStrings.GenericErrWrongNumArgsTxn, txId, expectedParams, count - 1));
            }
            else
                TryTransactionProc((byte)txId, proc, 1);

            return true;
        }
    }
}