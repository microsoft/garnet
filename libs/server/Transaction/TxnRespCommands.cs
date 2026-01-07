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
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_NESTED_MULTI, ref dcurr, dend))
                    SendAndReset();
                txnManager.Abort();
                return true;
            }
            txnManager.txnStartHead = readHead;
            txnManager.state = TxnState.Started;
            txnManager.operationCntTxn = 0;
            //Keep track of ptr for key verification when cluster mode is enabled
            txnManager.saveKeyRecvBufferPtr = recvBufferPtr;

            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
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
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_EXEC_ABORT, ref dcurr, dend))
                    SendAndReset();
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

                var startTxn = txnManager.Run();

                if (startTxn)
                {
                    while (!RespWriteUtils.TryWriteArrayLength(txnManager.operationCntTxn, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    endReadHead = _origReadHead;
                    WriteNullArray();
                }

                return true;
            }
            // EXEC without MULTI command
            while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_EXEC_WO_MULTI, ref dcurr, dend))
                SendAndReset();
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
            if (!RespCommandsInfo.TryGetSimpleRespCommandInfo(cmd, out var commandInfo, logger: logger) || !commandInfo.AllowedInTxn)
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_UNK_CMD, ref dcurr, dend))
                    SendAndReset();
                txnManager.Abort();
                return true;
            }

            // Check if input is valid and abort if necessary
            // NOTE: Negative arity means it's an expected minimum of args. Positive means exact.
            var count = parseState.Count;
            var arity = commandInfo.Arity > 0 ? commandInfo.Arity - 1 : commandInfo.Arity + 1;
            if (commandInfo.IsSubCommand)
                arity = arity > 0 ? arity - 1 : arity + 1;
            var invalidNumArgs = arity > 0 ? count != arity : count < -arity;

            // Watch not allowed during TXN
            var isWatch = cmd == RespCommand.WATCH || cmd == RespCommand.WATCHMS || cmd == RespCommand.WATCHOS;

            // todo: Remove once this is supported by enabling transactions across databases
            // SELECT / SWAPDB currently not allowed during TXN
            var isMultiDbCommand = cmd == RespCommand.SELECT || cmd == RespCommand.SWAPDB;

            if (invalidNumArgs || isWatch || isMultiDbCommand)
            {
                if (isWatch)
                {
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_WATCH_IN_MULTI, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                if (invalidNumArgs)
                {
                    var err = string.Format(CmdStrings.GenericErrWrongNumArgs, RespCommandsInfo.GetRespCommandName(cmd));
                    while (!RespWriteUtils.TryWriteError(err, ref dcurr, dend))
                        SendAndReset();
                    txnManager.Abort();
                    return true;
                }

                var errMsg = ReadOnlySpan<byte>.Empty;
                var abort = false;
                switch (cmd)
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
                    while (!RespWriteUtils.TryWriteError(errMsg, ref dcurr, dend))
                        SendAndReset();
                    txnManager.Abort();
                    return true;
                }
            }

            if (cmd == RespCommand.DEBUG && !CanRunDebug())
            {
                while (!RespWriteUtils.TryWriteError(System.Text.Encoding.ASCII.GetBytes(string.Format(
                           CmdStrings.GenericErrCommandDisallowedWithOption, RespCommand.DEBUG, "enable-debug-command")), ref dcurr, dend))
                    SendAndReset();

                txnManager.Abort();
                return true;
            }

            txnManager.LockKeys(commandInfo);

            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_QUEUED, ref dcurr, dend))
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
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_DISCARD_WO_MULTI);
            }
            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
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

            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

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
            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
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
                proc = customCommandManagerSession.GetCustomTransactionProcedure(txId, this, txnManager,
                    scratchBufferAllocator, out arity);
            }
            catch (Exception e)
            {
                logger?.LogError(e, "Getting customer transaction in RUNTXP failed");

                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_NO_TRANSACTION_PROCEDURE, ref dcurr, dend))
                    SendAndReset();

                return true;
            }

            if ((arity > 0 && count != arity) || (arity < 0 && count < -arity))
            {
                var expectedParams = arity > 0 ? arity - 1 : -arity - 1;
                while (!RespWriteUtils.TryWriteError(
                       string.Format(CmdStrings.GenericErrWrongNumArgsTxn, txId, expectedParams, count - 1), ref dcurr,
                       dend))
                    SendAndReset();
            }
            else
                TryTransactionProc((byte)txId, proc, 1);

            return true;
        }
    }
}