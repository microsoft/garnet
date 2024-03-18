// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Text;
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
                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_NESTED_MULTI, ref dcurr, dend))
                    SendAndReset();
                txnManager.Abort();
                readHead += 15;
                return true;
            }
            txnManager.txnStartHead = readHead;
            readHead += 15;
            txnManager.state = TxnState.Started;
            txnManager.operationCntTxn = 0;
            //Keep track of ptr for key verification when cluster mode is enabled
            txnManager.saveKeyRecvBufferPtr = recvBufferPtr;

            while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        private bool NetworkEXEC()
        {
            // pass over the EXEC in buffer during execution
            if (txnManager.state == TxnState.Running)
            {
                txnManager.Commit();
                readHead += 14;
                return true;

            }
            // Abort and reset the transaction 
            else if (txnManager.state == TxnState.Aborted)
            {
                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_EXEC_ABORT, ref dcurr, dend))
                    SendAndReset();
                txnManager.Reset(false);
                readHead += 14;
                return true;
            }
            // start running transaction and setting readHead to first operation
            else if (txnManager.state == TxnState.Started)
            {
                var _origReadHead = readHead;
                readHead = txnManager.txnStartHead + 15;

                txnManager.GetKeysForValidation(recvBufferPtr, out var keys, out int keyCount, out bool readOnly);
                if (NetworkKeyArraySlotVerify(ref keys, readOnly, keyCount))
                {
                    logger?.LogWarning("Failed CheckClusterTxnKeys");
                    txnManager.Reset(false);
                    txnManager.watchContainer.Reset();
                    readHead = _origReadHead + 14;
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
                    readHead = _origReadHead + 14;
                    while (!RespWriteUtils.WriteNull(ref dcurr, dend))
                        SendAndReset();
                }

                return true;
            }
            // EXEC without MULTI command
            while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_EXEC_WO_MULTI, ref dcurr, dend))
                SendAndReset();
            readHead += 14;
            return true;

        }

        /// <summary>
        /// Skip the commands, first phase of the transactions processing.
        /// </summary>
        private bool NetworkSKIP(RespCommand cmd)
        {
            var tmp = recvBufferPtr + readHead;
            if (!RespReadUtils.ReadArrayLength(out int count, ref tmp, recvBufferPtr + bytesRead))
                return false;
            readHead += (int)(tmp - (recvBufferPtr + readHead));
            ReadOnlySpan<byte> bufSpan = new ReadOnlySpan<byte>(recvBufferPtr, bytesRead);

            byte subCommand = 0;
            if (cmd == RespCommand.NONE)
            {
                (cmd, subCommand) = FastParseArrayCommand(count, recvBufferPtr + readHead);

                if (cmd == RespCommand.NONE)
                    (cmd, subCommand) = ParseAdminCommands();
            }

            if (cmd == RespCommand.NONE)
            {
                // Check if we received an entire command
                GetCommand(bufSpan, out bool success1);
                if (!success1) return false;

                if (cmd == RespCommand.NONE)
                {
                    if (!DrainCommands(bufSpan, count - 1))
                        return false;

                    // We got the entire command, but it is not a basic or array command
                    while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_ERR, ref dcurr, dend))
                        SendAndReset();
                    txnManager.Abort();

                    return true;
                }
            }

            // Skip the command itself
            GetCommand(bufSpan, out bool success);
            if (!success)
                return false;

            RespCommandsInfo commandInfo = RespCommandsInfo.findCommand(cmd, subCommand);
            if (commandInfo == null)
            {
                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_ERR, ref dcurr, dend))
                    SendAndReset();
                txnManager.Abort();
                if (!DrainCommands(bufSpan, count - 1))
                    return false;
                return true;
            }

            int abs_arity = commandInfo.arity > 0 ? commandInfo.arity : -commandInfo.arity;
            // Cheking the mininum arity || exact arity 
            bool arity_check = commandInfo.arity < 0 && count < abs_arity || commandInfo.arity > 0 && count != commandInfo.arity;
            // Watch not allowed during TXN
            bool is_watch = (commandInfo.command == RespCommand.WATCH || commandInfo.command == RespCommand.WATCHMS || commandInfo.command == RespCommand.WATCHOS);
            if (arity_check || is_watch)
            {
                if (is_watch)
                {
                    while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_WATCH_IN_MULTI, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    string err = string.Format(CmdStrings.ErrMissingParam, commandInfo.nameStr);
                    while (!RespWriteUtils.WriteResponse(new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes(err)), ref dcurr, dend))
                        SendAndReset();
                    txnManager.Abort();
                }
                if (!DrainCommands(bufSpan, count - 1))
                    return false;
                return true;
            }

            // Get and add keys to txn key list
            int skipped = txnManager.GetKeys(commandInfo.command, count, out ReadOnlySpan<byte> error, subCommand);

            if (skipped < 0)
            {
                // We ran out of data in network buffer, let caller handler it
                if (skipped == -2) return false;

                // Unsupported command
                while (!RespWriteUtils.WriteResponse(error, ref dcurr, dend))
                    SendAndReset();
                txnManager.Abort();
                if (!DrainCommands(bufSpan, count - 1))
                    return false;
                return true;
            }

            for (int i = skipped; i < count; i++)
            {
                GetCommand(bufSpan, out success);
                if (!success) return false;
            }

            while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_QUEUED, ref dcurr, dend))
                SendAndReset();

            txnManager.operationCntTxn++;
            return true;
        }

        /// <summary>
        /// DISCARD
        /// </summary>
        private bool NetworkDISCARD()
        {
            readHead += 17;
            if (txnManager.state == TxnState.None)
            {
                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_DISCARD_WO_MULTI, ref dcurr, dend))
                    SendAndReset();
                return true;
            }
            while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
            txnManager.Reset(false);
            return true;
        }

        /// <summary>
        /// Watch
        /// </summary>
        private bool NetworkWATCH(int count, byte* ptr, StoreType type = StoreType.All)
        {
            GetCommandAsArgSlice(out bool success);
            if (!success) return false;

            if (type != StoreType.All)
            {
                GetCommandAsArgSlice(out success);
                if (!success) return false;
                count--;
            }


            if (count > 2)
            {
                List<ArgSlice> keys = new();

                for (int c = 0; c < count - 1; c++)
                {
                    var key = GetCommandAsArgSlice(out success);
                    if (!success) return false;
                    keys.Add(key);
                }

                foreach (var key in keys)
                    txnManager.Watch(key, type);
            }
            else
            {
                for (int c = 0; c < count - 1; c++)
                {
                    var key = GetCommandAsArgSlice(out success);
                    if (!success) return false;
                    txnManager.Watch(key, type);
                }
            }

            while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        /// <summary>
        /// UNWATCH
        /// </summary>
        private bool NetworkUNWATCH()
        {
            readHead += 17;
            if (txnManager.state == TxnState.None)
            {
                txnManager.watchContainer.Reset();
            }
            while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        private bool NetworkRUNTXPFast(byte* ptr)
        {
            int count = *(ptr + 1) - '0';
            ptr += 16;
            return NetworkRUNTXP(ptr, count);
        }

        private bool NetworkRUNTXP(byte* ptr, int count)
        {
            if (!RespReadUtils.ReadIntWithLengthHeader(out int txid, ref ptr, recvBufferPtr + bytesRead))
                return false;

            byte* start = ptr;

            // Verify all args available
            for (int i = 0; i < count - 2; i++)
            {
                byte* result = default;
                int len = 0;
                if (!RespReadUtils.ReadPtrWithLengthHeader(ref result, ref len, ref ptr, recvBufferPtr + bytesRead))
                    return false;
            }

            // Shift read head
            readHead = (int)(ptr - recvBufferPtr);


            var (proc, numParams) = customCommandManagerSession.GetCustomTransactionProcedure(txid, txnManager, scratchBufferManager);
            if (count - 2 == numParams)
            {
                TryTransactionProc((byte)txid, start, ptr, proc);
            }
            else
            {
                while (!RespWriteUtils.WriteDirect(Encoding.ASCII.GetBytes($"-ERR Invalid number of parameters to stored proc {txid}, expected {numParams}, actual {count - 2}\r\n"), ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            return true;
        }
    }
}