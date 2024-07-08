// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {

        /// <summary>
        /// TryRENAME
        /// </summary>
        private bool NetworkRENAME<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.count != 2)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.RENAME), parseState.count);
            }

            if (NetworkMultiKeySlotVerify(readOnly: false))
                return true;

            var oldKeySlice = parseState.GetArgSliceByRef(0);
            var newKeySlice = parseState.GetArgSliceByRef(1);

            var status = storageApi.RENAME(oldKeySlice, newKeySlice);

            switch (status)
            {
                case GarnetStatus.OK:
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_NOSUCHKEY, ref dcurr, dend))
                        SendAndReset();
                    break;
            }
            return true;
        }

        /// <summary>
        /// GETDEL command processor
        /// </summary>
        /// <typeparam name="TGarnetApi"> Garnet API type </typeparam>
        /// <param name="ptr"> Location of command buffer </param>
        /// <param name="garnetApi"> Garnet API reference </param>
        /// <returns> True if successful, false otherwise </returns>
        private bool NetworkGETDEL<TGarnetApi>(byte* ptr, ref TGarnetApi garnetApi)
            where TGarnetApi : IGarnetApi
        {
            byte* keyPtr = null;
            int ksize = 0;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                return false;
            readHead = (int)(ptr - recvBufferPtr);

            if (NetworkMultiKeySlotVerify(readOnly: false))
                return true;

            keyPtr -= sizeof(int);
            *(int*)keyPtr = ksize;

            var o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
            var status = garnetApi.GETDEL(ref Unsafe.AsRef<SpanByte>(keyPtr), ref o);

            if (status == GarnetStatus.OK)
            {
                if (!o.IsSpanByte)
                    SendAndReset(o.Memory, o.Length);
                else
                    dcurr += o.Length;
            }
            else
            {
                Debug.Assert(o.IsSpanByte);
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// EXISTS multiple keys
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool NetworkEXISTS<TGarnetApi>(int count, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (count < 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.EXISTS), count);
            }

            int exists = 0;

            if (NetworkMultiKeySlotVerify(readOnly: true))
                return true;

            for (int i = 0; i < count; i++)
            {
                var key = parseState.GetArgSliceByRef(i);
                var status = storageApi.EXISTS(key);
                if (status == GarnetStatus.OK)
                    exists++;
            }

            while (!RespWriteUtils.WriteInteger(exists, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Set a timeout on a key.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="command">Indicates which command to use, expire or pexpire.</param>
        /// <param name="count">Number of arguments sent with this command.</param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool NetworkEXPIRE<TGarnetApi>(int count, RespCommand command, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (count < 2 || count > 3)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.EXPIRE), count);
            }

            var key = parseState.GetArgSliceByRef(0);
            if (!parseState.TryGetInt(1, out var expiryValue))
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var expiryMs = command == RespCommand.EXPIRE
                ? TimeSpan.FromSeconds(expiryValue)
                : TimeSpan.FromMilliseconds(expiryValue);

            var invalidOption = false;
            var expireOption = ExpireOption.None;
            var optionStr = "";

            if (count > 2)
            {
                optionStr = parseState.GetString(2);
                
                if (!Enum.TryParse(optionStr, ignoreCase: true, out expireOption))
                {
                    invalidOption = true;
                }
            }

            if (invalidOption)
            {
                while (!RespWriteUtils.WriteError($"ERR Unsupported option {optionStr}", ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (NetworkMultiKeySlotVerify(readOnly: false, firstKey: 0, lastKey: 0))
                return true;

            var status = command == RespCommand.EXPIRE ?
                        storageApi.EXPIRE(key, expiryMs, out var timeoutSet, StoreType.All, expireOption) :
                        storageApi.PEXPIRE(key, expiryMs, out timeoutSet, StoreType.All, expireOption);

            if (status == GarnetStatus.OK && timeoutSet)
            {
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_RETURN_VAL_1, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// PERSIST command
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="storageApi">The Garnet API instance</param>
        /// <returns></returns>
        private bool NetworkPERSIST<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.count != 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.PERSIST), parseState.count);
            }

            var key = parseState.GetArgSliceByRef(0);

            if (NetworkMultiKeySlotVerify(readOnly: false))
                return true;

            var status = storageApi.PERSIST(key);

            if (status == GarnetStatus.OK)
            {
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_RETURN_VAL_1, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                    SendAndReset();
            }
            return true;
        }

        /// <summary>
        /// Returns the remaining time to live of a key that has a timeout.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="ptr"></param>
        /// <param name="command">either if the call is for tll or pttl command</param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool NetworkTTL<TGarnetApi>(byte* ptr, RespCommand command, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.count != 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.PERSIST), parseState.count);
            }

            byte* keyPtr = null;
            int ksize = 0;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                return false;
            readHead = (int)(ptr - recvBufferPtr);

            if (NetworkMultiKeySlotVerify(readOnly: true))
                return true;

            keyPtr -= sizeof(int);
            *(int*)keyPtr = ksize;

            var o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
            var status = command == RespCommand.TTL ?
                        storageApi.TTL(ref Unsafe.AsRef<SpanByte>(keyPtr), StoreType.All, ref o) :
                        storageApi.PTTL(ref Unsafe.AsRef<SpanByte>(keyPtr), StoreType.All, ref o);

            if (status == GarnetStatus.OK)
            {
                if (!o.IsSpanByte)
                    SendAndReset(o.Memory, o.Length);
                else
                    dcurr += o.Length;
            }
            else
            {
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_RETURN_VAL_N2, ref dcurr, dend))
                    SendAndReset();
            }
            return true;
        }
    }
}