// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {

        /// <summary>
        /// TryRENAME
        /// </summary>
        private bool NetworkRENAME(ref GarnetApi storageApi)
        {
            if (parseState.Count != 2)
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.RENAME));

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
        /// TryRENAMENX
        /// </summary>
        private bool NetworkRENAMENX(ref GarnetApi storageApi)
        {
            if (parseState.Count != 2)
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.RENAMENX));

            var oldKeySlice = parseState.GetArgSliceByRef(0);
            var newKeySlice = parseState.GetArgSliceByRef(1);
            var status = storageApi.RENAMENX(oldKeySlice, newKeySlice, out var result);

            if (status == GarnetStatus.OK)
            {
                // Integer reply: 1 if key was renamed to newkey.
                // Integer reply: 0 if newkey already exists.
                while (!RespWriteUtils.WriteInteger(result, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_NOSUCHKEY, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// GETDEL command processor
        /// </summary>
        /// <param name="garnetApi"> Garnet API reference </param>
        /// <returns> True if successful, false otherwise </returns>
        private bool NetworkGETDEL<TKeyLocker, TEpochGuard>(ref GarnetApi garnetApi)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            if (parseState.Count != 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.PERSIST));
            }

            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
            var status = garnetApi.GETDEL<TKeyLocker, TEpochGuard>(ref sbKey, ref o);

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
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool NetworkEXISTS<TKeyLocker, TEpochGuard>(ref GarnetApi storageApi)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            var count = parseState.Count;
            if (count < 1)
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.EXISTS));

            var exists = 0;

            for (var i = 0; i < parseState.Count; i++)
            {
                var key = parseState.GetArgSliceByRef(i);
                var status = storageApi.EXISTS<TKeyLocker, TEpochGuard>(key);
                if (status == GarnetStatus.OK)
                    exists++;
            }

            while (!RespWriteUtils.WriteInteger(exists, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        bool TryGetExpireOption(ReadOnlySpan<byte> item, out ExpireOption option)
        {
            if (item.EqualsUpperCaseSpanIgnoringCase("NX"u8))
            {
                option = ExpireOption.NX;
                return true;
            }
            if (item.EqualsUpperCaseSpanIgnoringCase("XX"u8))
            {
                option = ExpireOption.XX;
                return true;
            }
            if (item.EqualsUpperCaseSpanIgnoringCase("GT"u8))
            {
                option = ExpireOption.GT;
                return true;
            }
            if (item.EqualsUpperCaseSpanIgnoringCase("LT"u8))
            {
                option = ExpireOption.LT;
                return true;
            }
            option = ExpireOption.None;
            return false;
        }

        /// <summary>
        /// Set a timeout on a key.
        /// </summary>
        /// <param name="command">Indicates which command to use, expire or pexpire.</param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool NetworkEXPIRE<TKeyLocker, TEpochGuard>(RespCommand command, ref GarnetApi storageApi)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            var count = parseState.Count;
            if (count is < 2 or > 3)
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.EXPIRE));

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

            var expireOption = ExpireOption.None;

            if (parseState.Count > 2)
            {
                if (!TryGetExpireOption(parseState.GetArgSliceByRef(2).ReadOnlySpan, out expireOption))
                {
                    var optionStr = parseState.GetString(2);

                    while (!RespWriteUtils.WriteError($"ERR Unsupported option {optionStr}", ref dcurr, dend))
                        SendAndReset();
                    return true;
                }
            }

            var status = command == RespCommand.EXPIRE ?
                        storageApi.EXPIRE<TKeyLocker, TEpochGuard>(key, expiryMs, out var timeoutSet, StoreType.All, expireOption) :
                        storageApi.PEXPIRE<TKeyLocker, TEpochGuard>(key, expiryMs, out timeoutSet, StoreType.All, expireOption);

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
        /// <param name="storageApi">The Garnet API instance</param>
        /// <returns></returns>
        private bool NetworkPERSIST<TKeyLocker, TEpochGuard>(ref GarnetApi storageApi)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            if (parseState.Count != 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.PERSIST));
            }

            var key = parseState.GetArgSliceByRef(0);
            var status = storageApi.PERSIST<TKeyLocker, TEpochGuard>(key);

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
        /// <param name="command">either if the call is for tll or pttl command</param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool NetworkTTL<TKeyLocker, TEpochGuard>(RespCommand command, ref GarnetApi storageApi)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            if (parseState.Count != 1)
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.PERSIST));

            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
            var status = command == RespCommand.TTL ?
                        storageApi.TTL<TKeyLocker, TEpochGuard>(ref sbKey, StoreType.All, ref o) :
                        storageApi.PTTL<TKeyLocker, TEpochGuard>(ref sbKey, StoreType.All, ref o);

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