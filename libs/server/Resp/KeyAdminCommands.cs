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
        private bool NetworkRENAME<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 2)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.RENAME));
            }

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
        private bool NetworkRENAMENX<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 2)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.RENAMENX));
            }

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
        /// <typeparam name="TGarnetApi"> Garnet API type </typeparam>
        /// <param name="garnetApi"> Garnet API reference </param>
        /// <returns> True if successful, false otherwise </returns>
        private bool NetworkGETDEL<TGarnetApi>(ref TGarnetApi garnetApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.PERSIST));
            }

            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
            var status = garnetApi.GETDEL(ref sbKey, ref o);

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
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool NetworkEXISTS<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            var count = parseState.Count;
            if (count < 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.EXISTS));
            }

            var exists = 0;

            for (var i = 0; i < parseState.Count; i++)
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
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="command">Indicates which command to use, expire or pexpire.</param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool NetworkEXPIRE<TGarnetApi>(RespCommand command, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            var count = parseState.Count;
            if (count < 2 || count > 3)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.EXPIRE));
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
            if (parseState.Count != 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.PERSIST));
            }

            var key = parseState.GetArgSliceByRef(0);
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
        /// <param name="command">either if the call is for tll or pttl command</param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool NetworkTTL<TGarnetApi>(RespCommand command, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.PERSIST));
            }

            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
            var status = command == RespCommand.TTL ?
                        storageApi.TTL(ref sbKey, StoreType.All, ref o) :
                        storageApi.PTTL(ref sbKey, StoreType.All, ref o);

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

        /// <summary>
        /// Get the absolute Unix timestamp at which the given key will expire.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="command">either if the call is for EXPIRETIME or PEXPIRETIME command</param>
        /// <param name="storageApi"></param>
        /// <returns>Returns the absolute Unix timestamp (since January 1, 1970) in seconds or milliseconds at which the given key will expire.</returns>
        private bool NetworkEXPIRETIME<TGarnetApi>(RespCommand command, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.EXPIRETIME));
            }

            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
            var status = command == RespCommand.EXPIRETIME ?
                        storageApi.EXPIRETIME(ref sbKey, StoreType.All, ref o) :
                        storageApi.PEXPIRETIME(ref sbKey, StoreType.All, ref o);

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