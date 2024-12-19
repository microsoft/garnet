﻿// Copyright (c) Microsoft Corporation.
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
            // one optional command for with etag
            if (parseState.Count < 2 || parseState.Count > 3)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.RENAME));
            }

            var oldKeySlice = parseState.GetArgSliceByRef(0);
            var newKeySlice = parseState.GetArgSliceByRef(1);

            var withEtag = false;
            if (parseState.Count == 3)
            {
                if (!parseState.GetArgSliceByRef(2).ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.WITHETAG))
                {
                    while (!RespWriteUtils.WriteError($"ERR Unsupported option {parseState.GetString(2)}", ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                withEtag = true;
            }

            var status = storageApi.RENAME(oldKeySlice, newKeySlice, withEtag);

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
            // one optional command for with etag
            if (parseState.Count < 2 || parseState.Count > 3)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.RENAMENX));
            }

            var oldKeySlice = parseState.GetArgSliceByRef(0);
            var newKeySlice = parseState.GetArgSliceByRef(1);

            var withEtag = false;
            if (parseState.Count == 3)
            {
                if (!parseState.GetArgSliceByRef(2).ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.WITHETAG))
                {
                    while (!RespWriteUtils.WriteError($"ERR Unsupported option {parseState.GetString(2)}", ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                withEtag = true;
            }

            var status = storageApi.RENAMENX(oldKeySlice, newKeySlice, out var result, withEtag);

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
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.GETDEL));
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
            if (count < 2 || count > 4)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.EXPIRE));
            }

            var key = parseState.GetArgSliceByRef(0);
            if (!parseState.TryGetInt(1, out _))
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var expireOption = ExpireOption.None;
            if (parseState.Count > 2)
            {
                if (!parseState.TryGetExpireOption(2, out expireOption))
                {
                    var optionStr = parseState.GetString(2);

                    while (!RespWriteUtils.WriteError($"ERR Unsupported option {optionStr}", ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                if (parseState.Count > 3)
                {
                    if (!parseState.TryGetExpireOption(3, out var additionExpireOption))
                    {
                        var optionStr = parseState.GetString(3);

                        while (!RespWriteUtils.WriteError($"ERR Unsupported option {optionStr}", ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }

                    if (expireOption == ExpireOption.XX && (additionExpireOption == ExpireOption.GT ||
                                                            additionExpireOption == ExpireOption.LT))
                    {
                        expireOption = ExpireOption.XX | additionExpireOption;
                    }
                    else if (expireOption == ExpireOption.GT && additionExpireOption == ExpireOption.XX)
                    {
                        expireOption = ExpireOption.XXGT;
                    }
                    else if (expireOption == ExpireOption.LT && additionExpireOption == ExpireOption.XX)
                    {
                        expireOption = ExpireOption.XXLT;
                    }
                    else
                    {
                        while (!RespWriteUtils.WriteError(
                                   "ERR NX and XX, GT or LT options at the same time are not compatible", ref dcurr,
                                   dend))
                            SendAndReset();
                    }
                }
            }

            var input = new RawStringInput(command, ref parseState, startIdx: 1, arg1: (byte)expireOption);
            var status = storageApi.EXPIRE(key, ref input, out var timeoutSet);

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
        /// Set a timeout on a key based on unix timestamp
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="command">Indicates which command to use, expire or pexpire.</param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool NetworkEXPIREAT<TGarnetApi>(RespCommand command, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            var count = parseState.Count;
            if (count < 2 || count > 4)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.EXPIREAT));
            }

            var key = parseState.GetArgSliceByRef(0);
            if (!parseState.TryGetLong(1, out _))
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var expireOption = ExpireOption.None;

            if (parseState.Count > 2)
            {
                if (!parseState.TryGetExpireOption(2, out expireOption))
                {
                    var optionStr = parseState.GetString(2);

                    while (!RespWriteUtils.WriteError($"ERR Unsupported option {optionStr}", ref dcurr, dend))
                        SendAndReset();
                    return true;
                }
            }

            if (parseState.Count > 3)
            {
                if (!parseState.TryGetExpireOption(3, out var additionExpireOption))
                {
                    var optionStr = parseState.GetString(3);

                    while (!RespWriteUtils.WriteError($"ERR Unsupported option {optionStr}", ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                if (expireOption == ExpireOption.XX && (additionExpireOption == ExpireOption.GT || additionExpireOption == ExpireOption.LT))
                {
                    expireOption = ExpireOption.XX | additionExpireOption;
                }
                else if (expireOption == ExpireOption.GT && additionExpireOption == ExpireOption.XX)
                {
                    expireOption = ExpireOption.XXGT;
                }
                else if (expireOption == ExpireOption.LT && additionExpireOption == ExpireOption.XX)
                {
                    expireOption = ExpireOption.XXLT;
                }
                else
                {
                    while (!RespWriteUtils.WriteError("ERR NX and XX, GT or LT options at the same time are not compatible", ref dcurr, dend))
                        SendAndReset();
                }
            }

            var input = new RawStringInput(command, ref parseState, startIdx: 1, arg1: (byte)expireOption);
            var status = storageApi.EXPIRE(key, ref input, out var timeoutSet);

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