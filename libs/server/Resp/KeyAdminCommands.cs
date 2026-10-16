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
        /// RESTORE
        /// </summary>
        bool NetworkRESTORE<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 3)
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.RESTORE));

            if (!parseState.TryGetLong(1, out var ttl))
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
            if (ttl < 0)
                return AbortWithErrorMessage("ERR Invalid TTL value, must be >= 0"u8);

            if (!RdbSerializer.TryDeserialize(parseState.GetArgSliceByRef(2).ReadOnlySpan,
                out var value, out var obj, out var error))
                return AbortWithErrorMessage(error);

            try
            {
                if (obj is not null && storeWrapper.serverOptions.DisableObjects)
                    return AbortWithErrorMessage("ERR Object store is disabled"u8);

                var now = DateTimeOffset.UtcNow.Ticks;
                if (ttl > (DateTime.MaxValue.Ticks - now) / TimeSpan.TicksPerMillisecond)
                    return AbortWithErrorMessage("ERR Invalid TTL value, deadline is out of range"u8);
                var expiration = ttl == 0 ? 0 : now + ttl * TimeSpan.TicksPerMillisecond;

                var key = parseState.GetArgSliceByRef(0);
                GarnetStatus status;
                if (obj is null)
                {
                    fixed (byte* valuePointer = value)
                    {
                        parseState.InitializeWithArgument(PinnedSpanByte.FromPinnedPointer(valuePointer, value.Length));
                        var input = new StringInput(RespCommand.SETEXNX, ref parseState, arg1: expiration);
                        status = storageApi.SET_Conditional(key, ref input);
                        if (status == GarnetStatus.WRONGTYPE)
                            status = storageApi.RESTORE(key, value, null, expiration);
                    }
                }
                else
                {
                    status = storageApi.RESTORE(key, default, obj, expiration);
                }
                if (status == GarnetStatus.NOTFOUND)
                {
                    obj = null;
                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_BUSSYKEY, ref dcurr, dend))
                        SendAndReset();
                }
                return true;
            }
            finally
            {
                obj?.Dispose();
            }
        }

        /// <summary>
        /// DUMP
        /// </summary>
        bool NetworkDUMP<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 1)
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.DUMP));

            var output = GetUnifiedOutput();
            var status = storageApi.DUMP(parseState.GetArgSliceByRef(0), ref output);
            if (status == GarnetStatus.OK)
                ProcessOutput(output.SpanByteAndMemory);
            else
            {
                output.Dispose();
                WriteNull();
            }
            return true;
        }

        /// <summary>
        /// TryRENAME
        /// </summary>
        private bool NetworkRENAME<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 2)
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.RENAME));

            var oldKeySlice = parseState.GetArgSliceByRef(0);
            var newKeySlice = parseState.GetArgSliceByRef(1);

            var status = storageApi.RENAME(oldKeySlice, newKeySlice);

            switch (status)
            {
                case GarnetStatus.OK:
                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_NOSUCHKEY, ref dcurr, dend))
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
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.RENAMENX));

            var oldKeySlice = parseState.GetArgSliceByRef(0);
            var newKeySlice = parseState.GetArgSliceByRef(1);

            var status = storageApi.RENAMENX(oldKeySlice, newKeySlice, out var result);

            if (status == GarnetStatus.OK)
            {
                // Integer reply: 1 if key was renamed to newkey.
                // Integer reply: 0 if newkey already exists.
                while (!RespWriteUtils.TryWriteInt32(result, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_NOSUCHKEY, ref dcurr, dend))
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

            var sbKey = parseState.GetArgSliceByRef(0);
            var output = GetStringOutput();
            var status = garnetApi.GETDEL(sbKey, ref output);

            if (status == GarnetStatus.OK)
            {
                ProcessOutput(output.SpanByteAndMemory);
            }
            else if (status == GarnetStatus.WRONGTYPE)
            {
                WriteError(CmdStrings.RESP_ERR_WRONG_TYPE);
            }
            else
            {
                Debug.Assert(output.SpanByteAndMemory.IsSpanByte);
                WriteNull();
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

            // Prepare input
            var input = new UnifiedInput(RespCommand.EXISTS);

            var output = new UnifiedOutput();

            for (var i = 0; i < parseState.Count; i++)
            {
                var key = parseState.GetArgSliceByRef(i);
                var status = storageApi.EXISTS(key, ref input, ref output);
                if (status == GarnetStatus.OK)
                    exists++;
            }

            while (!RespWriteUtils.TryWriteInt32(exists, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Set a timeout on a key.
        /// EXPIRE key seconds [NX | XX | GT | LT]
        /// PEXPIRE key milliseconds [NX | XX | GT | LT]
        /// EXPIREAT key unix-time-seconds [NX | XX | GT | LT]
        /// PEXPIREAT key unix-time-milliseconds [NX | XX | GT | LT]
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="command">Indicates which command to use.</param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool NetworkEXPIRE<TGarnetApi>(RespCommand command, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            var count = parseState.Count;
            if (count < 2 || count > 4)
            {
                return AbortWithWrongNumberOfArguments(command.ToString());
            }

            var key = parseState.GetArgSliceByRef(0);

            if (!parseState.TryGetLong(1, out var expiration))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
            }

            if (expiration < 0)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_INVALID_EXPIRE_TIME);
            }

            var expireOption = ExpireOption.None;
            if (parseState.Count > 2)
            {
                if (!parseState.TryGetExpireOption(2, out expireOption))
                {
                    return AbortWithErrorMessage(CmdStrings.GenericErrUnsupportedOption, parseState.GetString(2));
                }

                if (parseState.Count > 3)
                {
                    if (!parseState.TryGetExpireOption(3, out var additionExpireOption))
                    {
                        return AbortWithErrorMessage(CmdStrings.GenericErrUnsupportedOption, parseState.GetString(3));
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
                        return AbortWithErrorMessage("ERR NX and XX, GT or LT options at the same time are not compatible"u8);
                    }
                }
            }

            // Convert to expiration time in ticks
            var expirationTimeInTicks = command switch
            {
                RespCommand.EXPIRE => DateTimeOffset.UtcNow.AddSeconds(expiration).UtcTicks,
                RespCommand.PEXPIRE => DateTimeOffset.UtcNow.AddMilliseconds(expiration).UtcTicks,
                RespCommand.EXPIREAT => ConvertUtils.UnixTimestampInSecondsToTicks(expiration),
                _ => ConvertUtils.UnixTimestampInMillisecondsToTicks(expiration)
            };

            // Encode expiration time and expiration option and pass them into the input object
            var expirationWithOption = new ExpirationWithOption(expirationTimeInTicks, expireOption);

            var input = new UnifiedInput(RespCommand.EXPIRE, arg1: expirationWithOption.Word);

            // Prepare UnifiedOutput output
            var output = GetUnifiedOutput();

            var status = storageApi.EXPIRE(key, ref input, ref output);

            if (status == GarnetStatus.OK)
            {
                ProcessOutput(output.SpanByteAndMemory);
            }
            else
            {
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
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

            // Prepare input
            var input = new UnifiedInput(RespCommand.PERSIST);

            // Prepare UnifiedOutput output
            var output = GetUnifiedOutput();

            var status = storageApi.PERSIST(key, ref input, ref output);

            if (status == GarnetStatus.OK)
            {
                ProcessOutput(output.SpanByteAndMemory);
            }
            else
            {
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
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
                return AbortWithWrongNumberOfArguments(command.ToString());
            }

            var key = parseState.GetArgSliceByRef(0);

            // Prepare input
            var input = new UnifiedInput(command);

            // Prepare UnifiedOutput output
            var output = GetUnifiedOutput();

            var status = storageApi.TTL(key, ref input, ref output);

            if (status == GarnetStatus.OK)
            {
                ProcessOutput(output.SpanByteAndMemory);
            }
            else
            {
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_RETURN_VAL_N2, ref dcurr, dend))
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

            var key = parseState.GetArgSliceByRef(0);

            // Prepare input
            var input = new UnifiedInput(command);

            // Prepare UnifiedOutput output
            var output = GetUnifiedOutput();

            var status = storageApi.EXPIRETIME(key, ref input, ref output);

            if (status == GarnetStatus.OK)
            {
                ProcessOutput(output.SpanByteAndMemory);
            }
            else
            {
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_RETURN_VAL_N2, ref dcurr, dend))
                    SendAndReset();
            }
            return true;
        }
    }
}
