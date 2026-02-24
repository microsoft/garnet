// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// RDB format version
        /// </summary>
        private readonly byte RDB_VERSION = 11;

        /// <summary>
        /// RESTORE
        /// </summary>
        /// <param name="storageApi"></param>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <returns></returns>
        bool NetworkRESTORE<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 3)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.RESTORE));
            }

            var key = parseState.GetArgSliceByRef(0);

            if (!parseState.TryGetInt(parseState.Count - 2, out var expiry))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_TIMEOUT_NOT_VALID_FLOAT);
            }

            var value = parseState.GetArgSliceByRef(2);

            var valueSpan = value.ReadOnlySpan;

            // Restore is only implemented for string type
            if (valueSpan[0] != 0x00)
            {
                while (!RespWriteUtils.TryWriteError("ERR RESTORE currently only supports string types", ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            // check if length of value is at least 10
            if (valueSpan.Length < 10)
            {
                while (!RespWriteUtils.TryWriteError("ERR DUMP payload version or checksum are wrong", ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            // get footer (2 bytes of rdb version + 8 bytes of crc)
            var footer = valueSpan[^10..];

            var rdbVersion = (footer[1] << 8) | footer[0];

            if (rdbVersion > RDB_VERSION)
            {
                while (!RespWriteUtils.TryWriteError("ERR DUMP payload version or checksum are wrong", ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (storeWrapper.serverOptions.SkipRDBRestoreChecksumValidation)
            {
                // crc is calculated over the encoded payload length, payload and the rdb version bytes
                // skip's the value type byte and crc64 bytes
                var calculatedCrc = new ReadOnlySpan<byte>(Crc64.Hash(valueSpan.Slice(0, valueSpan.Length - 8)));

                // skip's rdb version bytes
                var payloadCrc = footer[2..];

                if (calculatedCrc.SequenceCompareTo(payloadCrc) != 0)
                {
                    while (!RespWriteUtils.TryWriteError("ERR DUMP payload version or checksum are wrong", ref dcurr, dend))
                        SendAndReset();
                    return true;
                }
            }

            // decode the length of payload
            if (!RespLengthEncodingUtils.TryReadLength(valueSpan.Slice(1), out var length, out var payloadStart))
            {
                while (!RespWriteUtils.TryWriteError("ERR DUMP payload length format is invalid", ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            // Start from payload start and skip the value type byte
            var val = value.ReadOnlySpan.Slice(payloadStart + 1, length);

            var valArgSlice = scratchBufferBuilder.CreateArgSlice(val);

            parseState.InitializeWithArgument(valArgSlice);

            StringInput input;
            if (expiry > 0)
            {
                var inputArg = DateTimeOffset.UtcNow.Ticks + TimeSpan.FromSeconds(expiry).Ticks;
                input = new StringInput(RespCommand.SETEXNX, ref parseState, arg1: inputArg);
            }
            else
            {
                input = new StringInput(RespCommand.SETEXNX, ref parseState);
            }

            var status = storageApi.SET_Conditional(key, ref input);

            if (status is GarnetStatus.NOTFOUND)
            {
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_BUSSYKEY, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// DUMP 
        /// </summary>
        bool NetworkDUMP<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.DUMP));
            }

            var key = parseState.GetArgSliceByRef(0);

            var status = storageApi.GET(key, out PinnedSpanByte value);

            if (status is GarnetStatus.NOTFOUND)
            {
                WriteNull();
                return true;
            }

            Span<byte> encodedLength = stackalloc byte[5];

            if (!RespLengthEncodingUtils.TryWriteLength(value.ReadOnlySpan.Length, encodedLength, out var bytesWritten))
            {
                while (!RespWriteUtils.TryWriteError("ERR DUMP payload length is invalid", ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            encodedLength = encodedLength.Slice(0, bytesWritten);

            // Len of the dump (payload type + redis encoded payload len + payload len + rdb version + crc64)
            var len = 1 + encodedLength.Length + value.ReadOnlySpan.Length + 2 + 8;
            Span<byte> lengthInASCIIBytes = stackalloc byte[NumUtils.CountDigits(len)];
            var lengthInASCIIBytesLen = NumUtils.WriteInt64(len, lengthInASCIIBytes);

            // Total len (% + length of ascii bytes + CR LF + payload type + redis encoded payload len + payload len + rdb version + crc64 + CR LF)
            var totalLength = 1 + lengthInASCIIBytesLen + 2 + 1 + encodedLength.Length + value.ReadOnlySpan.Length + 2 + 8 + 2;

            byte[] rentedBuffer = null;
            var buffer = totalLength <= (dend - dcurr)
                ? new Span<byte>(dcurr, (int)(dend - dcurr))
                : (rentedBuffer = ArrayPool<byte>.Shared.Rent(totalLength));

            var offset = 0;

            // Write RESP bulk string prefix and length
            buffer[offset++] = 0x24; // '$'
            lengthInASCIIBytes.CopyTo(buffer[offset..]);
            offset += lengthInASCIIBytes.Length;
            buffer[offset++] = 0x0D; // CR
            buffer[offset++] = 0x0A; // LF

            // value type byte
            buffer[offset++] = 0x00;

            // length of the span
            encodedLength.CopyTo(buffer[offset..]);
            offset += encodedLength.Length;

            // copy value to buffer
            value.ReadOnlySpan.CopyTo(buffer[offset..]);
            offset += value.ReadOnlySpan.Length;

            // Write RDB version
            buffer[offset++] = (byte)(RDB_VERSION & 0xff);
            buffer[offset++] = (byte)((RDB_VERSION >> 8) & 0xff);

            // Compute and write CRC64 checksum
            var payloadToHash = buffer.Slice(1 + lengthInASCIIBytes.Length + 2 + 1,
                encodedLength.Length + value.ReadOnlySpan.Length + 2);

            var crcBytes = Crc64.Hash(payloadToHash);
            crcBytes.CopyTo(buffer[offset..]);
            offset += crcBytes.Length;

            // Write final CRLF
            buffer[offset++] = 0x0D; // CR
            buffer[offset] = 0x0A; // LF

            if (rentedBuffer is not null)
            {
                WriteDirectLarge(buffer.Slice(0, totalLength));
                ArrayPool<byte>.Shared.Return(rentedBuffer);
            }
            else
            {
                dcurr += totalLength;
            }

            return true;
        }

        /// <summary>
        /// TryRENAME
        /// </summary>
        private bool NetworkRENAME<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            // one optional command for with etag
            if (parseState.Count < 2 || parseState.Count > 3)
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.RENAME));

            var oldKeySlice = parseState.GetArgSliceByRef(0);
            var newKeySlice = parseState.GetArgSliceByRef(1);

            var withEtag = false;
            if (parseState.Count == 3)
            {
                if (!parseState.GetArgSliceByRef(2).ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.WITHETAG))
                    return AbortWithErrorMessage(string.Format(CmdStrings.GenericErrUnsupportedOption, parseState.GetString(2)));
                withEtag = true;
            }

            var status = storageApi.RENAME(oldKeySlice, newKeySlice, withEtag);

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
            // one optional command for with etag
            if (parseState.Count < 2 || parseState.Count > 3)
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.RENAMENX));

            var oldKeySlice = parseState.GetArgSliceByRef(0);
            var newKeySlice = parseState.GetArgSliceByRef(1);

            var withEtag = false;
            if (parseState.Count == 3)
            {
                if (!parseState.GetArgSliceByRef(2).ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.WITHETAG))
                    return AbortWithErrorMessage(string.Format(CmdStrings.GenericErrUnsupportedOption, parseState.GetString(2)));
                withEtag = true;
            }

            var status = storageApi.RENAMENX(oldKeySlice, newKeySlice, out var result, withEtag);

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
                    return AbortWithErrorMessage(string.Format(CmdStrings.GenericErrUnsupportedOption, parseState.GetString(2)));
                }

                if (parseState.Count > 3)
                {
                    if (!parseState.TryGetExpireOption(3, out var additionExpireOption))
                    {
                        return AbortWithErrorMessage(string.Format(CmdStrings.GenericErrUnsupportedOption, parseState.GetString(2)));
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
                        while (!RespWriteUtils.TryWriteError(
                                   "ERR NX and XX, GT or LT options at the same time are not compatible", ref dcurr,
                                   dend))
                            SendAndReset();
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