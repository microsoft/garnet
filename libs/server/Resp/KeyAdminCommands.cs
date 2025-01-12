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

            if (!parseState.TryGetDouble(parseState.Count - 2, out var ttl))
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_TIMEOUT_NOT_VALID_FLOAT, ref dcurr, dend))
                    SendAndReset();
                return true;
            }
            
            var value = parseState.GetArgSliceByRef(2);
            
            // return error if key already exists 
            var keyExists = storageApi.EXISTS(key);
            switch (keyExists)
            {
                case GarnetStatus.OK:
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERR_KEY_ALREADY_EXISTS, ref dcurr, dend))
                        SendAndReset();
                    return true;
            }
            
            var valueSpan = value.ReadOnlySpan;

            // Restore is only implemented for string type
            if (valueSpan[0] != 0x00)
            {
                while(!RespWriteUtils.WriteError("ERR RESTORE currently only supports string types", ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            // get footer (2 bytes of rdb version + 8 bytes of crc)
            var footer = valueSpan[^10..];

            var rdbVersion = (footer[1] << 8) | footer[0];

            if (rdbVersion > RDB_VERSION)
            {
                while(!RespWriteUtils.WriteError("ERR DUMP payload version or checksum are wrong", ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (storeWrapper.serverOptions.SkipChecksumValidation)
            {
                // crc is calculated over the encoded payload length, payload and the rdb version bytes
                // skip's the value type byte and crc64 bytes
                var calculatedCrc = new ReadOnlySpan<byte>(Crc64.Hash(valueSpan.Slice(0, valueSpan.Length - 8)));
                
                // skip's rdb version bytes
                var payloadCrc = footer[2..];

                if (calculatedCrc.SequenceCompareTo(payloadCrc) != 0)
                {
                    while(!RespWriteUtils.WriteError("ERR DUMP payload version or checksum are wrong", ref dcurr, dend))
                        SendAndReset();
                    return true;
                }
            }
            
            // decode the length of payload
            var (length, payloadStart) = RespLengthEncodingUtils.DecodeLength(ref valueSpan);
            
            // Start from payload start and skip the value type byte
            var val = value.ReadOnlySpan.Slice(payloadStart + 1, (int)length);
            
            var valArgSlice = scratchBufferManager.CreateArgSlice(val);

            if (ttl > 0)
            {
                storageApi.SETEX(key, valArgSlice, TimeSpan.FromMilliseconds(ttl));
            }
            else
            {
                storageApi.SET(key, valArgSlice);
            }
            
            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
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

            var status = storageApi.GET(key, out var value);

            switch (status)
            {
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                        SendAndReset();
                    return true;
            }
            
            var encodedLength = RespLengthEncodingUtils.EncodeLength(value.ReadOnlySpan.Length);
            
            // Len of the dump (payload type + redis encoded payload len + payload len + rdb version + crc64)
            var len = 1 + encodedLength.Length + value.ReadOnlySpan.Length + 2 + 8;
            var lengthInASCIIBytes = new Span<byte>(new byte[NumUtils.NumDigitsInLong(len)]);
            var lengthInASCIIBytesLen = NumUtils.LongToSpanByte(len, lengthInASCIIBytes);
            
            // Total len (% + length of ascii bytes + CR LF + payload type + redis encoded payload len + payload len + rdb version + crc64 + CR LF)
            var totalLength = 1 + lengthInASCIIBytesLen + 2 + 1 + encodedLength.Length + value.ReadOnlySpan.Length + 2 + 8 + 2;
            Span<byte> buffer = stackalloc byte[totalLength];
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
            foreach (var b in encodedLength)
                buffer[offset++] = b;
            
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
            buffer[offset++] = 0x0A; // LF

            while (!RespWriteUtils.WriteDirect(buffer, ref dcurr, dend))
                SendAndReset();
            
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