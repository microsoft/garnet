// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {

        /// <summary>
        /// TryRENAME
        /// </summary>
        private bool NetworkRENAME<TGarnetApi>(byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            ptr += 16;

            byte* key1Ptr = null, key2Ptr = null;
            int ksize1 = 0, ksize2 = 0;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref key1Ptr, ref ksize1, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref key2Ptr, ref ksize2, ref ptr, recvBufferPtr + bytesRead))
                return false;
            readHead = (int)(ptr - recvBufferPtr);

            if (NetworkSingleKeySlotVerify(key1Ptr, ksize1, false) || NetworkSingleKeySlotVerify(key2Ptr, ksize2, false))
                return true;

            ArgSlice oldKeySlice = new(key1Ptr, ksize1);
            ArgSlice newKeySlice = new(key2Ptr, ksize2);

            var status = storageApi.RENAME(oldKeySlice, newKeySlice);

            switch (status)
            {
                case GarnetStatus.OK:
                    while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_ERRNOSUCHKEY, ref dcurr, dend))
                        SendAndReset();
                    break;
            }
            return true;
        }

        /// <summary>
        /// DEL
        /// </summary>
        private bool NetworkDEL<TGarnetApi>(byte* ptr, ref TGarnetApi garnetApi)
            where TGarnetApi : IGarnetApi
        {
            ptr += 13;

            byte* keyPtr = null;
            int ksize = 0;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                return false;
            readHead = (int)(ptr - recvBufferPtr);

            if (NetworkSingleKeySlotVerify(keyPtr, ksize, false))
                return true;

            var key = new Span<byte>(keyPtr, ksize);
            keyPtr -= sizeof(int);
            *(int*)keyPtr = ksize;

            var status = garnetApi.DELETE(ref Unsafe.AsRef<SpanByte>(keyPtr), StoreType.All);

            // This is only an approximate return value because the deletion of a key on disk is performed as a blind tombstone append
            if (status == GarnetStatus.OK)
            {
                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_RETURN_VAL_1, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                    SendAndReset();
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
            ptr += 16;

            byte* keyPtr = null;
            int ksize = 0;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                return false;
            readHead = (int)(ptr - recvBufferPtr);

            if (NetworkSingleKeySlotVerify(keyPtr, ksize, false))
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
                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// EXISTS multiple keys
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool NetworkEXISTS<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            ptr += 12;
            int exists = 0;

            for (int i = 0; i < count - 1; i++)
            {
                byte* keyPtr = null;
                int ksize = 0;

                if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                if (NetworkSingleKeySlotVerify(keyPtr, ksize, true))
                    return true;

                ArgSlice key = new(keyPtr, ksize);
                var status = storageApi.EXISTS(key);
                if (status == GarnetStatus.OK)
                    exists++;
            }

            while (!RespWriteUtils.WriteInteger(exists, ref dcurr, dend))
                SendAndReset();

            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }


        /// <summary>
        /// Exists RESP command
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="ptr">Reading pointer to network buffer</param>
        /// <param name="storageApi">Garnet API instance</param>
        /// <returns></returns>
        private bool NetworkEXISTS<TGarnetApi>(byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            ptr += 16;

            byte* keyPtr = null;
            int ksize = 0;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                return false;
            readHead = (int)(ptr - recvBufferPtr);

            if (NetworkSingleKeySlotVerify(keyPtr, ksize, true))
                return true;

            ArgSlice key = new(keyPtr, ksize);
            var status = storageApi.EXISTS(key);

            if (status == GarnetStatus.OK)
            {
                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_RETURN_VAL_1, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                    SendAndReset();
            }
            return true;
        }

        /// <summary>
        /// Set a timeout on a key.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="ptr"></param>
        /// <param name="command">indicates which command to use, expire or pexpire</param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool NetworkEXPIRE<TGarnetApi>(byte* ptr, RespCommand command, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            int count = *(ptr + 1) - '0';
            ptr += command == RespCommand.EXPIRE ? 16 : 17;

            byte* keyPtr = null;
            int ksize = 0;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (!RespReadUtils.ReadIntWithLengthHeader(out int expiryValue, ref ptr, recvBufferPtr + bytesRead))
                return false;

            var expiryMs = command == RespCommand.EXPIRE ? TimeSpan.FromSeconds(expiryValue) : TimeSpan.FromMilliseconds(expiryValue);

            bool invalidOption = false;
            ExpireOption expireOption = ExpireOption.None;
            string optionStr = "";
            if (count > 3)
            {
                if (!RespReadUtils.ReadStringWithLengthHeader(out optionStr, ref ptr, recvBufferPtr + bytesRead))
                    return false;

                optionStr = optionStr.ToUpper();
                try
                {
                    expireOption = (ExpireOption)Enum.Parse(typeof(ExpireOption), optionStr);
                }
                catch
                {
                    invalidOption = true;
                }
            }
            readHead = (int)(ptr - recvBufferPtr);

            if (invalidOption)
            {
                while (!RespWriteUtils.WriteResponse(new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR Unsupported option {optionStr}\r\n")), ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (NetworkSingleKeySlotVerify(keyPtr, ksize, false))
                return true;

            var key = new ArgSlice(keyPtr, ksize);
            var status = command == RespCommand.EXPIRE ?
                        storageApi.EXPIRE(key, expiryMs, out bool timeoutSet, StoreType.All, expireOption) :
                        storageApi.PEXPIRE(key, expiryMs, out timeoutSet, StoreType.All, expireOption);

            if (status == GarnetStatus.OK && timeoutSet)
            {
                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_RETURN_VAL_1, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// PERSIST command
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="ptr">Reading pointer to the buffer</param>
        /// <param name="storageApi">The Garnet API instance</param>
        /// <returns></returns>
        private bool NetworkPERSIST<TGarnetApi>(byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            ptr += 17;
            byte* keyPtr = null;
            int ksize = 0;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                return false;
            readHead = (int)(ptr - recvBufferPtr);

            if (NetworkSingleKeySlotVerify(keyPtr, ksize, true))
                return true;

            var key = new ArgSlice(keyPtr, ksize);
            var status = storageApi.PERSIST(key);

            if (status == GarnetStatus.OK)
            {
                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_RETURN_VAL_1, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
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
            ptr += command == RespCommand.TTL ? 13 : 14;
            byte* keyPtr = null;
            int ksize = 0;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                return false;
            readHead = (int)(ptr - recvBufferPtr);

            if (NetworkSingleKeySlotVerify(keyPtr, ksize, true))
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
                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_RETURN_VAL_N1, ref dcurr, dend))
                    SendAndReset();
            }
            return true;
        }
    }
}