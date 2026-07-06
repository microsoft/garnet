// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO;

namespace Tsavorite.core
{
    /// <summary>
    /// Recovery info for TsavoriteLog
    /// </summary>
    public struct TsavoriteLogRecoveryInfo
    {
        /// <summary>
        /// TsavoriteLog recovery version
        /// </summary>
        const int TsavoriteLogRecoveryVersion = 1;

        /// <summary>
        /// Begin address
        /// </summary>
        public long BeginAddress;

        /// <summary>
        /// Flushed logical address
        /// </summary>
        public long UntilAddress;

        /// <summary>
        /// User-specified commit cookie
        /// </summary>
        public byte[] Cookie;

        /// <summary>
        /// commit num unique to this commit request
        /// </summary>
        public long CommitNum;

        /// <summary>
        /// whether this request is from a strong commit
        /// </summary>
        public bool FastForwardAllowed;

        /// <summary>
        /// callback to invoke when commit is persisted
        /// </summary>
        public Action Callback;

        /// <summary>
        /// Initialize
        /// </summary>
        public void Initialize()
        {
            BeginAddress = 0;
            UntilAddress = 0;
            Cookie = null;
        }


        /// <summary>
        /// Initialize from stream
        /// </summary>
        /// <param name="input"></param>
        public void Initialize(ReadOnlySpan<byte> input)
        {
            int version = BinaryPrimitives.ReadInt32LittleEndian(input);
            input = input.Slice(sizeof(int));

            long checkSum = BinaryPrimitives.ReadInt64LittleEndian(input);
            input = input.Slice(sizeof(long));

            long beginAddress = BinaryPrimitives.ReadInt64LittleEndian(input);
            input = input.Slice(sizeof(long));

            long untilAddress = BinaryPrimitives.ReadInt64LittleEndian(input);
            input = input.Slice(sizeof(long));

            long commitNum = BinaryPrimitives.ReadInt64LittleEndian(input);
            input = input.Slice(sizeof(long));

            if (version < 0 || version > TsavoriteLogRecoveryVersion)
                throw new TsavoriteException("Invalid version found during commit recovery");

            int cookieLength = -1;
            long cookieChecksum = 0;
            byte[] cookie = null;

            if (BinaryPrimitives.TryReadInt32LittleEndian(input, out cookieLength))
                input = input.Slice(sizeof(int));

            if (cookieLength >= 0)
            {
                cookie = input.Slice(0, cookieLength).ToArray();
                unsafe
                {
                    fixed (byte* ptr = cookie)
                        cookieChecksum = (long)Utility.XorBytes(ptr, cookieLength);
                }
            }

            long computedChecksum = beginAddress ^ untilAddress ^ commitNum ^ cookieLength ^ cookieChecksum;

            // Handle case where all fields are zero
            if (version == 0 && beginAddress == 0 && untilAddress == 0)
                throw new TsavoriteException("Invalid all-fields-zero found during commit recovery");

            if (checkSum != computedChecksum)
                throw new TsavoriteException("Invalid checksum found during commit recovery");

            // Validation succeeded: it is now safe to commit the parsed values to this struct.
            BeginAddress = beginAddress;
            UntilAddress = untilAddress;
            CommitNum = commitNum;
            if (cookieLength >= 0)
                Cookie = cookie;
        }

        /// <summary>
        /// Reset
        /// </summary>
        public void Reset()
        {
            Initialize();
        }

        /// <summary>
        /// Write info to byte array
        /// </summary>
        public readonly byte[] ToByteArray()
        {
            using MemoryStream ms = new();
            using (BinaryWriter writer = new(ms))
            {
                writer.Write(TsavoriteLogRecoveryVersion); // version

                int cookieLength = -1;
                long cookieChecksum = 0;
                if (Cookie != null)
                {
                    cookieLength = Cookie.Length;
                    if (cookieLength > 0)
                        unsafe
                        {
                            fixed (byte* ptr = Cookie)
                                cookieChecksum = (long)Utility.XorBytes(ptr, cookieLength);
                        }
                }

                writer.Write(BeginAddress ^ UntilAddress ^ CommitNum ^ cookieLength ^ cookieChecksum); // checksum
                writer.Write(BeginAddress);
                writer.Write(UntilAddress);
                writer.Write(CommitNum);
                writer.Write(cookieLength);
                if (cookieLength > 0)
                    writer.Write(Cookie);
            }
            return ms.ToArray();
        }

        /// <summary>
        /// </summary>
        /// <returns> size of this recovery info serialized </returns>
        public int SerializedSize()
        {
            return sizeof(int) + 4 * sizeof(long) + sizeof(int) + (Cookie?.Length ?? 0);
        }

        /// <summary>
        /// Print checkpoint info for debugging purposes
        /// </summary>
        public void DebugPrint()
        {
            Debug.WriteLine("******** Log Commit Info ********");

            Debug.WriteLine("BeginAddress: {0}", BeginAddress);
            Debug.WriteLine("FlushedUntilAddress: {0}", UntilAddress);
        }
    }
}