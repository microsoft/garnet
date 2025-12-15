// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO;
using System.Text;

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
        /// callback to invoke when commit is presistent
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

            BeginAddress = BinaryPrimitives.ReadInt64LittleEndian(input);
            input = input.Slice(sizeof(long));

            UntilAddress = BinaryPrimitives.ReadInt64LittleEndian(input);
            input = input.Slice(sizeof(long));

            if (version > 0)
            {
                CommitNum = BinaryPrimitives.ReadInt64LittleEndian(input);
                input = input.Slice(sizeof(long));
            }
            else
            {
                CommitNum = -1;
            }

            if (version < 0 || version > TsavoriteLogRecoveryVersion)
                throw new TsavoriteException("Invalid version found during commit recovery");

            if (BinaryPrimitives.TryReadInt32LittleEndian(input, out var iteratorCount))
                input = input.Slice(sizeof(int));

            if (iteratorCount > 0)
            {
                for (var i = 0; i < iteratorCount; i++)
                {
                    var keyLength = BinaryPrimitives.ReadInt32LittleEndian(input);
                    input = input.Slice(sizeof(int));

                    var iteratorKey = Encoding.UTF8.GetString(input.Slice(0, keyLength));
                    input = input.Slice(keyLength);

                    var iteratorValue = BinaryPrimitives.ReadInt64LittleEndian(input);
                    input = input.Slice(sizeof(long));
                }
            }

            int cookieLength = -1;
            long cookieChecksum = 0;
            if (version >= TsavoriteLogRecoveryVersion)
            {
                if (BinaryPrimitives.TryReadInt32LittleEndian(input, out cookieLength))
                    input = input.Slice(sizeof(int));

                if (cookieLength >= 0)
                {
                    Cookie = input.Slice(0, cookieLength).ToArray();
                    unsafe
                    {
                        fixed (byte* ptr = Cookie)
                            cookieChecksum = (long)Utility.XorBytes(ptr, cookieLength);
                    }
                }
            }

            long computedChecksum = BeginAddress ^ UntilAddress;
            if (version >= TsavoriteLogRecoveryVersion)
                computedChecksum ^= CommitNum ^ iteratorCount ^ cookieLength ^ cookieChecksum;

            // Handle case where all fields are zero
            if (version == 0 && BeginAddress == 0 && UntilAddress == 0 && iteratorCount == 0)
                throw new TsavoriteException("Invalid checksum found during commit recovery");

            if (checkSum != computedChecksum)
                throw new TsavoriteException("Invalid checksum found during commit recovery");
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

                int iteratorCount = 0;
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

                writer.Write(BeginAddress ^ UntilAddress ^ CommitNum ^ iteratorCount ^ cookieLength ^ cookieChecksum); // checksum
                writer.Write(BeginAddress);
                writer.Write(UntilAddress);
                writer.Write(CommitNum);
                writer.Write(iteratorCount); // leaving this field for backwards compatibility
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
            return sizeof(int) + 4 * sizeof(long) + sizeof(int) + sizeof(int) + (Cookie?.Length ?? 0);
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