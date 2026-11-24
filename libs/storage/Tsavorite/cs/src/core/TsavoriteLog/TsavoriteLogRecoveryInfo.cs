// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO;

namespace Tsavorite.core
{
    /// <summary>
    /// Describes one <see cref="TsavoriteLog"/> commit. Its serialized form is stored in two places: in the log itself, as the
    /// commit record that fast-commit mode appends (an entry with a negative length), and in the commit-metadata file that
    /// <see cref="ILogCommitManager.Commit"/> writes for the commit's number. Recovery reads both.
    /// </summary>
    /// <remarks>
    /// Serialized as version, checksum, <see cref="BeginAddress"/>, <see cref="UntilAddress"/>, <see cref="CommitNum"/>, cookie
    /// length (-1 for none) and <see cref="Cookie"/>. The checksum is the XOR of the fields and of the cookie's bytes.
    /// </remarks>
    public struct TsavoriteLogRecoveryInfo
    {
        /// <summary>
        /// Serialized version written by this build. Version 0 is also read.
        /// </summary>
        const int TsavoriteLogRecoveryVersion = 1;

        /// <summary>
        /// Lowest valid address of the log as of this commit; the log below it may have been truncated. Serialized.
        /// </summary>
        public long BeginAddress;

        /// <summary>
        /// Address up to which the log is committed: in fast-commit mode the end of this commit's commit record, otherwise the
        /// log's tail when the commit was taken. Recovery restores the log's tail to this address. Serialized.
        /// </summary>
        public long UntilAddress;

        /// <summary>
        /// Caller-supplied data stored with the commit and returned by recovery as <see cref="TsavoriteLog.RecoveredCookie"/>.
        /// Null if the commit has none, in which case recovery returns the cookie of the latest earlier commit it read that has
        /// one. Serialized.
        /// </summary>
        public byte[] Cookie;

        /// <summary>
        /// Commit number: one more than the previous commit's, or a larger number proposed by the caller. It names the commit's
        /// commit-metadata file and selects the commit for <see cref="TsavoriteLog.RecoverAsync(long, System.Threading.CancellationToken)"/>.
        /// Recovery continues numbering from the recovered commit's number, and <see cref="TsavoriteLog.Initialize"/> and
        /// <see cref="TsavoriteLog.Reset"/> restart it, so later commits can reuse the numbers of commits that are no longer in
        /// the log. Serialized.
        /// </summary>
        public long CommitNum;

        /// <summary>
        /// Whether the commit may be skipped by the commit policy, and whether its commit-metadata file may be left to a later
        /// commit. True for commits made by <see cref="TsavoriteLog.Commit"/> or <c>CommitAsync</c> without a cookie; false for
        /// strong commits and for commits with a cookie, which are always passed to <see cref="ILogCommitManager.Commit"/>.
        /// Not serialized.
        /// </summary>
        public bool FastForwardAllowed;

        /// <summary>
        /// Invoked once the commit is durable. Not serialized.
        /// </summary>
        public Action Callback;

        /// <summary>
        /// Clear the addresses and the cookie. <see cref="CommitNum"/> is unchanged.
        /// </summary>
        public void Initialize()
        {
            BeginAddress = 0;
            UntilAddress = 0;
            Cookie = null;
        }


        /// <summary>
        /// Set the serialized fields from <paramref name="input"/>, which starts with a serialized
        /// <see cref="TsavoriteLogRecoveryInfo"/>; any bytes after it are ignored. If the input has no cookie,
        /// <see cref="Cookie"/> is left unchanged. Nothing is changed if this throws.
        /// </summary>
        /// <param name="input">Serialized bytes</param>
        /// <exception cref="TsavoriteException">The input is damaged or of an unknown version</exception>
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
        /// Same as <see cref="Initialize()"/>
        /// </summary>
        public void Reset()
        {
            Initialize();
        }

        /// <summary>
        /// Serialize the fields that are stored, as described on <see cref="TsavoriteLogRecoveryInfo"/>
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
        /// Length of <see cref="ToByteArray"/>'s result
        /// </summary>
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