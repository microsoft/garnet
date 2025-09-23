// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    public interface IAppendOnlyFile
    {
        long BeginAddress { get; }
        long TailAddress { get; }
        long CommittedUntilAddress { get; }

        long MemorySizeBytes { get; }

        long MaxMemorySizeBytes { get; }

        long CommittedBeginAddress { get; }

        long FlushedUntilAddress { get; }

        long HeaderSize { get; }

        Action<long, long> SafeTailShiftCallback { get; set; }

        void Recover();
        void Reset();
        void Dispose();

        TsavoriteLogScanIterator Scan(long beginAddress, long endAddress, bool recover = true, ScanBufferingMode scanBufferingMode = ScanBufferingMode.DoublePageBuffering, bool scanUncommitted = false, ILogger logger = null);

        TsavoriteLogScanSingleIterator ScanSingle(long beginAddress, long endAddress, bool recover = true, ScanBufferingMode scanBufferingMode = ScanBufferingMode.DoublePageBuffering, bool scanUncommitted = false, ILogger logger = null);

        void Initialize(long beginAddress, long committedUntilAddress, long lastCommitNum = 0);

        void SafeInitialize(long beginAddress, long committedUntilAddress, long lastCommitNum = 0);

        unsafe int UnsafeGetLength(byte* headerPtr);

        void UnsafeCommitMetadataOnly(TsavoriteLogRecoveryInfo info, bool isProtected);

        long UnsafeEnqueueRaw(ReadOnlySpan<byte> entryBytes, bool noCommit = false);

        int UnsafeGetLogPageSizeBits();

        long UnsafeGetReadOnlyAddressLagOffset();

        void UnsafeShiftBeginAddress(long untilAddress, bool snapToPageStart = false, bool truncateLog = false);

        void Enqueue<THeader>(THeader userHeader, out long logicalAddress)
            where THeader : unmanaged;

        void Enqueue<THeader, TInput>(THeader userHeader, ref SpanByte item1, ref TInput input, out long logicalAddress)
            where THeader : unmanaged where TInput : IStoreInput;

        void Enqueue<THeader>(THeader userHeader, ref SpanByte item1, ref SpanByte item2, out long logicalAddress)
            where THeader : unmanaged;

        void Enqueue<THeader, TInput>(THeader userHeader, ref SpanByte item1, ref SpanByte item2, ref TInput input, out long logicalAddress)
            where THeader : unmanaged where TInput : IStoreInput;

        void Enqueue<THeader, TInput>(THeader userHeader, ref TInput input, out long logicalAddress)
            where THeader : unmanaged where TInput : IStoreInput;

        void TruncateUntil(long untilAddress);

        ValueTask CommitAsync(byte[] cookie = null, CancellationToken token = default);

        void Commit(bool spinWait = false, byte[] cookie = null);

        ValueTask WaitForCommitAsync(long untilAddress = 0, long commitNum = -1, CancellationToken token = default);

        void WaitForCommit(long untilAddress = 0, long commitNum = -1);
    }
}
