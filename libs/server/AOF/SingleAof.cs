// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    public sealed class SingleAof(TsavoriteLogSettings logSettings, ILogger logger = null) : IAppendOnlyFile
    {
        readonly TsavoriteLog log = new(logSettings, logger);

        public void Recover() => log.Recover();
        public void Reset() => log.Reset();
        public void Dispose() => log.Dispose();    

        public long BeginAddress => log.BeginAddress;
        public long TailAddress => log.TailAddress;
        public long CommittedUntilAddress => log.CommittedUntilAddress;
        public long MemorySizeBytes => log.MemorySizeBytes;
        public long CommittedBeginAddress => log.CommittedBeginAddress;
        public long FlushedUntilAddress => log.FlushedUntilAddress;
        public long HeaderSize => log.HeaderSize;
        public long MaxMemorySizeBytes => log.MaxMemorySizeBytes;

        public Action<long, long> SafeTailShiftCallback
        {
            get => log.SafeTailShiftCallback;
            set => log.SafeTailShiftCallback = value;
        }

        public TsavoriteLogScanIterator Scan(long beginAddress, long endAddress, bool recover = true, ScanBufferingMode scanBufferingMode = ScanBufferingMode.DoublePageBuffering, bool scanUncommitted = false, ILogger logger = null)
            => log.Scan(beginAddress, endAddress, recover, scanBufferingMode, scanUncommitted, logger);

        public TsavoriteLogScanSingleIterator ScanSingle(long beginAddress, long endAddress, bool recover = true, ScanBufferingMode scanBufferingMode = ScanBufferingMode.DoublePageBuffering, bool scanUncommitted = false, ILogger logger = null)
            => log.ScanSingle(beginAddress, endAddress, recover, scanBufferingMode, scanUncommitted, logger);

        public void Initialize(long beginAddress, long committedUntilAddress, long lastCommitNum = 0)
            => log.Initialize(beginAddress, committedUntilAddress, lastCommitNum);

        public void SafeInitialize(long beginAddress, long committedUntilAddress, long lastCommitNum = 0)
            => log.SafeInitialize(beginAddress, committedUntilAddress, lastCommitNum);

        public void Enqueue<THeader>(THeader userHeader, out long logicalAddress)
            where THeader : unmanaged
            => log.Enqueue(userHeader, out logicalAddress);

        public void Enqueue<THeader, TInput>(THeader userHeader, ref SpanByte item1, ref TInput input, out long logicalAddress)
            where THeader : unmanaged where TInput : IStoreInput
            => log.Enqueue(userHeader, ref item1, ref input, out logicalAddress);

        public void Enqueue<THeader>(THeader userHeader, ref SpanByte item1, ref SpanByte item2, out long logicalAddress)
            where THeader : unmanaged
            => log.Enqueue(userHeader, ref item1, ref item2, out logicalAddress);

        public void Enqueue<THeader, TInput>(THeader userHeader, ref SpanByte item1, ref SpanByte item2, ref TInput input, out long logicalAddress)
            where THeader : unmanaged where TInput : IStoreInput
            => log.Enqueue(userHeader, ref item1, ref item2, ref input, out logicalAddress);

        public void Enqueue<THeader, TInput>(THeader userHeader, ref TInput input, out long logicalAddress)
            where THeader : unmanaged where TInput : IStoreInput
            => log.Enqueue(userHeader, ref input, out logicalAddress);

        public unsafe int UnsafeGetLength(byte* headerPtr)
            => log.UnsafeGetLength(headerPtr);

        public void UnsafeCommitMetadataOnly(TsavoriteLogRecoveryInfo info, bool isProtected)
            => log.UnsafeCommitMetadataOnly(info, isProtected);

        public long UnsafeEnqueueRaw(ReadOnlySpan<byte> entryBytes, bool noCommit = false)
            => log.UnsafeEnqueueRaw(entryBytes, noCommit);

        public int UnsafeGetLogPageSizeBits()
            => log.UnsafeGetLogPageSizeBits();

        public long UnsafeGetReadOnlyAddressLagOffset()
            => log.UnsafeGetReadOnlyAddressLagOffset();

        public void UnsafeShiftBeginAddress(long untilAddress, bool snapToPageStart = false, bool truncateLog = false)
            => log.UnsafeShiftBeginAddress(untilAddress, snapToPageStart, truncateLog);

        public void TruncateUntil(long untilAddress) => log.TruncateUntil(untilAddress);

        public ValueTask CommitAsync(byte[] cookie = null, CancellationToken token = default)
            => log.CommitAsync(cookie, token);

        public void Commit(bool spinWait = false, byte[] cookie = null)
            => log.Commit(spinWait, cookie);

        public ValueTask WaitForCommitAsync(long untilAddress = 0, long commitNum = -1, CancellationToken token = default)
            => log.WaitForCommitAsync(untilAddress, commitNum, token);

        public void WaitForCommit(long untilAddress = 0, long commitNum = -1)
            => log.WaitForCommit(untilAddress, commitNum);
    }
}
