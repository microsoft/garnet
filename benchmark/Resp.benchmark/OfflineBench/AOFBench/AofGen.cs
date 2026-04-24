// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Text;
using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Resp.benchmark
{
    public class Page(int size)
    {
        public int Length => payload.Length;
        public byte[] payload = GC.AllocateArray<byte>(size, pinned: true);
        public int payloadLength = 0;
        public int recordCount = 0;
    }

    public class RespReplayMessage(int size)
    {
        public byte[] buffer = GC.AllocateArray<byte>(size, pinned: true);
        public int messageOffset = 0;
        public int messageLength = 0;
        public int payloadOffset = 0;
        public int payloadLength = 0;
        public int recordCount = 0;

        // Byte offsets into buffer where fixed-width address digit fields begin.
        // Used for in-place patching of addresses during circular replay.
        public int previousAddressDigitOffset;
        public int currentAddressDigitOffset;
        public int nextAddressDigitOffset;
    }

    public sealed class AofGen
    {
        /// <summary>
        /// Maximum size of the RESP header for CLUSTER APPENDLOG (args 1-7 + bulk string header).
        /// Generous upper bound to accommodate fixed-width address fields and nodeId.
        /// </summary>
        const int MaxRespHeaderSize = 512;

        /// <summary>
        /// Maximum digits for a non-negative long address value (long.MaxValue = 19 digits).
        /// Addresses are always zero-padded to this width to allow in-place patching during circular replay.
        /// </summary>
        const int MaxAddressDigits = 19;
        readonly GarnetLog garnetLog;

        /// <summary>
        /// Writes a complete RESP-formatted CLUSTER APPENDLOG message including the payload.
        /// </summary>
        internal static unsafe int WriterClusterAppendLog(
            byte* bufferPtr,
            int bufferLength,
            string nodeId,
            int physicalSublogIdx,
            long previousAddress,
            long currentAddress,
            long nextAddress,
            long payloadPtr,
            int payloadLength)
        {
            var CLUSTER = "$7\r\nCLUSTER\r\n"u8;
            var appendLog = "APPENDLOG"u8;

            var curr = bufferPtr;
            var end = bufferPtr + bufferLength;

            var arraySize = 8;

            // 
            if (!RespWriteUtils.TryWriteArrayLength(arraySize, ref curr, end))
                throw new GarnetException("Not enough space in buffer");
            // 1
            if (!RespWriteUtils.TryWriteDirect(CLUSTER, ref curr, end))
                throw new GarnetException("Not enough space in buffer");
            // 2
            if (!RespWriteUtils.TryWriteBulkString(appendLog, ref curr, end))
                throw new GarnetException("Not enough space in buffer");
            // 3
            if (!RespWriteUtils.TryWriteAsciiBulkString(nodeId, ref curr, end))
                throw new GarnetException("Not enough space in buffer");
            // 4
            if (!RespWriteUtils.TryWriteArrayItem(physicalSublogIdx, ref curr, end))
                throw new GarnetException("Not enough space in buffer");
            // 5
            if (!RespWriteUtils.TryWriteArrayItem(previousAddress, ref curr, end))
                throw new GarnetException("Not enough space in buffer");
            // 6
            if (!RespWriteUtils.TryWriteArrayItem(currentAddress, ref curr, end))
                throw new GarnetException("Not enough space in buffer");
            // 7
            if (!RespWriteUtils.TryWriteArrayItem(nextAddress, ref curr, end))
                throw new GarnetException("Not enough space in buffer");
            // 8
            if (!RespWriteUtils.TryWriteBulkString(new Span<byte>((void*)payloadPtr, payloadLength), ref curr, end))
                throw new GarnetException("Not enough space in buffer");

            return (int)(curr - bufferPtr);
        }

        /// <summary>
        /// Like <see cref="WriterClusterAppendLog"/> but allocates maximum space for address values
        /// (zero-padded to <see cref="MaxAddressDigits"/> digits) so they can be patched in-place
        /// during circular replay. Records the digit offsets in <paramref name="msg"/>.
        /// </summary>
        internal static unsafe int WriterClusterAppendLogFixedWidth(
            byte* bufferPtr,
            int bufferLength,
            string nodeId,
            int physicalSublogIdx,
            long previousAddress,
            long currentAddress,
            long nextAddress,
            long payloadPtr,
            int payloadLength,
            RespReplayMessage msg)
        {
            var CLUSTER = "$7\r\nCLUSTER\r\n"u8;
            var appendLog = "APPENDLOG"u8;

            var curr = bufferPtr;
            var end = bufferPtr + bufferLength;

            if (!RespWriteUtils.TryWriteArrayLength(8, ref curr, end))
                throw new GarnetException("Not enough space in buffer");
            if (!RespWriteUtils.TryWriteDirect(CLUSTER, ref curr, end))
                throw new GarnetException("Not enough space in buffer");
            if (!RespWriteUtils.TryWriteBulkString(appendLog, ref curr, end))
                throw new GarnetException("Not enough space in buffer");
            if (!RespWriteUtils.TryWriteAsciiBulkString(nodeId, ref curr, end))
                throw new GarnetException("Not enough space in buffer");
            if (!RespWriteUtils.TryWriteArrayItem(physicalSublogIdx, ref curr, end))
                throw new GarnetException("Not enough space in buffer");

            // Write address fields with fixed-width zero-padded digits to allow in-place patching
            msg.previousAddressDigitOffset = WriteFixedWidthAddress(previousAddress, ref curr, end, bufferPtr);
            msg.currentAddressDigitOffset = WriteFixedWidthAddress(currentAddress, ref curr, end, bufferPtr);
            msg.nextAddressDigitOffset = WriteFixedWidthAddress(nextAddress, ref curr, end, bufferPtr);

            if (!RespWriteUtils.TryWriteBulkString(new Span<byte>((void*)payloadPtr, payloadLength), ref curr, end))
                throw new GarnetException("Not enough space in buffer");

            return (int)(curr - bufferPtr);
        }

        /// <summary>
        /// Writes a long as a fixed-width RESP bulk string: $19\r\n{zero-padded digits}\r\n.
        /// Returns the byte offset from <paramref name="bufferStart"/> where the digit field begins.
        /// </summary>
        static unsafe int WriteFixedWidthAddress(long value, ref byte* curr, byte* end, byte* bufferStart)
        {
            // "$19\r\n" (5 bytes) + 19 digits + "\r\n" (2 bytes) = 26 bytes
            const int totalLen = 5 + MaxAddressDigits + 2;
            if (totalLen > (int)(end - curr))
                throw new GarnetException("Not enough space in buffer");

            *curr++ = (byte)'$';
            *curr++ = (byte)'1';
            *curr++ = (byte)'9';
            *curr++ = (byte)'\r';
            *curr++ = (byte)'\n';

            var digitOffset = (int)(curr - bufferStart);
            WriteZeroPaddedInt64(value, curr, MaxAddressDigits);
            curr += MaxAddressDigits;

            *curr++ = (byte)'\r';
            *curr++ = (byte)'\n';

            return digitOffset;
        }

        /// <summary>
        /// Overwrites a fixed-width address digit field in-place during circular replay.
        /// </summary>
        internal static unsafe void PatchAddress(byte* buffer, int digitOffset, long value)
        {
            WriteZeroPaddedInt64(value, buffer + digitOffset, MaxAddressDigits);
        }

        static unsafe void WriteZeroPaddedInt64(long value, byte* dest, int width)
        {
            for (var i = width - 1; i >= 0; i--)
            {
                dest[i] = (byte)('0' + (value % 10));
                value /= 10;
            }
        }

        public readonly GarnetAppendOnlyFile appendOnlyFile;

        readonly Options options;
        readonly GarnetServerOptions aofServerOptions;

        /// <summary>
        /// threads x pageNum
        /// </summary>
        Page[][] pageBuffers;

        /// <summary>
        /// threads x pageNum (RESP-formatted replay messages, only for AofBenchType.Replay)
        /// </summary>
        RespReplayMessage[][] respReplayMessageBuffers;

        /// <summary>
        /// Primary node ID used for generating RESP replay messages
        /// </summary>
        internal string primaryId;

        /// <summary>
        /// DBSize kv pairs
        /// </summary>
        List<(byte[], byte[])>[] kvPairBuffers;

        long total_number_of_aof_records = 0L;
        long total_number_of_aof_bytes = 0L;

        public Page[] GetPageBuffers(int threadIdx) => pageBuffers[threadIdx];
        public RespReplayMessage[] GetRespReplayMessages(int threadIdx) => respReplayMessageBuffers[threadIdx];
        public List<(byte[], byte[])> GetKVPairBuffer(int threadIdx) => kvPairBuffers[threadIdx];

        public AofGen(Options options)
        {
            this.options = options;
            this.aofServerOptions = new GarnetServerOptions()
            {
                EnableAOF = true,
                AofMemorySize = options.AofMemorySize,
                AofPageSize = options.AofPageSize,
                UseAofNullDevice = true,
                EnableFastCommit = true,
                CommitFrequencyMs = -1,
                FastAofTruncate = true,
                EnableCluster = true,
                ReplicationOffsetMaxLag = 0,
                AofPhysicalSublogCount = options.AofPhysicalSublogCount
            };
            aofServerOptions.GetAofSettings(0, out var logSettings);
            appendOnlyFile = new GarnetAppendOnlyFile(aofServerOptions, logSettings, Program.loggerFactory.CreateLogger("AofGen - AOF instance"));
            garnetLog = appendOnlyFile.Log;

            if (options.IsReplayEnabled)
            {
                if (options.AofBenchType == AofBenchType.Replay)
                {
                    respReplayMessageBuffers = new RespReplayMessage[options.AofPhysicalSublogCount][];
                }
                else
                {
                    pageBuffers = new Page[options.AofPhysicalSublogCount][];
                }
            }
            else
            {
                kvPairBuffers = new List<(byte[], byte[])>[options.NumThreads.Max()];
            }

            if (options.AofPhysicalSublogCount != options.NumThreads.Max() && options.AofBenchType == AofBenchType.EnqueueSharded)
                throw new Exception("Use --threads(MAX)== --aof-sublog-count to generated perfectly sharded data!");
        }

        byte[] GetKey() => Encoding.ASCII.GetBytes(Generator.CreateHexId(size: Math.Max(options.KeyLength, 8)));

        byte[] GetKey(int threadId)
        {
            while (true)
            {
                var keyData = Encoding.ASCII.GetBytes(Generator.CreateHexId(size: Math.Max(options.KeyLength, 8)));
                var physicalSublogIdx = garnetLog.GetPhysicalSublogIdx(keyData);
                if (physicalSublogIdx == threadId) return keyData;
            }
        }

        byte[] GetValue() => Encoding.ASCII.GetBytes(Generator.CreateHexId(size: Math.Max(options.ValueLength, 8)));

        List<(byte[], byte[])> GenerateKVPairs(int threadId, bool random)
        {
            var kvPairs = new List<(byte[], byte[])>();

            for (var i = 0; i < options.DbSize; i++)
            {
                var key = random ? GetKey() : GetKey(threadId);
                var value = GetValue();
                kvPairs.Add((key, value));
            }
            return kvPairs;
        }

        public void GenerateData()
        {
            Console.WriteLine($"Generating AofBench Data!");
            var threads = options.IsReplayEnabled ? options.AofPhysicalSublogCount : options.NumThreads.Max();
            var workers = new Thread[threads];

            // Run the experiment.
            for (var idx = 0; idx < threads; ++idx)
            {
                var x = idx;
                workers[idx] = options.AofBenchType switch
                {
                    AofBenchType.Replay or AofBenchType.ReplayNoResp or AofBenchType.ReplayDirect => new Thread(() => GeneratePages(x)),
                    AofBenchType.EnqueueSharded or AofBenchType.EnqueueRandom => new Thread(() => GenerateKeys(x)),
                    _ => throw new Exception($"AofBenchType {options.AofBenchType} not supported"),
                };
            }

            Stopwatch swatch = new();
            swatch.Start();

            // Start threads.
            foreach (var worker in workers)
                worker.Start();

            // Wait for workers to complete
            foreach (var worker in workers)
                worker.Join();

            swatch.Stop();

            var seconds = swatch.ElapsedMilliseconds / 1000.0;
            if (options.IsReplayEnabled)
            {
                Console.WriteLine($"Generated {threads}x{options.AofGenPages} pages of size {aofServerOptions.AofPageSize} in {seconds:N2} secs");
                Console.WriteLine($"Generated number of AOF records: {total_number_of_aof_records:N0}");
                Console.WriteLine($"Generated number of AOF bytes: {total_number_of_aof_bytes:N0}");
            }
            else
            {
                Console.WriteLine($"Generated {threads}x{options.DbSize} KV pairs in {seconds:N2} secs");
            }
        }

        unsafe void GeneratePages(int threadId)
        {
            var seqNumGen = new SequenceNumberGenerator(0);
            var number_of_aof_records = 0L;
            var number_of_aof_bytes = 0L;
            var kvPairs = GenerateKVPairs(threadId, options.AofPhysicalSublogCount == 1);
            var pages = options.AofGenPages;
            var pageSize = 1 << aofServerOptions.AofPageSizeBits();
            var generateResp = options.AofBenchType == AofBenchType.Replay;

            if (generateResp)
            {
                respReplayMessageBuffers[threadId] = new RespReplayMessage[pages];

                // Simulate address progression matching RunAofReplayBench
                var previousAddress = 64L;
                var currentAddress = 64L;

                // Temp page used to fill data, then copied into the final RESP message
                var tempPage = new Page(pageSize);

                for (var i = 0; i < pages; i++)
                {
                    // Fill page data into temp buffer to determine payload length
                    tempPage.payloadLength = 0;
                    tempPage.recordCount = 0;
                    FillPage(threadId, kvPairs, i, tempPage);

                    var nextAddress = currentAddress + tempPage.payloadLength;

                    // Allocate RESP message buffer: header overhead + payload + trailing \r\n
                    var respMessage = new RespReplayMessage(MaxRespHeaderSize + tempPage.payloadLength + 2);

                    fixed (byte* bufferPtr = respMessage.buffer)
                    fixed (byte* payloadPtr = tempPage.payload)
                    {
                        // Write RESP message with fixed-width zero-padded address fields
                        // to enable in-place patching during circular replay
                        var messageLen = WriterClusterAppendLogFixedWidth(
                            bufferPtr,
                            respMessage.buffer.Length,
                            nodeId: primaryId,
                            physicalSublogIdx: threadId,
                            previousAddress,
                            currentAddress,
                            nextAddress,
                            (long)payloadPtr,
                            tempPage.payloadLength,
                            respMessage);

                        respMessage.messageOffset = 0;
                        respMessage.messageLength = messageLen;
                        respMessage.payloadLength = tempPage.payloadLength;
                        respMessage.recordCount = tempPage.recordCount;
                    }

                    respReplayMessageBuffers[threadId][i] = respMessage;

                    previousAddress = nextAddress;
                    currentAddress = currentAddress == 64 ? pageSize : currentAddress + pageSize;
                }
            }
            else
            {
                pageBuffers[threadId] = new Page[pages];
                for (var i = 0; i < pages; i++)
                {
                    pageBuffers[threadId][i] = new Page(pageSize);
                    FillPage(threadId, kvPairs, i, pageBuffers[threadId][i]);
                }
            }

            _ = Interlocked.Add(ref total_number_of_aof_records, number_of_aof_records);
            _ = Interlocked.Add(ref total_number_of_aof_bytes, number_of_aof_bytes);

            void FillPage(int threadId, List<(byte[], byte[])> kvPairs, int pageCount, Page page)
            {
                fixed (byte* pagePtr = page.payload)
                {
                    var pageOffset = pagePtr;
                    // First page starts from 64 address, so the payload space must be smaller
                    var pageEnd = pageOffset + page.Length - (pageCount == 0 ? 64 : 0);
                    var kvOffset = 0;
                    while (true)
                    {
                        var kvPair = kvPairs[kvOffset++ % kvPairs.Count];
                        var keyData = kvPair.Item1;
                        var valueData = kvPair.Item2;
                        StringInput input = default;
                        fixed (byte* keyPtr = keyData)
                        fixed (byte* valuePtr = valueData)
                        {
                            var key = SpanByte.FromPinnedPointer(keyPtr, keyData.Length);
                            var value = SpanByte.FromPinnedPointer(valuePtr, valueData.Length);
                            var aofHeader = new AofHeader { opType = AofEntryType.StoreUpsert, storeVersion = 1, sessionID = 0 };
                            var useShardedHeader = options.AofPhysicalSublogCount > 1 || options.AofReplayTaskCount > 1;
                            if (!useShardedHeader)
                            {
                                if (!garnetLog.GetSubLog(threadId).DummyEnqueue(
                                    ref pageOffset,
                                    pageEnd,
                                    aofHeader,
                                    key,
                                    value,
                                    ref input))
                                    break;
                            }
                            else
                            {
                                var extendedAofHeader = new AofShardedHeader
                                {
                                    basicHeader = new AofHeader
                                    {
                                        padding = (byte)AofHeaderType.ShardedHeader,
                                        opType = aofHeader.opType,
                                        storeVersion = aofHeader.storeVersion,
                                        sessionID = aofHeader.sessionID
                                    },
                                    sequenceNumber = seqNumGen.GetSequenceNumber()
                                };

                                if (!garnetLog.GetSubLog(threadId).DummyEnqueue(
                                    ref pageOffset,
                                    pageEnd,
                                    extendedAofHeader,
                                    key,
                                    value,
                                    ref input))
                                    break;
                            }
                            page.recordCount++;
                        }
                    }

                    var payloadLength = (int)(pageOffset - pagePtr);
                    page.payloadLength = payloadLength;
                    number_of_aof_records += page.recordCount;
                    number_of_aof_bytes += payloadLength;
                }
            }
        }

        void GenerateKeys(int threadId)
        {
            kvPairBuffers[threadId] = GenerateKVPairs(threadId, options.AofBenchType == AofBenchType.EnqueueRandom);
            //Console.WriteLine($"[{threadId}] - Generated {kvPairBuffers[threadId].Count} KV pairs for {options.AofBenchType}");
        }
    }
}