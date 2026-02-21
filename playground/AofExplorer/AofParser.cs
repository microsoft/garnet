// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers;
using System.Runtime.InteropServices;
using Garnet.server;
using Tsavorite.core;
// Use AOF types directly from Garnet.server
using AofEntryType = Garnet.server.AofEntryType;
using AofShardedHeader = Garnet.server.AofShardedHeader;
using AofHeader = Garnet.server.AofHeader;

namespace AofExplorer
{
    internal class AofFileParser(Options opts)
    {
        readonly Options opts = opts;
        private GarnetLog garnetLog;

        /// <summary>
        /// Parse AOF file using Garnet's scan iterator (like AofProcessor.RecoverReplay)
        /// This follows the exact same workflow as recovery but for read-only parsing
        /// </summary>
        public IEnumerable<AofEntry> ParseFile()
        {
            try
            {
                // Initialize GarnetLog with minimal settings (following GarnetServer.CreateAOF pattern)
                InitializeGarnetLog();

                // Parse using scan iterator (following AofProcessor.RecoverReplay pattern)
                return ScanParse();
            }
            finally
            {
                garnetLog?.Dispose();
            }

            void InitializeGarnetLog()
            {
                var serverOptions = new GarnetServerOptions
                {
                    AofPhysicalSublogCount = 2,
                    EnableAOF = true,
                    CheckpointDir = opts.AofDir,
                    CommitFrequencyMs = -1,
                    EnableFastCommit = true,
                    DeviceFactoryCreator = new LocalStorageNamedDeviceFactoryCreator()
                };
                serverOptions.GetAofSettings(dbId: 0, out var aofSettings);
                Console.WriteLine($"Initializing GarnetLog from {serverOptions.GetAppendOnlyFileDirectory(0)}");
                garnetLog = new GarnetLog(serverOptions, aofSettings);
                garnetLog.Recover();

                ConsoleFormatter.DisplayGarnetLogInfo(garnetLog);
            }
        }

        /// <summary>
        /// Scan and parse AOF entries
        /// </summary>
        /// <returns></returns>
        internal IEnumerable<AofEntry> ScanParse()
        {
            var entries = new List<AofEntry>();

            // Get the single sublog (we're using AofSublogCount = 1)
            var sublogIdx = 0;
            var beginAddress = garnetLog.BeginAddress[sublogIdx];
            var tailAddress = garnetLog!.TailAddress[sublogIdx];
            var committedAddress = garnetLog.CommittedUntilAddress[sublogIdx];

            var count = 0;
            using var scan = garnetLog.GetSubLog(sublogIdx).Scan(beginAddress, tailAddress, recover: false);
            // Iterate through entries (same pattern as RecoverReplayTask)
            while (scan.GetNext(MemoryPool<byte>.Shared, out var entry, out var length, out _, out long nextAddress))
            {
                count++;
                //Console.WriteLine($"Processing entry {count} at address 0x{nextAddress:X}, length: {length}");

                // Parse AOF entry from the scanned data (like ProcessAofRecord)
                var aofEntry = ParseAofEntryFromScanData(entry, length, nextAddress);
                if (aofEntry != null)
                {
                    entries.Add(aofEntry);
                    //Console.WriteLine($"Parsed AOF entry: Type={aofEntry.Header.opType}");
                }

                // Dispose the entry memory
                entry.Dispose();

                if (count % 1000 == 0)
                    Console.WriteLine($"Processed {count} entries...");
            }

            Console.WriteLine($"Scan parsing completed. Found {entries.Count} AOF entries from {count} total entries.");
            return entries;
        }

        /// <summary>
        /// Parse AOF entry from scan iterator data (following ProcessAofRecord pattern)
        /// </summary>d
        private unsafe AofEntry ParseAofEntryFromScanData(IMemoryOwner<byte> entry, int length, long address)
        {
            fixed (byte* ptr = entry.Memory.Span)
            {
                return ParseAofEntryFromBytes(ptr, length, address);
            }
        }

        /// <summary>
        /// Parse AOF entry from byte pointer (following ProcessAofRecordInternal pattern)
        /// </summary>
        private unsafe AofEntry ParseAofEntryFromBytes(byte* ptr, int length, long address)
        {
            try
            {
                var entry = new AofEntry { FileOffset = address };
                var offset = 0;

                // Ensure we have at least enough data for AofHeader
                if (length < 16)
                {
                    return null;
                }

                // Read data as span for easier access
                var data = new ReadOnlySpan<byte>(ptr, length);

                // Validate AOF header version first
                var version = data[0];
                if (version is not 1 and not 2)
                    throw new Exception($"AOF version {version} not supported!");

                // Read the padding byte to determine header type
                // The first bit of the padding field indicates header type:
                // - 0: Standard AofHeader (16 bytes)
                // - 1: AofExtendedHeader (25 bytes) with sequence number and log access count
                var paddingByte = data[1];
                var hasExtendedHeader = (paddingByte & 1) != 0;

                if (hasExtendedHeader && data.Length >= 25)
                {
                    entry.IsExtended = true;
                    entry.ExtendedHeader = MemoryMarshal.Read<AofShardedHeader>(data.Slice(offset, 25));
                    entry.Header = entry.ExtendedHeader.Value.basicHeader;
                    offset += 25;
                }
                else
                {
                    entry.IsExtended = false;
                    entry.Header = MemoryMarshal.Read<AofHeader>(data.Slice(offset, 16));
                    offset += 16;
                }

                // Parse based on operation type
                switch (entry.Header.opType)
                {
                    case AofEntryType.StoreUpsert:
                    case AofEntryType.ObjectStoreUpsert:
                        return ParseUpsertEntry(entry, data.Slice(offset));

                    case AofEntryType.StoreRMW:
                    case AofEntryType.ObjectStoreRMW:
                        return ParseRMWEntry(entry, data.Slice(offset));

                    case AofEntryType.StoreDelete:
                    case AofEntryType.ObjectStoreDelete:
                        return ParseDeleteEntry(entry, data.Slice(offset));

                    case AofEntryType.TxnStart:
                    case AofEntryType.TxnCommit:
                    case AofEntryType.TxnAbort:
                        return ParseTransactionEntry(entry, data.Slice(offset));

                    case AofEntryType.StoredProcedure:
                        return ParseStoredProcedureEntry(entry, data.Slice(offset));

                    default:
                        // Unknown operation type, return basic entry
                        return entry;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error parsing entry at address 0x{address:X}: {ex.Message}");
                return null;
            }
        }

        /// <summary>
        /// Parse SpanByte structure
        /// </summary>
        private unsafe (byte[] data, int bytesRead) ParseSpanByte(ReadOnlySpan<byte> data)
        {
            //if (data.Length < 4) // Need at least 4 bytes for the length
            //    return (Array.Empty<byte>(), 0);

            //fixed (byte* ptr = data)
            //{
            //    // Read SpanByte directly using unsafe code
            //    ref var spanByte = ref Unsafe.AsRef<SpanByte>(ptr);

            //    if (spanByte.TotalSize > data.Length)
            //        return (Array.Empty<byte>(), 0);

            //    var payloadLength = spanByte.Length;
            //    if (payloadLength < 0)
            //        return (Array.Empty<byte>(), spanByte.TotalSize);

            //    var result = spanByte.ToByteArray();
            //    return (result, spanByte.TotalSize);
            //}
            return (new byte[8], 8);
        }

        private AofEntry ParseUpsertEntry(AofEntry entry, ReadOnlySpan<byte> data)
        {
            var offset = 0;

            // Parse key
            var (key, keyBytes) = ParseSpanByte(data.Slice(offset));
            entry.Key = key;
            offset += keyBytes;

            // Parse value  
            var (value, valueBytes) = ParseSpanByte(data.Slice(offset));
            entry.Value = value;
            offset += valueBytes;

            // Debug: Check if we have remaining data that should be value
            if (entry.Value.Length == 0 && offset < data.Length)
            {
                // If value parsing failed but we have remaining data, treat it all as input for now
                entry.Input = data.Slice(offset).ToArray();
            }
            else if (offset < data.Length)
            {
                // Remaining data is input (if any)
                entry.Input = data.Slice(offset).ToArray();
            }

            return entry;
        }

        private AofEntry ParseRMWEntry(AofEntry entry, ReadOnlySpan<byte> data)
        {
            var offset = 0;

            // Parse key
            var (key, keyBytes) = ParseSpanByte(data.Slice(offset));
            entry.Key = key;
            offset += keyBytes;

            // Remaining data is input
            if (offset < data.Length)
            {
                entry.Input = data.Slice(offset).ToArray();
            }

            return entry;
        }

        private AofEntry ParseDeleteEntry(AofEntry entry, ReadOnlySpan<byte> data)
        {
            var offset = 0;

            // Parse key
            var (key, keyBytes) = ParseSpanByte(data.Slice(offset));
            entry.Key = key;
            offset += keyBytes;

            // Parse value (usually empty for delete)
            var (value, valueBytes) = ParseSpanByte(data.Slice(offset));
            entry.Value = value;

            return entry;
        }

        private AofEntry ParseTransactionEntry(AofEntry entry, ReadOnlySpan<byte> data)
        {
            // Transaction entries typically don't have key/value data
            // Just the header information is relevant
            return entry;
        }

        private AofEntry ParseStoredProcedureEntry(AofEntry entry, ReadOnlySpan<byte> data)
        {
            // Stored procedure entries contain custom input data
            if (data.Length > 0)
            {
                entry.Input = data.ToArray();
            }
            return entry;
        }
    }
}