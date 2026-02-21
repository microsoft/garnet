// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text;
using AofEntryType = Garnet.server.AofEntryType;

namespace AofExplorer
{
    /// <summary>
    /// Formats AOF entries for display with different output modes
    /// </summary>
    internal static class ConsoleFormatter
    {
        /// <summary>
        /// Display format options
        /// </summary>
        public enum DisplayFormat
        {
            /// <summary>
            /// Compact single-line format
            /// </summary>
            Compact,
            /// <summary>
            /// Standard multi-line format
            /// </summary>
            Standard,
            /// <summary>
            /// Verbose format with all details
            /// </summary>
            Verbose
        }

        /// <summary>
        /// Display an AOF entry using the specified format
        /// </summary>
        public static void DisplayEntry(AofEntry entry, DisplayFormat format)
        {
            switch (format)
            {
                case DisplayFormat.Compact:
                    DisplayCompact(entry);
                    break;
                case DisplayFormat.Standard:
                    DisplayStandard(entry);
                    break;
                case DisplayFormat.Verbose:
                    DisplayVerbose(entry);
                    break;
            }
        }

        /// <summary>
        /// Compact single-line format: [Offset] Op:Type V:Version S:Session Key Value
        /// </summary>
        private static void DisplayCompact(AofEntry entry)
        {
            var keyStr = entry.Key.Length > 0 ? TruncateString(TryGetString(entry.Key), 20) : "<no-key>";
            var valueStr = entry.Value.Length > 0 ? TruncateString(TryGetString(entry.Value), 20) :
                          entry.Header.opType is AofEntryType.StoreUpsert or AofEntryType.ObjectStoreUpsert ? "<empty>" : "<n/a>";

            var seqStr = entry.IsExtended && entry.ExtendedHeader.HasValue ?
                        $" Seq:{entry.ExtendedHeader.Value.sequenceNumber}" : "";

            Console.WriteLine($"[{entry.FileOffset:X8}] {entry.Header.opType} V:{entry.Header.storeVersion} S:{entry.Header.sessionID}{seqStr} {keyStr} {valueStr}");
        }

        /// <summary>
        /// Standard multi-line format with key information
        /// </summary>
        private static void DisplayStandard(AofEntry entry)
        {
            Console.WriteLine($"Entry at offset {entry.FileOffset:X8}:");
            Console.WriteLine($"  Operation: {entry.Header.opType}");
            Console.WriteLine($"  Version: {entry.Header.storeVersion}");
            Console.WriteLine($"  Session ID: {entry.Header.sessionID}");

            if (entry.IsExtended && entry.ExtendedHeader.HasValue)
            {
                var ext = entry.ExtendedHeader.Value;
                Console.WriteLine($"  Sequence Number: {ext.sequenceNumber}");
                //Console.WriteLine($"  Log Access Count: {ext.sublogAccessCount}");
            }

            if (entry.Key.Length > 0)
            {
                var keyStr = TryGetString(entry.Key);
                Console.WriteLine($"  Key: {keyStr} (length: {entry.Key.Length})");
            }

            if (entry.Value.Length > 0)
            {
                var valueStr = TryGetString(entry.Value);
                Console.WriteLine($"  Value: {valueStr} (length: {entry.Value.Length})");
            }
            else if (entry.Header.opType is AofEntryType.StoreUpsert or AofEntryType.ObjectStoreUpsert)
            {
                Console.WriteLine($"  Value: <empty>");
            }

            if (entry.Input.Length > 0)
            {
                Console.WriteLine($"  Input: {entry.Input.Length} bytes");
            }
            else if (entry.Header.opType is AofEntryType.StoreRMW or AofEntryType.ObjectStoreRMW)
            {
                Console.WriteLine($"  Input: <no input data> (RMW operations modify existing values)");
            }

            Console.WriteLine();
        }

        /// <summary>
        /// Verbose format with all available details including hex dumps
        /// </summary>
        private static void DisplayVerbose(AofEntry entry)
        {
            Console.WriteLine($"Entry at offset {entry.FileOffset:X8}:");
            Console.WriteLine($"  Operation: {entry.Header.opType}");
            Console.WriteLine($"  Version: {entry.Header.storeVersion}");
            Console.WriteLine($"  Session ID: {entry.Header.sessionID}");
            Console.WriteLine($"  Header Version: {entry.Header.aofHeaderVersion}");
            Console.WriteLine($"  Procedure ID: {entry.Header.procedureId}");

            if (entry.IsExtended && entry.ExtendedHeader.HasValue)
            {
                var ext = entry.ExtendedHeader.Value;
                Console.WriteLine($"  Sequence Number: {ext.sequenceNumber}");
                //Console.WriteLine($"  Log Access Count: {ext.sublogAccessCount}");
            }

            if (entry.Key.Length > 0)
            {
                var keyStr = TryGetString(entry.Key);
                Console.WriteLine($"  Key: {keyStr} (length: {entry.Key.Length})");
                if (entry.Key.Length <= 64)
                {
                    Console.WriteLine($"    Key Hex: {Convert.ToHexString(entry.Key)}");
                }
            }

            if (entry.Value.Length > 0)
            {
                var valueStr = TryGetString(entry.Value);
                Console.WriteLine($"  Value: {valueStr} (length: {entry.Value.Length})");
                if (entry.Value.Length <= 64)
                {
                    Console.WriteLine($"    Value Hex: {Convert.ToHexString(entry.Value)}");
                }
            }
            else if (entry.Header.opType is AofEntryType.StoreUpsert or AofEntryType.ObjectStoreUpsert)
            {
                Console.WriteLine($"  Value: <empty>");
            }

            if (entry.Input.Length > 0)
            {
                Console.WriteLine($"  Input: {entry.Input.Length} bytes");
                if (entry.Input.Length < 100)
                {
                    Console.WriteLine($"    Input Hex: {Convert.ToHexString(entry.Input)}");
                }
            }
            else if (entry.Header.opType is AofEntryType.StoreRMW or AofEntryType.ObjectStoreRMW)
            {
                Console.WriteLine($"  Input: <no input data> (RMW operations modify existing values)");
            }

            Console.WriteLine($"  Total Length: {entry.TotalLength}");
            Console.WriteLine();
        }

        /// <summary>
        /// Try to convert byte array to readable string representation
        /// </summary>
        private static string TryGetString(byte[] data)
        {
            try
            {
                // Try to decode as UTF-8
                var str = Encoding.UTF8.GetString(data);

                // Check if it's printable
                if (str.All(c => !char.IsControl(c) || char.IsWhiteSpace(c)))
                {
                    return $"\"{str}\"";
                }
            }
            catch
            {
                // Fall through to hex representation
            }

            // Return hex representation if not printable string
            if (data.Length <= 32)
            {
                return Convert.ToHexString(data);
            }
            else
            {
                return $"{Convert.ToHexString(data.Take(16).ToArray())}...({data.Length} bytes)";
            }
        }

        /// <summary>
        /// Truncate string to specified length with ellipsis
        /// </summary>
        private static string TruncateString(string str, int maxLength)
        {
            if (str.Length <= maxLength)
                return str;

            return str.Substring(0, maxLength - 3) + "...";
        }

        /// <summary>
        /// Display multiple AOF entries with the specified format
        /// </summary>
        public static void DisplayEntries(IEnumerable<AofEntry> entries, DisplayFormat displayFormat)
        {
            var count = 0;
            foreach (var entry in entries)
            {
                count++;
                DisplayEntry(entry, displayFormat);
            }

            Console.WriteLine(new string('=', 80));
            Console.WriteLine($"Total entries processed: {count}");
        }

        /// <summary>
        /// Display statistics summary for a collection of AOF entries
        /// </summary>
        public static void DisplayStatistics(IEnumerable<AofEntry> entries)
        {
            var stats = new Dictionary<string, int>();
            var sessionStats = new Dictionary<int, int>();
            var totalEntries = 0;
            var totalBytes = 0L;

            foreach (var entry in entries)
            {
                totalEntries++;
                totalBytes += entry.TotalLength;

                var opType = entry.Header.opType.ToString();
                stats[opType] = stats.GetValueOrDefault(opType, 0) + 1;

                var sessionId = entry.Header.sessionID;
                sessionStats[sessionId] = sessionStats.GetValueOrDefault(sessionId, 0) + 1;
            }

            Console.WriteLine("AOF File Statistics:");
            Console.WriteLine($"Total entries: {totalEntries:N0}");
            Console.WriteLine($"Total size: {totalBytes:N0} bytes");
            Console.WriteLine();

            Console.WriteLine("Operations by type:");
            foreach (var kvp in stats.OrderByDescending(x => x.Value))
            {
                var percentage = (double)kvp.Value / totalEntries * 100;
                Console.WriteLine($"  {kvp.Key}: {kvp.Value:N0} ({percentage:F1}%)");
            }

            Console.WriteLine();
            Console.WriteLine("Operations by session:");
            foreach (var kvp in sessionStats.OrderByDescending(x => x.Value).Take(10))
            {
                var percentage = (double)kvp.Value / totalEntries * 100;
                Console.WriteLine($"  Session {kvp.Key}: {kvp.Value:N0} ({percentage:F1}%)");
            }

            if (sessionStats.Count > 10)
            {
                Console.WriteLine($"  ... and {sessionStats.Count - 10} more sessions");
            }

            Console.WriteLine(new string('=', 80));
            Console.WriteLine($"Statistics for {totalEntries:N0} entries");
        }

        /// <summary>
        /// Display GarnetLog metadata information with colored output
        /// </summary>
        /// <param name="garnetLog">The GarnetLog object to display metadata for</param>
        public static void DisplayGarnetLogInfo(Garnet.server.GarnetLog garnetLog)
        {
            if (garnetLog == null)
            {
                WriteColored("GarnetLog is null", ConsoleColor.Red);
                return;
            }

            WriteColored("\n📊 GarnetLog Metadata Information", ConsoleColor.Cyan, true);
            Console.WriteLine(new string('─', 50));

            // Log structure info
            WriteColored("📁 Log Structure:", ConsoleColor.Yellow, true);
            WriteColored($"   Sublog Count: ", ConsoleColor.White, false);
            WriteColored($"{garnetLog.Size}", ConsoleColor.Green);

            WriteColored($"   Header Size: ", ConsoleColor.White, false);
            WriteColored($"{garnetLog.HeaderSize} bytes", ConsoleColor.Green);

            // Address information for each sublog
            WriteColored("\n📍 Address Information:", ConsoleColor.Yellow, true);
            for (int i = 0; i < garnetLog.Size; i++)
            {
                WriteColored($"   Sublog {i}:", ConsoleColor.Magenta, true);

                var beginAddr = garnetLog.BeginAddress[i];
                var tailAddr = garnetLog.TailAddress[i];
                var committedAddr = garnetLog.CommittedUntilAddress[i];
                var committedBeginAddr = garnetLog.CommittedBeginAddress[i];
                var flushedAddr = garnetLog.FlushedUntilAddress[i];

                WriteColored($"     Begin Address:     ", ConsoleColor.White, false);
                WriteColored($"0x{beginAddr:X8} ({beginAddr:N0})", ConsoleColor.Cyan, true);

                WriteColored($"     Tail Address:      ", ConsoleColor.White, false);
                WriteColored($"0x{tailAddr:X8} ({tailAddr:N0})", ConsoleColor.Cyan, true);

                WriteColored($"     Committed Until:   ", ConsoleColor.White, false);
                WriteColored($"0x{committedAddr:X8} ({committedAddr:N0})", ConsoleColor.Green, true);

                WriteColored($"     Committed Begin:   ", ConsoleColor.White, false);
                WriteColored($"0x{committedBeginAddr:X8} ({committedBeginAddr:N0})", ConsoleColor.Green, true);

                WriteColored($"     Flushed Until:     ", ConsoleColor.White, false);
                WriteColored($"0x{flushedAddr:X8} ({flushedAddr:N0})", ConsoleColor.Blue, true);

                // Calculate sizes
                var logSize = tailAddr - beginAddr;
                var committedSize = committedAddr - beginAddr;
                var uncommittedSize = tailAddr - committedAddr;

                WriteColored($"     Log Size:          ", ConsoleColor.White, false);
                WriteColored($"{FormatBytes(logSize)}", ConsoleColor.Yellow, true);

                WriteColored($"     Committed Size:    ", ConsoleColor.White, false);
                WriteColored($"{FormatBytes(committedSize)}", ConsoleColor.Green, true);

                if (uncommittedSize > 0)
                {
                    WriteColored($"     Uncommitted Size:  ", ConsoleColor.White, false);
                    WriteColored($"{FormatBytes(uncommittedSize)}", ConsoleColor.Red, true);
                }

                if (i < garnetLog.Size - 1)
                    Console.WriteLine();
            }

            // Memory information
            WriteColored("\n💾 Memory Information:", ConsoleColor.Yellow, true);
            var totalMemorySize = 0L;
            var totalMaxMemorySize = 0L;

            for (var i = 0; i < garnetLog.Size; i++)
            {
                totalMemorySize += garnetLog.MemorySizeBytes[i];
                totalMaxMemorySize += garnetLog.MaxMemorySizeBytes[i];
            }

            WriteColored($"   Memory Size:       ", ConsoleColor.White, false);
            WriteColored($"{FormatBytes(totalMemorySize)}", ConsoleColor.Cyan);

            WriteColored($"   Max Memory Size:   ", ConsoleColor.White, false);
            WriteColored($"{FormatBytes(totalMaxMemorySize)}", ConsoleColor.Cyan);

            Console.WriteLine("\n" + new string('─', 50));
        }

        /// <summary>
        /// Write colored text to console
        /// </summary>
        /// <param name="text">Text to write</param>
        /// <param name="color">Console color</param>
        /// <param name="bold">Whether to make text bold (not supported in all terminals)</param>
        private static void WriteColored(string text, ConsoleColor color, bool bold = false)
        {
            var originalColor = Console.ForegroundColor;
            Console.ForegroundColor = color;
            if (bold && text.Length > 0)
            {
                Console.WriteLine(text);
            }
            else
            {
                Console.Write(text);
            }
            Console.ForegroundColor = originalColor;
        }

        /// <summary>
        /// Format bytes into human-readable format
        /// </summary>
        /// <param name="bytes">Number of bytes</param>
        /// <returns>Formatted string (e.g., "1.5 KB", "2.3 MB")</returns>
        private static string FormatBytes(long bytes)
        {
            if (bytes == 0) return "0 bytes";

            string[] suffixes = ["bytes", "KB", "MB", "GB", "TB"];
            int suffixIndex = 0;
            double size = bytes;

            while (size >= 1024 && suffixIndex < suffixes.Length - 1)
            {
                size /= 1024;
                suffixIndex++;
            }

            return suffixIndex == 0
                ? $"{size:N0} {suffixes[suffixIndex]}"
                : $"{size:F1} {suffixes[suffixIndex]}";
        }
    }
}