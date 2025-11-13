// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using CommandLine;

namespace AofExplorer
{
    /// <summary>
    /// Display format options for AOF entries
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
    /// Log checksum type options
    /// </summary>
    public enum ChecksumType
    {
        /// <summary>
        /// No checksum validation
        /// </summary>
        None,
        /// <summary>
        /// Per-entry checksum validation
        /// </summary>
        PerEntry
    }

    /// <summary>
    /// Command line options for AOF Browser
    /// </summary>
    public class Options
    {
        [Option('d', "aof-dir", Required = false, Default = "AOF/")]
        public string AofDir { get; set; } = string.Empty;

        [Option('c', "checksum-type", Required = false, Default = ChecksumType.None,
                HelpText = "Checksum validation type (None, PerEntry)")]
        public ChecksumType ChecksumType { get; set; }

        [Option('m', "max-entries", Required = false, Default = int.MaxValue,
                HelpText = "Maximum number of entries to display")]
        public int MaxEntries { get; set; }

        [Option('f', "format", Required = false, Default = DisplayFormat.Standard,
                HelpText = "Display format (Compact, Standard, Verbose)")]
        public DisplayFormat Format { get; set; }

        [Option('v', "verbose", Required = false, Default = false,
                HelpText = "Enable verbose output (same as --format Verbose)")]
        public bool Verbose { get; set; }

        [Option('s', "skip", Required = false, Default = 0,
                HelpText = "Skip the first N entries")]
        public int Skip { get; set; }

        [Option("filter-operation", Required = false,
                HelpText = "Filter entries by operation type (StoreUpsert, StoreRMW, StoreDelete, etc.)")]
        public string FilterOperation { get; set; }

        [Option("filter-session", Required = false,
                HelpText = "Filter entries by session ID")]
        public int? FilterSession { get; set; }

        [Option("filter-version", Required = false,
                HelpText = "Filter entries by store version")]
        public long? FilterVersion { get; set; }

        [Option("min-offset", Required = false,
                HelpText = "Start parsing from this file offset (hex format supported with 0x prefix)")]
        public string MinOffset { get; set; }

        [Option("max-offset", Required = false,
                HelpText = "Stop parsing at this file offset (hex format supported with 0x prefix)")]
        public string MaxOffset { get; set; }

        [Option("key-filter", Required = false,
                HelpText = "Filter entries containing this key substring")]
        public string KeyFilter { get; set; }

        [Option("value-filter", Required = false,
                HelpText = "Filter entries containing this value substring")]
        public string ValueFilter { get; set; }

        [Option("export-csv", Required = false,
                HelpText = "Export results to CSV file")]
        public string ExportCsv { get; set; }

        [Option("export-json", Required = false,
                HelpText = "Export results to JSON file")]
        public string ExportJson { get; set; }
        [Option("stats-only", Required = false, Default = false,
                HelpText = "Show only statistics summary (no entry details)")]
        public bool StatsOnly { get; set; }

        /// <summary>
        /// Get the effective display format, considering verbose flag
        /// </summary>
        public DisplayFormat GetEffectiveFormat()
        {
            return Verbose ? DisplayFormat.Verbose : Format;
        }

        /// <summary>
        /// Parse offset string that may be in hex (0x prefix) or decimal format
        /// </summary>
        public static long? ParseOffset(string offsetStr)
        {
            if (string.IsNullOrEmpty(offsetStr))
                return null;

            if (offsetStr.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
            {
                if (long.TryParse(offsetStr.Substring(2), System.Globalization.NumberStyles.HexNumber, null, out var hexValue))
                    return hexValue;
            }
            else
            {
                if (long.TryParse(offsetStr, out var decValue))
                    return decValue;
            }

            return null;
        }

        /// <summary>
        /// Convert DisplayFormat enum to AofEntryDisplayFormatter.DisplayFormat
        /// </summary>
        internal ConsoleFormatter.DisplayFormat GetDisplayFormat()
        {
            var effectiveFormat = GetEffectiveFormat();
            return effectiveFormat switch
            {
                DisplayFormat.Compact => ConsoleFormatter.DisplayFormat.Compact,
                DisplayFormat.Verbose => ConsoleFormatter.DisplayFormat.Verbose,
                _ => ConsoleFormatter.DisplayFormat.Standard
            };
        }
    }
}