// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Options when creating Garnet server
    /// </summary>
    public class ServerOptions
    {
        public const byte DEFAULT_RESP_VERSION = 2;

        /// <summary>
        /// Endpoints to bind server to.
        /// </summary>
        public EndPoint[] EndPoints { get; set; } = [new IPEndPoint(IPAddress.Loopback, 6379)];

        /// <summary>
        /// Cluster announce Endpoint
        /// </summary>
        public EndPoint ClusterAnnounceEndpoint { get; set; }

        /// <summary>
        /// Total main-log memory (inline and heap) to use, in bytes. Does not need to be a power of 2
        /// </summary>
        public string LogMemorySize = "16g";

        /// <summary>
        /// Size of each main-log page in bytes (rounds down to power of 2).
        /// </summary>
        public string PageSize = "32m";

        /// <summary>
        /// Number of main-log pages (rounds down to power of 2). This allows specifying less pages initially than <see cref="LogMemorySize"/> divided by <see cref="PageSize"/>
        /// </summary>
        /// <remarks>The default empty value means to calculate <see cref="PageCount"/> based on <see cref="LogMemorySize"/> divided by <see cref="PageSize"/></remarks>
        public string PageCount = "";

        /// <summary>
        /// Size of each main-log segment in bytes on disk (rounds down to power of 2).
        /// </summary>
        public string SegmentSize = "1g";

        /// <summary>
        /// Size of each object-log segment in bytes on disk (rounds down to power of 2).
        /// </summary>
        public string ObjectLogSegmentSize = "1g";

        /// <summary>
        /// Size of hash index in bytes (rounds down to power of 2).
        /// </summary>
        public string IndexMemorySize = "128m";

        /// <summary>
        /// Max size of hash index in bytes (rounds down to power of 2). If unspecified, index size doesn't grow (default behavior).
        /// </summary>
        public string IndexMaxMemorySize = string.Empty;

        /// <summary>
        /// Percentage of log memory that is kept mutable.
        /// </summary>
        public int MutablePercent = 90;

        /// <summary>
        /// Enable tiering of records (hybrid log) to storage, to support a larger-than-memory store. Use LogDir to specify storage directory.
        /// </summary>
        public bool EnableStorageTier = false;

        /// <summary>
        /// When records are read from the main store's in-memory immutable region or storage device, copy them to the tail of the log.
        /// </summary>
        public bool CopyReadsToTail = false;

        /// <summary>
        /// Storage directory for tiered records (hybrid log), if storage tiering (UseStorage) is enabled. Uses current directory if unspecified.
        /// </summary>
        public string LogDir = null;

        /// <summary>
        /// Storage directory for checkpoints. Uses LogDir if unspecified.
        /// </summary>
        public string CheckpointDir = null;

        /// <summary>
        /// Recover from latest checkpoint.
        /// </summary>
        public bool Recover = false;

        /// <summary>
        /// Disable pub/sub feature on server.
        /// </summary>
        public bool DisablePubSub = false;

        /// <summary>
        /// Page size of log used for pub/sub (rounds down to power of 2).
        /// </summary>
        public string PubSubPageSize = "4k";

        /// <summary>
        /// Server bootup should fail if errors happen during bootup of AOF and checkpointing.
        /// </summary>
        public bool FailOnRecoveryError = false;

        /// <summary>
        /// Skip RDB restore checksum validation
        /// </summary>
        public bool SkipRDBRestoreChecksumValidation = false;

        /// <summary>
        /// Logger
        /// </summary>
        public ILogger logger;

        /// <summary>
        /// Constructor
        /// </summary>
        public ServerOptions(ILogger logger = null)
        {
            this.logger = logger;
        }

        /// <summary>
        /// Get memory size
        /// </summary>
        /// <returns></returns>
        public int MemorySizeBits()
        {
            long size = ParseSize(LogMemorySize, out _);
            long adjustedSize = PreviousPowerOf2(size);
            if (size != adjustedSize)
                logger?.LogInformation("Warning: using lower log memory size than specified (power of 2)");
            return (int)Math.Log(adjustedSize, 2);
        }

        /// <summary>
        /// Get page size
        /// </summary>
        /// <returns></returns>
        public int PageSizeBits()
        {
            var size = ParseSize(PageSize, out _);
            var adjustedSize = PreviousPowerOf2(size);
            if (size != adjustedSize)
                logger?.LogInformation("Warning: using lower page size than specified (power of 2)");
            return (int)Math.Log(adjustedSize, 2);
        }

        /// <summary>
        /// Get pub/sub page size
        /// </summary>
        /// <returns></returns>
        public long PubSubPageSizeBytes()
        {
            long size = ParseSize(PubSubPageSize, out _);
            long adjustedSize = PreviousPowerOf2(size);
            if (size != adjustedSize)
                logger?.LogInformation("Warning: using lower pub/sub page size than specified (power of 2)");
            return adjustedSize;
        }

        /// <summary>
        /// Get segment size bits for either the main log or object log
        /// </summary>
        /// <returns></returns>
        public int SegmentSizeBits(bool isObj)
        {
            long size = ParseSize(isObj ? ObjectLogSegmentSize : SegmentSize, out _);
            long adjustedSize = PreviousPowerOf2(size);
            if (size != adjustedSize)
                logger?.LogInformation("Warning: using lower {SegmentType} than specified (power of 2)", isObj ? "ObjSegmentSize" : "SegmentSize");
            return (int)Math.Log(adjustedSize, 2);
        }

        /// <summary>
        /// Get index size
        /// </summary>
        /// <returns></returns>
        public int IndexSizeCachelines(string name, string indexSize)
        {
            long size = ParseSize(indexSize, out _);
            long adjustedSize = PreviousPowerOf2(size);
            if (adjustedSize < 64 || adjustedSize > (1L << 37)) throw new Exception($"Invalid {name}");
            if (size != adjustedSize)
                logger?.LogInformation("Warning: using lower {name} than specified (power of 2)", name);
            return (int)(adjustedSize / 64);
        }

        /// <summary>
        /// Parse size from string specification
        /// </summary>
        /// <param name="value"></param>
        /// <param name="bytesRead"></param>
        /// <returns></returns>
        public static long ParseSize(string value, out int bytesRead)
        {
            ReadOnlySpan<char> suffix = ['k', 'm', 'g', 't', 'p'];
            long result = 0;
            bytesRead = 0;
            for (var i = 0; i < value.Length; i++)
            {
                var c = value[i];
                if (char.IsDigit(c))
                {
                    result = (result * 10) + (byte)c - '0';
                    bytesRead++;
                }
                else
                {
                    for (var s = 0; s < suffix.Length; s++)
                    {
                        if (char.ToLower(c) == suffix[s])
                        {
                            result *= (long)Math.Pow(1024, s + 1);
                            bytesRead++;

                            if (i + 1 < value.Length && char.ToLower(value[i + 1]) == 'b')
                                bytesRead++;

                            return result;
                        }
                    }
                }
            }
            return result;
        }

        /// <summary>
        /// Try to parse size from string specification
        /// </summary>
        /// <param name="value">String size value</param>
        /// <param name="size">Parsed size</param>
        /// <returns>True if successful</returns>
        public static bool TryParseSize(string value, out long size)
        {
            size = ParseSize(value, out var charsRead);
            return charsRead == value.Length;
        }

        /// <summary>
        /// Pretty print value
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        internal static string PrettySize(long value)
        {
            ReadOnlySpan<char> suffix = ['k', 'm', 'g', 't', 'p'];
            double v = value;
            int exp = 0;
            while (v - Math.Floor(v) > 0)
            {
                if (exp >= 18)
                    break;
                exp += 3;
                v *= 1024;
                v = Math.Round(v, 12);
            }

            while (Math.Floor(v).ToString().Length > 3)
            {
                if (exp <= -18)
                    break;
                exp -= 3;
                v /= 1024;
                v = Math.Round(v, 12);
            }
            if (exp > 0)
                return v.ToString() + suffix[exp / 3 - 1];
            else if (exp < 0)
                return v.ToString() + suffix[-exp / 3 - 1];
            return v.ToString();
        }

        /// <summary>
        /// Previous power of 2
        /// </summary>
        /// <param name="v"></param>
        /// <returns></returns>
        public static long PreviousPowerOf2(long v)
        {
            v |= v >> 1;
            v |= v >> 2;
            v |= v >> 4;
            v |= v >> 8;
            v |= v >> 16;
            v |= v >> 32;
            return v - (v >> 1);
        }

        /// <summary>
        /// Next power of 2
        /// </summary>
        /// <param name="v"></param>
        /// <returns></returns>
        internal static long NextPowerOf2(long v)
        {
            v--;
            v |= v >> 1;
            v |= v >> 2;
            v |= v >> 4;
            v |= v >> 8;
            v |= v >> 16;
            v |= v >> 32;
            return v + 1;
        }
    }
}