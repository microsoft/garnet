// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Options when creating Garnet server
    /// </summary>
    public class ServerOptions
    {
        /// <summary>
        /// Port to run server on.
        /// </summary>
        public int Port = 3278;

        /// <summary>
        /// IP address to bind server to.
        /// </summary>
        public string Address = "127.0.0.1";

        /// <summary>
        /// Total log memory used in bytes (rounds down to power of 2).
        /// </summary>
        public string MemorySize = "16g";

        /// <summary>
        /// Size of each page in bytes (rounds down to power of 2).
        /// </summary>
        public string PageSize = "32m";

        /// <summary>
        /// Size of each log segment in bytes on disk (rounds down to power of 2).
        /// </summary>
        public string SegmentSize = "1g";

        /// <summary>
        /// Size of hash index in bytes (rounds down to power of 2).
        /// </summary>
        public string IndexSize = "8g";

        /// <summary>
        /// Max size of hash index in bytes (rounds down to power of 2). If unspecified, index size doesn't grow (default behavior).
        /// </summary>
        public string IndexMaxSize = string.Empty;

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
        /// When records are read from the object store's in-memory immutable region or storage device, copy them to the tail of the log.
        /// </summary>
        public bool ObjectStoreCopyReadsToTail = false;

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

        public int MemorySizePagesCalculator(string memorySize, int pageSizeBits)
        {
            long size = ParseSize(memorySize);
            long pageSize = 1L << pageSizeBits;
            long numPagesLong = (size + pageSize - 1) / pageSize;
            int numPages = numPagesLong > int.MaxValue ? int.MaxValue : (int)numPagesLong;
            long adjustedSize = numPages * pageSize;
            if (size != adjustedSize)
                logger?.LogInformation($"Warning: using adjusted memory size {PrettySize(adjustedSize)} which is different from specified size of {PrettySize(size)}");
            return numPages;
        }

        /// <summary>
        /// Get page size
        /// </summary>
        /// <returns></returns>
        public int PageSizeBits()
        {
            long size = ParseSize(PageSize);
            long adjustedSize = PreviousPowerOf2(size);
            if (size != adjustedSize)
                logger?.LogInformation($"Warning: using lower page size than specified (power of 2)");
            return (int)Math.Log(adjustedSize, 2);
        }

        /// <summary>
        /// Get pub/sub page size
        /// </summary>
        /// <returns></returns>
        public long PubSubPageSizeBytes()
        {
            long size = ParseSize(PubSubPageSize);
            long adjustedSize = PreviousPowerOf2(size);
            if (size != adjustedSize)
                logger?.LogInformation($"Warning: using lower pub/sub page size than specified (power of 2)");
            return adjustedSize;
        }

        /// <summary>
        /// Get segment size
        /// </summary>
        /// <returns></returns>
        public int SegmentSizeBits()
        {
            long size = ParseSize(SegmentSize);
            long adjustedSize = PreviousPowerOf2(size);
            if (size != adjustedSize)
                logger?.LogInformation($"Warning: using lower disk segment size than specified (power of 2)");
            return (int)Math.Log(adjustedSize, 2);
        }

        /// <summary>
        /// Get index size
        /// </summary>
        /// <returns></returns>
        public int IndexSizeCachelines(string name, string indexSize)
        {
            long size = ParseSize(indexSize);
            long adjustedSize = PreviousPowerOf2(size);
            if (adjustedSize < 64 || adjustedSize > (1L << 37)) throw new Exception($"Invalid {name}");
            if (size != adjustedSize)
                logger?.LogInformation($"Warning: using lower {name} than specified (power of 2)");
            return (int)(adjustedSize / 64);
        }

        /// <summary>
        /// Parse size from string specification
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        protected static long ParseSize(string value)
        {
            char[] suffix = new char[] { 'k', 'm', 'g', 't', 'p' };
            long result = 0;
            foreach (char c in value)
            {
                if (char.IsDigit(c))
                {
                    result = result * 10 + (byte)c - '0';
                }
                else
                {
                    for (int i = 0; i < suffix.Length; i++)
                    {
                        if (char.ToLower(c) == suffix[i])
                        {
                            result *= (long)Math.Pow(1024, i + 1);
                            return result;
                        }
                    }
                }
            }
            return result;
        }

        /// <summary>
        /// Pretty print value
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        protected static string PrettySize(long value)
        {
            char[] suffix = new char[] { 'k', 'm', 'g', 't', 'p' };
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
        protected long PreviousPowerOf2(long v)
        {
            v |= v >> 1;
            v |= v >> 2;
            v |= v >> 4;
            v |= v >> 8;
            v |= v >> 16;
            v |= v >> 32;
            return v - (v >> 1);
        }
    }
}