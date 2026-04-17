// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using Embedded.server;

namespace BDN.benchmark.Operations
{
    /// <summary>
    /// Benchmark for RangeIndex (BfTree) operations.
    /// Pre-creates a memory-only range index with 100 key-value entries,
    /// then benchmarks individual RI.SET, RI.GET, RI.DEL, RI.SCAN, and RI.RANGE operations.
    /// </summary>
    [MemoryDiagnoser]
    public class RangeIndexOperations : OperationsBase
    {
        // RI.SET myindex key00000 val00000
        static ReadOnlySpan<byte> RISET => "*4\r\n$6\r\nRI.SET\r\n$7\r\nmyindex\r\n$8\r\nkey00000\r\n$8\r\nval00000\r\n"u8;
        Request riSet;

        // RI.GET myindex key00000
        static ReadOnlySpan<byte> RIGET => "*3\r\n$6\r\nRI.GET\r\n$7\r\nmyindex\r\n$8\r\nkey00000\r\n"u8;
        Request riGet;

        // RI.DEL myindex key99999 (non-existent field — bf-tree delete is void, so this is a no-op)
        static ReadOnlySpan<byte> RIDEL => "*3\r\n$6\r\nRI.DEL\r\n$7\r\nmyindex\r\n$8\r\nkey99999\r\n"u8;
        Request riDel;

        // RI.SCAN myindex key00048 COUNT 5
        static ReadOnlySpan<byte> RISCAN => "*5\r\n$7\r\nRI.SCAN\r\n$7\r\nmyindex\r\n$8\r\nkey00048\r\n$5\r\nCOUNT\r\n$1\r\n5\r\n"u8;
        Request riScan;

        // RI.RANGE myindex key00048 key00052
        static ReadOnlySpan<byte> RIRANGE => "*4\r\n$8\r\nRI.RANGE\r\n$7\r\nmyindex\r\n$8\r\nkey00048\r\n$8\r\nkey00052\r\n"u8;
        Request riRange;

        // RI.SCAN myindex key00048 COUNT 5 FIELDS KEY
        static ReadOnlySpan<byte> RISCAN_KEYS => "*7\r\n$7\r\nRI.SCAN\r\n$7\r\nmyindex\r\n$8\r\nkey00048\r\n$5\r\nCOUNT\r\n$1\r\n5\r\n$6\r\nFIELDS\r\n$3\r\nKEY\r\n"u8;
        Request riScanKeys;

        /// <summary>
        /// Skip ACL variants — RI commands are not in the default ACL whitelist.
        /// </summary>
        public new IEnumerable<OperationParams> OperationParamsProvider()
        {
            yield return new(false, false);
            yield return new(false, true);
        }

        public override void GlobalSetup()
        {
            base.GlobalSetup();

            SetupOperation(ref riSet, RISET);
            SetupOperation(ref riGet, RIGET);
            SetupOperation(ref riDel, RIDEL);
            SetupOperation(ref riScan, RISCAN);
            SetupOperation(ref riRange, RIRANGE);
            SetupOperation(ref riScanKeys, RISCAN_KEYS);

            // Create a disk-backed range index (memory-only mode does not support scan)
            var createCmd = "*5\r\n$9\r\nRI.CREATE\r\n$7\r\nmyindex\r\n$4\r\nDISK\r\n$9\r\nMINRECORD\r\n$1\r\n8\r\n";
            SlowConsumeMessage(System.Text.Encoding.UTF8.GetBytes(createCmd));

            // Pre-populate 1000 key-value entries (key00000..key00999 → val00000..val00999)
            // so the total data exceeds the base page size (~4 KB default). This
            // ensures reads are served from the cache and disk-backed reads don't
            // hit a cold-page corner case.
            for (var i = 0; i < 1000; i++)
            {
                var key = $"key{i:D5}";
                var val = $"val{i:D5}";
                var cmd = $"*4\r\n$6\r\nRI.SET\r\n$7\r\nmyindex\r\n${key.Length}\r\n{key}\r\n${val.Length}\r\n{val}\r\n";
                SlowConsumeMessage(System.Text.Encoding.UTF8.GetBytes(cmd));
            }
        }

        [Benchmark]
        public void RISet()
        {
            Send(riSet);
        }

        [Benchmark]
        public void RIGet()
        {
            Send(riGet);
        }

        [Benchmark]
        public void RIDel()
        {
            Send(riDel);
        }

        [Benchmark]
        public void RIScan()
        {
            Send(riScan);
        }

        [Benchmark]
        public void RIScanKeysOnly()
        {
            Send(riScanKeys);
        }

        [Benchmark]
        public void RIRange()
        {
            Send(riRange);
        }
    }
}