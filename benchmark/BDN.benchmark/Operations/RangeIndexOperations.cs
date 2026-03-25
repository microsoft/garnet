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
    public unsafe class RangeIndexOperations : OperationsBase
    {
        // RI.SET myindex key050 val050
        static ReadOnlySpan<byte> RISET => "*4\r\n$6\r\nRI.SET\r\n$7\r\nmyindex\r\n$6\r\nkey050\r\n$6\r\nval050\r\n"u8;
        Request riSet;

        // RI.GET myindex key050
        static ReadOnlySpan<byte> RIGET => "*3\r\n$6\r\nRI.GET\r\n$7\r\nmyindex\r\n$6\r\nkey050\r\n"u8;
        Request riGet;

        // RI.DEL myindex key999 (non-existent field — bf-tree delete is void, so this is a no-op)
        static ReadOnlySpan<byte> RIDEL => "*3\r\n$6\r\nRI.DEL\r\n$7\r\nmyindex\r\n$6\r\nkey999\r\n"u8;
        Request riDel;

        // RI.SCAN myindex key048 COUNT 5
        static ReadOnlySpan<byte> RISCAN => "*5\r\n$7\r\nRI.SCAN\r\n$7\r\nmyindex\r\n$6\r\nkey048\r\n$5\r\nCOUNT\r\n$1\r\n5\r\n"u8;
        Request riScan;

        // RI.RANGE myindex key048 key052
        static ReadOnlySpan<byte> RIRANGE => "*4\r\n$8\r\nRI.RANGE\r\n$7\r\nmyindex\r\n$6\r\nkey048\r\n$6\r\nkey052\r\n"u8;
        Request riRange;

        // RI.SCAN myindex key048 COUNT 5 FIELDS KEY
        static ReadOnlySpan<byte> RISCAN_KEYS => "*7\r\n$7\r\nRI.SCAN\r\n$7\r\nmyindex\r\n$6\r\nkey048\r\n$5\r\nCOUNT\r\n$1\r\n5\r\n$6\r\nFIELDS\r\n$3\r\nKEY\r\n"u8;
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
            var dataDir = Path.Combine(Path.GetTempPath(), "bdn-ri-" + Guid.NewGuid().ToString("N")[..8]);
            Directory.CreateDirectory(dataDir);
            var dataFile = Path.Combine(dataDir, "bench.bftree");
            var createCmd = $"*8\r\n$9\r\nRI.CREATE\r\n$7\r\nmyindex\r\n$4\r\nDISK\r\n${dataFile.Length}\r\n{dataFile}\r\n$9\r\nCACHESIZE\r\n$5\r\n65536\r\n$9\r\nMINRECORD\r\n$1\r\n8\r\n";
            SlowConsumeMessage(System.Text.Encoding.UTF8.GetBytes(createCmd));

            // Pre-populate 100 key-value entries (key000..key099 → val000..val099)
            for (var i = 0; i < 100; i++)
            {
                var key = $"key{i:D3}";
                var val = $"val{i:D3}";
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