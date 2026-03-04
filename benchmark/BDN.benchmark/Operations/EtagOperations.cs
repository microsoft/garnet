// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using Embedded.server;

namespace BDN.benchmark.Operations
{
    /// <summary>
    /// Benchmark for RawStringOperations
    /// </summary>
    [MemoryDiagnoser]
    public class EtagOperations : OperationsBase
    {
        static ReadOnlySpan<byte> SETWITHETAG => "*4\r\n$12\r\nEXECWITHETAG\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\na\r\n"u8;
        Request setwithetag;

        static ReadOnlySpan<byte> SETIFMATCH => "*5\r\n$11\r\nEXECIFMATCH\r\n$1\r\n1\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\na\r\n"u8;
        Request setifmatch;

        static ReadOnlySpan<byte> GETWITHETAG => "*3\r\n$12\r\nEXECWITHETAG\r\n$3\r\nGET\r\n$1\r\na\r\n"u8;
        Request getwithetag;

        static ReadOnlySpan<byte> GETIFMATCH => "*4\r\n$11\r\nEXECIFMATCH\r\n$1\r\n1\r\n$3\r\nGET\r\n$1\r\na\r\n"u8;
        Request getifmatch;

        public override void GlobalSetup()
        {
            base.GlobalSetup();
            SetupOperation(ref setwithetag, SETWITHETAG);
            SetupOperation(ref setifmatch, SETIFMATCH);
            SetupOperation(ref getwithetag, GETWITHETAG);
            SetupOperation(ref getifmatch, GETIFMATCH);

            // Pre-populate data
            SlowConsumeMessage("*4\r\n$12\r\nEXECWITHETAG\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\na\r\n"u8);
        }

        [Benchmark]
        public void SetWithEtag()
        {
            Send(setwithetag);
        }

        [Benchmark]
        public void SetIfMatch()
        {
            Send(setifmatch);
        }

        [Benchmark]
        public void GetWithEtag()
        {
            Send(getwithetag);
        }

        [Benchmark]
        public void GetIfMatch()
        {
            Send(getifmatch);
        }
    }
}
