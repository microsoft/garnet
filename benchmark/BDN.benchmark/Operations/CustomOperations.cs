// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BDN.benchmark.CustomProcs;
using BenchmarkDotNet.Attributes;
using Embedded.server;
using Garnet;
using Garnet.server;

namespace BDN.benchmark.Operations
{
    /// <summary>
    /// Benchmark for ObjectOperations
    /// </summary>
    [MemoryDiagnoser]
    public unsafe class CustomOperations : OperationsBase
    {
        static ReadOnlySpan<byte> SETIFPM => "*4\r\n$7\r\nSETIFPM\r\n$1\r\nk\r\n$3\r\nval\r\n$1\r\nv\r\n"u8;
        Request setIfPm;

        static ReadOnlySpan<byte> MYDICTSETGET => "*4\r\n$9\r\nMYDICTSET\r\n$2\r\nck\r\n$1\r\nf\r\n$1\r\nv\r\n*3\r\n$9\r\nMYDICTGET\r\n$2\r\nck\r\n$1\r\nf\r\n"u8;
        Request myDictSetGet;

        static ReadOnlySpan<byte> CTXNSET => "*9\r\n$7\r\nCTXNSET\r\n$6\r\n{0}000\r\n$6\r\n{0}001\r\n$6\r\n{0}002\r\n$6\r\n{0}003\r\n$6\r\n{0}000\r\n$6\r\n{0}001\r\n$6\r\n{0}002\r\n$6\r\n{0}003\r\n"u8;
        Request ctxnset;

        static ReadOnlySpan<byte> CPROCSET => "*9\r\n$8\r\nCPROCSET\r\n$6\r\n{0}000\r\n$6\r\n{0}001\r\n$6\r\n{0}002\r\n$6\r\n{0}003\r\n$6\r\n{0}000\r\n$6\r\n{0}001\r\n$6\r\n{0}002\r\n$6\r\n{0}003\r\n"u8;
        Request cprocset;

        void CreateExtensions()
        {
            // Register custom raw string command
            server.Register.NewCommand("SETIFPM", CommandType.ReadModifyWrite, new SetIfPMCustomCommand(), new RespCommandsInfo { Arity = 4 });

            // Register custom object type and commands
            var factory = new MyDictFactory();
            server.Register.NewType(factory);
            server.Register.NewCommand("MYDICTSET", CommandType.ReadModifyWrite, factory, new MyDictSet(), new RespCommandsInfo { Arity = 4 });
            server.Register.NewCommand("MYDICTGET", CommandType.Read, factory, new MyDictGet(), new RespCommandsInfo { Arity = 3 });

            // Register custom transaction
            server.Register.NewTransactionProc(CustomProcs.CustomTxnSet.CommandName, () => new CustomTxnSet(),
                new RespCommandsInfo { Arity = CustomProcs.CustomTxnSet.Arity });

            // Register custom procedure
            server.Register.NewProcedure(CustomProcs.CustomProcSet.CommandName, () => new CustomProcSet(),
                new RespCommandsInfo { Arity = CustomProcs.CustomProcSet.Arity });
        }

        public override void GlobalSetup()
        {
            base.GlobalSetup();
            CreateExtensions();

            SetupOperation(ref setIfPm, SETIFPM);
            SetupOperation(ref myDictSetGet, MYDICTSETGET);
            SetupOperation(ref ctxnset, CTXNSET);
            SetupOperation(ref cprocset, CPROCSET);

            SlowConsumeMessage("*4\r\n$7\r\nSETIFPM\r\n$1\r\nk\r\n$3\r\nval\r\n$1\r\nv\r\n"u8);
            SlowConsumeMessage("*4\r\n$9\r\nMYDICTSET\r\n$2\r\nck\r\n$1\r\nf\r\n$1\r\nv\r\n"u8);
            SlowConsumeMessage(CTXNSET);
            SlowConsumeMessage(CPROCSET);
        }

        [Benchmark]
        public void CustomRawStringCommand()
        {
            Send(setIfPm);
        }

        [Benchmark]
        public void CustomObjectCommand()
        {
            Send(myDictSetGet);
        }

        [Benchmark]
        public void CustomTransaction()
        {
            Send(ctxnset);
        }

        [Benchmark]
        public void CustomProcedure()
        {
            Send(cprocset);
        }
    }
}