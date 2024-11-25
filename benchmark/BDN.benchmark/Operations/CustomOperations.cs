// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BDN.benchmark.CustomProcs;
using BenchmarkDotNet.Attributes;
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
        byte[] setIfPmRequestBuffer;
        byte* setIfPmRequestBufferPointer;

        static ReadOnlySpan<byte> MYDICTSETGET => "*4\r\n$9\r\nMYDICTSET\r\n$2\r\nck\r\n$1\r\nf\r\n$1\r\nv\r\n*3\r\n$9\r\nMYDICTGET\r\n$2\r\nck\r\n$1\r\nf\r\n"u8;
        byte[] myDictSetGetRequestBuffer;
        byte* myDictSetGetRequestBufferPointer;

        static ReadOnlySpan<byte> CTXNSET => "*9\r\n$7\r\nCTXNSET\r\n$6\r\n{0}000\r\n$6\r\n{0}001\r\n$6\r\n{0}002\r\n$6\r\n{0}003\r\n$6\r\n{0}000\r\n$6\r\n{0}001\r\n$6\r\n{0}002\r\n$6\r\n{0}003\r\n"u8;
        byte[] ctxnsetBuffer;
        byte* ctxnsetBufferPointer;

        static ReadOnlySpan<byte> CPROCSET => "*9\r\n$8\r\nCPROCSET\r\n$6\r\n{0}000\r\n$6\r\n{0}001\r\n$6\r\n{0}002\r\n$6\r\n{0}003\r\n$6\r\n{0}000\r\n$6\r\n{0}001\r\n$6\r\n{0}002\r\n$6\r\n{0}003\r\n"u8;
        byte[] cprocsetBuffer;
        byte* cprocsetBufferPointer;

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

            SetupOperation(ref setIfPmRequestBuffer, ref setIfPmRequestBufferPointer, SETIFPM);
            SetupOperation(ref myDictSetGetRequestBuffer, ref myDictSetGetRequestBufferPointer, MYDICTSETGET);
            SetupOperation(ref ctxnsetBuffer, ref ctxnsetBufferPointer, CTXNSET);
            SetupOperation(ref cprocsetBuffer, ref cprocsetBufferPointer, CPROCSET);

            SlowConsumeMessage("*4\r\n$7\r\nSETIFPM\r\n$1\r\nk\r\n$3\r\nval\r\n$1\r\nv\r\n"u8);
            SlowConsumeMessage("*4\r\n$9\r\nMYDICTSET\r\n$2\r\nck\r\n$1\r\nf\r\n$1\r\nv\r\n"u8);
            SlowConsumeMessage(ctxnsetBuffer);
            SlowConsumeMessage(cprocsetBuffer);
        }

        [Benchmark]
        public void CustomRawStringCommand()
        {
            _ = session.TryConsumeMessages(setIfPmRequestBufferPointer, setIfPmRequestBuffer.Length);
        }

        [Benchmark]
        public void CustomObjectCommand()
        {
            _ = session.TryConsumeMessages(myDictSetGetRequestBufferPointer, myDictSetGetRequestBuffer.Length);
        }

        [Benchmark]
        public void CustomTransaction()
        {
            _ = session.TryConsumeMessages(ctxnsetBufferPointer, ctxnsetBuffer.Length);
        }

        [Benchmark]
        public void CustomProcedure()
        {
            _ = session.TryConsumeMessages(cprocsetBufferPointer, cprocsetBuffer.Length);
        }
    }
}