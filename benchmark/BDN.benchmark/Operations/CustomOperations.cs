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
        static ReadOnlySpan<byte> MYDICTSETGET => "*4\r\n$9\r\nMYDICTSET\r\n$2\r\nck\r\n$1\r\nf\r\n$1\r\nv\r\n*3\r\n$9\r\nMYDICTGET\r\n$2\r\nck\r\n$1\r\nf\r\n"u8;
        byte[] myDictSetGetRequestBuffer;
        byte* myDictSetGetRequestBufferPointer;

        static ReadOnlySpan<byte> CPBSET => "*9\r\n$6\r\nCPBSET\r\n$6\r\n{0}000\r\n$6\r\n{0}001\r\n$6\r\n{0}002\r\n$6\r\n{0}003\r\n$6\r\n{0}000\r\n$6\r\n{0}001\r\n$6\r\n{0}002\r\n$6\r\n{0}003\r\n"u8;
        byte[] cpbsetBuffer;
        byte* cpbsetBufferPointer;

        void CreateExtensions()
        {
            // Register custom object type and commands
            var factory = new MyDictFactory();
            server.Register.NewType(factory);
            server.Register.NewCommand("MYDICTSET", CommandType.ReadModifyWrite, factory, new MyDictSet(), new RespCommandsInfo { Arity = 4 });
            server.Register.NewCommand("MYDICTGET", CommandType.Read, factory, new MyDictGet(), new RespCommandsInfo { Arity = 3 });

            // Register custom transaction
            server.Register.NewTransactionProc(CustomProcs.CustomProcSet.CommandName, () => new CustomProcSet(), new RespCommandsInfo { Arity = CustomProcs.CustomProcSet.Arity });
        }
        public override void GlobalSetup()
        {
            base.GlobalSetup();
            CreateExtensions();

            SetupOperation(ref myDictSetGetRequestBuffer, ref myDictSetGetRequestBufferPointer, MYDICTSETGET);
            SetupOperation(ref cpbsetBuffer, ref cpbsetBufferPointer, CPBSET);
            SlowConsumeMessage("*4\r\n$9\r\nMYDICTSET\r\n$2\r\nck\r\n$1\r\nf\r\n$1\r\nv\r\n"u8);
            SlowConsumeMessage(cpbsetBuffer);
        }

        [Benchmark]
        public void MyDictSetGet()
        {
            _ = session.TryConsumeMessages(myDictSetGetRequestBufferPointer, myDictSetGetRequestBuffer.Length);
        }

        [Benchmark]
        public void CustomProcSet()
        {
            _ = session.TryConsumeMessages(cpbsetBufferPointer, cpbsetBuffer.Length);
        }
    }
}