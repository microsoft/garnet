// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using BDN.benchmark.CustomProcs;
using BenchmarkDotNet.Attributes;
using Embedded.perftest;
using Garnet;
using Garnet.server;

namespace BDN.benchmark.Operations
{
    [MemoryDiagnoser]
    public unsafe class ObjectOperations : OperationsBase
    {
        static ReadOnlySpan<byte> ZADDREM => "*4\r\n$4\r\nZADD\r\n$1\r\nc\r\n$1\r\n1\r\n$1\r\nc\r\n*3\r\n$4\r\nZREM\r\n$1\r\nc\r\n$1\r\nc\r\n"u8;
        byte[] zAddRemRequestBuffer;
        byte* zAddRemRequestBufferPointer;

        static ReadOnlySpan<byte> LPUSHPOP => "*3\r\n$5\r\nLPUSH\r\n$1\r\nd\r\n$1\r\ne\r\n*2\r\n$4\r\nLPOP\r\n$1\r\nd\r\n"u8;
        byte[] lPushPopRequestBuffer;
        byte* lPushPopRequestBufferPointer;

        static ReadOnlySpan<byte> SADDREM => "*3\r\n$4\r\nSADD\r\n$1\r\ne\r\n$1\r\na\r\n*3\r\n$4\r\nSREM\r\n$1\r\ne\r\n$1\r\na\r\n"u8;
        byte[] sAddRemRequestBuffer;
        byte* sAddRemRequestBufferPointer;

        static ReadOnlySpan<byte> HSETDEL => "*4\r\n$4\r\nHSET\r\n$1\r\nf\r\n$1\r\na\r\n$1\r\na\r\n*3\r\n$4\r\nHDEL\r\n$1\r\nf\r\n$1\r\na\r\n"u8;
        byte[] hSetDelRequestBuffer;
        byte* hSetDelRequestBufferPointer;

        static ReadOnlySpan<byte> MYDICTSETGET => "*4\r\n$9\r\nMYDICTSET\r\n$2\r\nck\r\n$1\r\nf\r\n$1\r\nv\r\n*3\r\n$9\r\nMYDICTGET\r\n$2\r\nck\r\n$1\r\nf\r\n"u8;
        byte[] myDictSetGetRequestBuffer;
        byte* myDictSetGetRequestBufferPointer;

        static ReadOnlySpan<byte> CPBSET => "*9\r\n$6\r\nCPBSET\r\n$6\r\n{0}000\r\n$6\r\n{0}001\r\n$6\r\n{0}002\r\n$6\r\n{0}003\r\n$6\r\n{0}000\r\n$6\r\n{0}001\r\n$6\r\n{0}002\r\n$6\r\n{0}003\r\n"u8;
        byte[] cpbsetBuffer;
        byte* cpbsetBufferPointer;

        void CreateExtensions()
        {
            var factory = new MyDictFactory();
            server.Register.NewType(factory);
            server.Register.NewCommand("MYDICTSET", CommandType.ReadModifyWrite, factory, new MyDictSet(), new RespCommandsInfo { Arity = 4 });
            server.Register.NewCommand("MYDICTGET", CommandType.Read, factory, new MyDictGet(), new RespCommandsInfo { Arity = 3 });
            server.Register.NewTransactionProc(CustomProcs.CustomProcSet.CommandName, () => new CustomProcSet(), new RespCommandsInfo { Arity = CustomProcs.CustomProcSet.Arity });
        }

        [GlobalSetup]
        public void GlobalSetup()
        {
            Setup();
            CreateExtensions();
            SetupOperation(ref zAddRemRequestBuffer, ref zAddRemRequestBufferPointer, ZADDREM);
            SetupOperation(ref lPushPopRequestBuffer, ref lPushPopRequestBufferPointer, LPUSHPOP);
            SetupOperation(ref sAddRemRequestBuffer, ref sAddRemRequestBufferPointer, SADDREM);
            SetupOperation(ref hSetDelRequestBuffer, ref hSetDelRequestBufferPointer, HSETDEL);
            SetupOperation(ref myDictSetGetRequestBuffer, ref myDictSetGetRequestBufferPointer, MYDICTSETGET);
            SetupOperation(ref cpbsetBuffer, ref cpbsetBufferPointer, CPBSET);

            // Pre-populate data
            SlowConsumeMessage("*4\r\n$4\r\nZADD\r\n$1\r\nc\r\n$1\r\n1\r\n$1\r\nd\r\n"u8);
            SlowConsumeMessage("*3\r\n$5\r\nLPUSH\r\n$1\r\nd\r\n$1\r\nf\r\n"u8);
            SlowConsumeMessage("*3\r\n$4\r\nSADD\r\n$1\r\ne\r\n$1\r\nb\r\n"u8);
            SlowConsumeMessage("*3\r\n$4\r\nHSET\r\n$1\r\nf\r\n$1\r\nb\r\n$1\r\nb\r\n"u8);
            SlowConsumeMessage("*4\r\n$9\r\nMYDICTSET\r\n$2\r\nck\r\n$1\r\nf\r\n$1\r\nv\r\n"u8);
            SlowConsumeMessage(cpbsetBuffer);
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            Cleanup();
        }

        [Benchmark]
        public void ZAddRem()
        {
            _ = session.TryConsumeMessages(zAddRemRequestBufferPointer, zAddRemRequestBuffer.Length);
        }

        [Benchmark]
        public void LPushPop()
        {
            _ = session.TryConsumeMessages(lPushPopRequestBufferPointer, lPushPopRequestBuffer.Length);
        }

        [Benchmark]
        public void SAddRem()
        {
            _ = session.TryConsumeMessages(sAddRemRequestBufferPointer, sAddRemRequestBuffer.Length);
        }

        [Benchmark]
        public void HSetDel()
        {
            _ = session.TryConsumeMessages(hSetDelRequestBufferPointer, hSetDelRequestBuffer.Length);
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