// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;

namespace BDN.benchmark.Operations
{
    /// <summary>
    /// Benchmark for SCRIPT LOAD, SCRIPT EXISTS, EVAL, and EVALSHA
    /// </summary>
    [MemoryDiagnoser]
    public unsafe class ScriptOperations : OperationsBase
    {
        static ReadOnlySpan<byte> SCRIPT_LOAD => "*3\r\n$6\r\nSCRIPT\r\n$4\r\nLOAD\r\n$8\r\nreturn 1\r\n"u8;
        byte[] scriptLoadRequestBuffer;
        byte* scriptLoadRequestBufferPointer;

        static ReadOnlySpan<byte> SCRIPT_EXISTS_LOADED => "*3\r\n$6\r\nSCRIPT\r\n$4\r\nLOAD\r\n$10\r\nreturn nil\r\n"u8;
        
        static ReadOnlySpan<byte> SCRIPT_EXISTS_TRUE => "*3\r\n$6\r\nSCRIPT\r\n$6\r\nEXISTS\r\n$40\r\n79cefb99366d8809d2e903c5f36f50c2b731913f\r\n"u8;
        byte[] scriptExistsTrueRequestBuffer;
        byte* scriptExistsTrueRequestBufferPointer;

        static ReadOnlySpan<byte> SCRIPT_EXISTS_FALSE => "*3\r\n$6\r\nSCRIPT\r\n$6\r\nEXISTS\r\n$40\r\n0000000000000000000000000000000000000000\r\n"u8;
        byte[] scriptExistsFalseRequestBuffer;
        byte* scriptExistsFalseRequestBufferPointer;

        static ReadOnlySpan<byte> EVAL => "*3\r\n$4\r\nEVAL\r\n$10\r\nreturn nil\r\n$1\r\n0\r\n"u8;
        byte[] evalRequestBuffer;
        byte* evalRequestBufferPointer;

        static ReadOnlySpan<byte> EVALSHA => "*3\r\n$7\r\nEVALSHA\r\n$40\r\n79cefb99366d8809d2e903c5f36f50c2b731913f\r\n$1\r\n0\r\n"u8;
        byte[] evalShaRequestBuffer;
        byte* evalShaRequestBufferPointer;

        public override void GlobalSetup()
        {
            base.GlobalSetup();
            
            SetupOperation(ref scriptLoadRequestBuffer, ref scriptLoadRequestBufferPointer, SCRIPT_LOAD);

            byte[] scriptExistsLoadedBuffer = null;
            byte* scriptExistsLoadedPointer = null;
            SetupOperation(ref scriptExistsLoadedBuffer, ref scriptExistsLoadedPointer, SCRIPT_EXISTS_LOADED);
            _ = session.TryConsumeMessages(scriptExistsLoadedPointer, scriptExistsLoadedBuffer.Length);
            SetupOperation(ref scriptExistsTrueRequestBuffer, ref scriptExistsTrueRequestBufferPointer, SCRIPT_EXISTS_TRUE);

            SetupOperation(ref scriptExistsFalseRequestBuffer, ref scriptExistsFalseRequestBufferPointer, SCRIPT_EXISTS_FALSE);

            SetupOperation(ref evalRequestBuffer, ref evalRequestBufferPointer, EVAL);

            SetupOperation(ref evalShaRequestBuffer, ref evalShaRequestBufferPointer, EVALSHA);
        }

        [Benchmark]
        public void ScriptLoad()
        {
            _ = session.TryConsumeMessages(scriptLoadRequestBufferPointer, scriptLoadRequestBuffer.Length);
        }

        [Benchmark]
        public void ScriptExistsTrue()
        {
            _ = session.TryConsumeMessages(scriptExistsTrueRequestBufferPointer, scriptExistsTrueRequestBuffer.Length);
        }

        [Benchmark]
        public void ScriptExistsFalse()
        {
            _ = session.TryConsumeMessages(scriptExistsFalseRequestBufferPointer, scriptExistsFalseRequestBuffer.Length);
        }

        [Benchmark]
        public void Eval()
        {
            _ = session.TryConsumeMessages(evalRequestBufferPointer, evalRequestBuffer.Length);
        }

        [Benchmark]
        public void EvalSha()
        {
            _ = session.TryConsumeMessages(evalShaRequestBufferPointer, evalShaRequestBuffer.Length);
        }
    }
}
