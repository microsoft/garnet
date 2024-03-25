// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text;
using Garnet.common;
using BenchmarkDotNet.Attributes;

namespace BenchmarkDotNet.benchmark.Resp
{
    public unsafe class RespIntegerReadBenchmarks
    {
        private const int OperationsPerInvoke = 16;

        [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
        [ArgumentsSource(nameof(SignedInt32EncodedValues))]
        public int ReadInt32(AsciiTestCase testCase)
        {
            int result = 0;
            fixed (byte* inputPtr = testCase.Bytes)
            {
                for (int i = 0; i < OperationsPerInvoke; i++)
                {
                    var start = inputPtr;
                    _ = RespReadUtils.ReadInt(out var value, ref start, start + testCase.Bytes.Length);
                    result += value;
                }
            }
            return result;
        }

        [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
        [ArgumentsSource(nameof(SignedInt64EncodedValues))]
        public long ReadInt64(AsciiTestCase testCase)
        {
            long result = 0;
            fixed (byte* inputPtr = testCase.Bytes)
            {
                for (int i = 0; i < OperationsPerInvoke; i++)
                {
                    var start = inputPtr;
                    _ = RespReadUtils.Read64Int(out var value, ref start, start + testCase.Bytes.Length);
                    result += value;
                }
            }
            return result;
        }

        [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
        [ArgumentsSource(nameof(SignedInt32EncodedValuesWithLengthHeader))]
        public int ReadIntWithLengthHeader(AsciiTestCase testCase)
        {
            int result = 0;
            fixed (byte* inputPtr = testCase.Bytes)
            {
                for (int i = 0; i < OperationsPerInvoke; i++)
                {
                    var start = inputPtr;
                    _ = RespReadUtils.ReadIntWithLengthHeader(out var value, ref start, start + testCase.Bytes.Length);
                    result += value;
                }
            }
            return result;
        }

        [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
        [ArgumentsSource(nameof(SignedInt64EncodedValuesWithLengthHeader))]
        public long ReadLongWithLengthHeader(AsciiTestCase testCase)
        {
            long result = 0;
            fixed (byte* inputPtr = testCase.Bytes)
            {
                for (int i = 0; i < OperationsPerInvoke; i++)
                {
                    var start = inputPtr;
                    _ = RespReadUtils.ReadLongWithLengthHeader(out var value, ref start, start + testCase.Bytes.Length);
                    result += value;
                }
            }
            return result;
        }

        [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
        [ArgumentsSource(nameof(UnsignedInt64EncodedValuesWithLengthHeader))]
        public ulong ReadULongWithLengthHeader(AsciiTestCase testCase)
        {
            ulong result = 0;
            fixed (byte* inputPtr = testCase.Bytes)
            {
                for (int i = 0; i < OperationsPerInvoke; i++)
                {
                    var start = inputPtr;
                    _ = RespReadUtils.ReadULongWithLengthHeader(out var value, ref start, start + testCase.Bytes.Length);
                    result += value;
                }
            }
            return result;
        }

        public static IEnumerable<object> SignedInt32EncodedValues
            => ToRespIntegerTestCases(RespIntegerWriteBenchmarks.SignedInt32Values);

        public static IEnumerable<object> SignedInt64EncodedValues
            => ToRespIntegerTestCases(RespIntegerWriteBenchmarks.SignedInt64Values);

        public static IEnumerable<object> UnsignedInt64EncodedValues
            => ToRespIntegerTestCases(RespIntegerWriteBenchmarks.UnsignedInt64Values);

        public static IEnumerable<object> SignedInt32EncodedValuesWithLengthHeader
            => ToRespIntegerWithLengthHeader(RespIntegerWriteBenchmarks.SignedInt32Values);

        public static IEnumerable<object> SignedInt64EncodedValuesWithLengthHeader
            => ToRespIntegerWithLengthHeader(RespIntegerWriteBenchmarks.SignedInt64Values);

        public static IEnumerable<object> UnsignedInt64EncodedValuesWithLengthHeader
            => ToRespIntegerWithLengthHeader(RespIntegerWriteBenchmarks.UnsignedInt64Values);

        public static IEnumerable<AsciiTestCase> ToRespIntegerTestCases<T>(T[] integerValues) where T : struct
            => integerValues.Select(testCase => new AsciiTestCase($":{testCase}\r\n"));

        public static IEnumerable<AsciiTestCase> ToRespIntegerWithLengthHeader<T>(T[] integerValues) where T : struct
            => integerValues.Select(testCase => new AsciiTestCase($"${testCase.ToString()?.Length ?? 0}\r\n{testCase}\r\n"));

        public sealed class AsciiTestCase(string text)
        {
            public byte[] Bytes { get; } = Encoding.ASCII.GetBytes(text);

            public override string ToString() => text; // displayed by BDN
        }
    }
}
