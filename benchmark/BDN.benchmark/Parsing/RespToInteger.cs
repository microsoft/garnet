// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text;
using BenchmarkDotNet.Attributes;
using Garnet.common;

namespace BDN.benchmark.Parsing
{
    /// <summary>
    /// Benchmark for RespToInteger
    /// </summary>
    [MemoryDiagnoser]
    public unsafe class RespToInteger
    {
        [Benchmark]
        [ArgumentsSource(nameof(LengthHeaderValues))]
        public int ReadLengthHeader(AsciiTestCase testCase)
        {
            fixed (byte* inputPtr = testCase.Bytes)
            {
                var start = inputPtr;
                RespReadUtils.ReadSignedLengthHeader(out var value, ref start, start + testCase.Bytes.Length);
                return value;
            }
        }

        [Benchmark]
        [ArgumentsSource(nameof(SignedInt64EncodedValues))]
        public long ReadInt64(AsciiTestCase testCase)
        {
            fixed (byte* inputPtr = testCase.Bytes)
            {
                var start = inputPtr;
                RespReadUtils.Read64Int(out var value, ref start, start + testCase.Bytes.Length);
                return value;
            }
        }

        [Benchmark]
        [ArgumentsSource(nameof(SignedInt32EncodedValuesWithLengthHeader))]
        public int ReadIntWithLengthHeader(AsciiTestCase testCase)
        {
            fixed (byte* inputPtr = testCase.Bytes)
            {
                var start = inputPtr;
                RespReadUtils.ReadIntWithLengthHeader(out var value, ref start, start + testCase.Bytes.Length);
                return value;
            }
        }

        [Benchmark]
        [ArgumentsSource(nameof(SignedInt64EncodedValuesWithLengthHeader))]
        public long ReadLongWithLengthHeader(AsciiTestCase testCase)
        {
            fixed (byte* inputPtr = testCase.Bytes)
            {
                var start = inputPtr;
                RespReadUtils.ReadLongWithLengthHeader(out var value, ref start, start + testCase.Bytes.Length);
                return value;
            }
        }

        [Benchmark]
        [ArgumentsSource(nameof(UnsignedInt64EncodedValuesWithLengthHeader))]
        public ulong ReadULongWithLengthHeader(AsciiTestCase testCase)
        {
            fixed (byte* inputPtr = testCase.Bytes)
            {
                var start = inputPtr;
                RespReadUtils.ReadULongWithLengthHeader(out var value, ref start, start + testCase.Bytes.Length);
                return value;
            }
        }

        public static IEnumerable<object> SignedInt32EncodedValues
            => ToRespIntegerTestCases(IntegerToResp.SignedInt32Values);

        public static IEnumerable<object> LengthHeaderValues
           => ToRespLengthHeaderTestCases(IntegerToResp.SignedInt32Values);

        public static IEnumerable<object> SignedInt64EncodedValues
            => ToRespIntegerTestCases(IntegerToResp.SignedInt64Values);

        public static IEnumerable<object> UnsignedInt64EncodedValues
            => ToRespIntegerTestCases(IntegerToResp.UnsignedInt64Values);

        public static IEnumerable<object> SignedInt32EncodedValuesWithLengthHeader
            => ToRespIntegerWithLengthHeader(IntegerToResp.SignedInt32Values);

        public static IEnumerable<object> SignedInt64EncodedValuesWithLengthHeader
            => ToRespIntegerWithLengthHeader(IntegerToResp.SignedInt64Values);

        public static IEnumerable<object> UnsignedInt64EncodedValuesWithLengthHeader
            => ToRespIntegerWithLengthHeader(IntegerToResp.UnsignedInt64Values);

        public static IEnumerable<AsciiTestCase> ToRespIntegerTestCases<T>(T[] integerValues) where T : struct
            => integerValues.Select(testCase => new AsciiTestCase($":{testCase}\r\n"));

        public static IEnumerable<AsciiTestCase> ToRespLengthHeaderTestCases<T>(T[] integerValues) where T : struct
            => integerValues.Select(testCase => new AsciiTestCase($"${testCase}\r\n"));

        public static IEnumerable<AsciiTestCase> ToRespIntegerWithLengthHeader<T>(T[] integerValues) where T : struct
            => integerValues.Select(testCase => new AsciiTestCase($"${testCase.ToString()?.Length ?? 0}\r\n{testCase}\r\n"));

        public sealed class AsciiTestCase
        {
            public byte[] Bytes { get; }
            private string Text { get; }

            public AsciiTestCase(string text)
            {
                Text = text;
                Bytes = Encoding.ASCII.GetBytes(text);
            }

            public override string ToString() => Text; // displayed by BDN
        }
    }
}