// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text;
using Garnet.common;
using BenchmarkDotNet.Attributes;

namespace BenchmarkDotNet.benchmark.Resp;

public unsafe class RespIntegerReadBenchmarks
{
    [Benchmark]
    [ArgumentsSource(nameof(SignedInt32EncodedValues))]
    public int ReadInt32(AsciiTestCase testCase)
    {
        fixed (byte* inputPtr = testCase.Bytes)
        {
            var start = inputPtr;
            RespReadUtils.ReadInt(out var value, ref start, start + testCase.Bytes.Length);
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
