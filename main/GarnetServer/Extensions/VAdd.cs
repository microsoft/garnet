// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers.Binary;
using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    /// <summary>
    /// VADD key dim dtype payload
    ///
    /// Stores a vector as the value for 'key'.
    /// Value layout (little-endian):
    /// [0]    : byte version = 1
    /// [1]    : byte dtype   (1=f32, 2=f16, 3=u8)
    /// [2..5] : int32 dim
    /// [6.. ] : payload bytes (length = dim * sizeof(dtype))
    ///
    /// Examples (StackExchange.Redis):
    ///   db.Execute("VADD", "v:1", "4", "f32", vectorBytes);
    ///
    /// Examples (redis-cli, last arg from stdin):
    ///   cat vec.bin | redis-cli -x VADD v:1 4 f32
    /// </summary>
    sealed class VAddCustomCommand : CustomRawStringFunctions
    {
        const byte Version = 1;
        const int HeaderLen = 1 + 1 + 4; // version + dtype + dim(int32)

        // Not used for this write-only command
        public override bool Reader(ReadOnlySpan<byte> key, ref RawStringInput input, ReadOnlySpan<byte> value, ref RespMemoryWriter writer, ref ReadInfo readInfo)
            => throw new InvalidOperationException();

        // We can always initialize if key is missing
        public override bool NeedInitialUpdate(ReadOnlySpan<byte> key, ref RawStringInput input, ref RespMemoryWriter writer) => true;

        public override int GetInitialLength(ref RawStringInput input)
        {
            // Args: dim, dtype, payload
            var dimArg = GetFirstArg(ref input);
            var dtypeArg = GetFirstArg(ref input);
            var payload = GetFirstArg(ref input);

            // We don't strictly require matching sizes here, just compute final length.
            // If you want to enforce (payload.Length == dim * elemSize), do it in InitialUpdater.
            return HeaderLen + payload.Length;
        }

        public override bool InitialUpdater(ReadOnlySpan<byte> key, ref RawStringInput input, Span<byte> value, ref RespMemoryWriter writer, ref RMWInfo rmwInfo)
        {
            var dimArg   = GetFirstArg(ref input);
            var dtypeArg = GetFirstArg(ref input);
            var payload  = GetFirstArg(ref input);

            var (dtypeCode, elemSize) = ParseDType(dtypeArg);
            int dim = ParseInt(dimArg);

            // Optional sanity check (uncomment to enforce):
            // if (elemSize <= 0 || payload.Length != dim * elemSize)
            // {
            //     writer.WriteError("ERR VADD payload size does not match dim*dtype");
            //     return true; // we already wrote response
            // }

            WriteHeader(value, dtypeCode, dim);
            payload.CopyTo(value.Slice(HeaderLen));
            return true; // +OK by default
        }

        public override bool InPlaceUpdater(ReadOnlySpan<byte> key, ref RawStringInput input, Span<byte> value, ref int valueLength, ref RespMemoryWriter writer, ref RMWInfo rmwInfo)
        {
            var dimArg   = GetFirstArg(ref input);
            var dtypeArg = GetFirstArg(ref input);
            var payload  = GetFirstArg(ref input);

            var (dtypeCode, elemSize) = ParseDType(dtypeArg);
            int dim = ParseInt(dimArg);

            int newLen = HeaderLen + payload.Length;
            if (newLen != valueLength)
                return false; // trigger CopyUpdater path

            WriteHeader(value, dtypeCode, dim);
            payload.CopyTo(value.Slice(HeaderLen));
            return true; // +OK
        }

        public override bool NeedCopyUpdate(ReadOnlySpan<byte> key, ref RawStringInput input, ReadOnlySpan<byte> oldValue, ref RespMemoryWriter writer)
        {
            var dimArg   = GetFirstArg(ref input);
            var dtypeArg = GetFirstArg(ref input);
            var payload  = GetFirstArg(ref input);

            int newLen = HeaderLen + payload.Length;
            return newLen != oldValue.Length;
        }

        public override int GetLength(ReadOnlySpan<byte> value, ref RawStringInput input)
        {
            var dimArg   = GetFirstArg(ref input);
            var dtypeArg = GetFirstArg(ref input);
            var payload  = GetFirstArg(ref input);
            return HeaderLen + payload.Length;
        }

        public override bool CopyUpdater(ReadOnlySpan<byte> key, ref RawStringInput input, ReadOnlySpan<byte> oldValue, Span<byte> newValue, ref RespMemoryWriter writer, ref RMWInfo rmwInfo)
        {
            var dimArg   = GetFirstArg(ref input);
            var dtypeArg = GetFirstArg(ref input);
            var payload  = GetFirstArg(ref input);

            var (dtypeCode, _) = ParseDType(dtypeArg);
            int dim = ParseInt(dimArg);

            WriteHeader(newValue, dtypeCode, dim);
            payload.CopyTo(newValue.Slice(HeaderLen));
            return true; // +OK
        }

        private static void WriteHeader(Span<byte> dst, byte dtypeCode, int dim)
        {
            dst[0] = Version;
            dst[1] = dtypeCode;
            BinaryPrimitives.WriteInt32LittleEndian(dst.Slice(2, 4), dim);
        }

        private static (byte code, int size) ParseDType(ReadOnlySpan<byte> s)
        {
            // Accepts f32, f16, u8 (lowercase)
            if (s.SequenceEqual("f32"u8)) return (1, 4);
            if (s.SequenceEqual("f16"u8)) return (2, 2);
            if (s.SequenceEqual("u8"u8))  return (3, 1);
            // default/fallback
            return (1, 4); // f32
        }

        private static int ParseInt(ReadOnlySpan<byte> s)
        {
            int i = 0, sign = 1, val = 0;
            if (s.Length > 0 && s[0] == (byte)'-') { sign = -1; i = 1; }
            for (; i < s.Length; i++)
            {
                byte c = s[i];
                if (c < (byte)'0' || c > (byte)'9') break;
                val = checked(val * 10 + (c - (byte)'0'));
            }
            return val * sign;
        }
    }
}

