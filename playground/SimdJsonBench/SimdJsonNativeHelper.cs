// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// SimdJsonNativeHelper — P/Invoke wrapper for the native simdjson On-Demand DLL.
//
// This calls into simdjson_native.dll (simdjson 3.x C++ On-Demand API compiled
// to a native Win64 DLL via Zig C++ compiler). Each call does:
//   - sj_extract_field:  ondemand::parser.iterate → obj.find_field → get typed value
//   - sj_extract_fields: single forward pass over all object fields, matching N keys
//
// The native parser handle is created once and reused across calls (amortized alloc).
// String results point into the parser's internal scratch buffer (valid until next call).

#nullable enable

using System.Runtime.InteropServices;
using Garnet.server.Vector.Filter;

[StructLayout(LayoutKind.Sequential)]
internal struct SjFieldResult
{
    public int Type;        // 0=None, 1=Num, 2=Str, 3=Bool(unused - treated as Num), 4=Null
    public int _pad;
    public double NumValue;
    public IntPtr StrPtr;   // valid until next sj_ call
    public uint StrLen;
    public uint _pad2;
}

internal static class SimdJsonNativeHelper
{
    private const string Lib = "simdjson_native";

    [DllImport(Lib, CallingConvention = CallingConvention.Cdecl)]
    private static extern IntPtr sj_parser_create(nuint maxCapacity);

    [DllImport(Lib, CallingConvention = CallingConvention.Cdecl)]
    private static extern void sj_parser_destroy(IntPtr parser);

    [DllImport(Lib, CallingConvention = CallingConvention.Cdecl)]
    private static extern unsafe SjFieldResult sj_extract_field(
        IntPtr parser, byte* jsonBuf, nuint jsonLen, nuint jsonCapacity,
        byte* fieldName, nuint fieldNameLen);

    [DllImport(Lib, CallingConvention = CallingConvention.Cdecl)]
    private static extern unsafe int sj_extract_fields(
        IntPtr parser, byte* jsonBuf, nuint jsonLen, nuint jsonCapacity,
        byte** fieldNames, nuint* fieldLens, nuint fieldCount,
        SjFieldResult* results);

    [DllImport(Lib, CallingConvention = CallingConvention.Cdecl)]
    private static extern IntPtr sj_simdjson_version();

    [DllImport(Lib, CallingConvention = CallingConvention.Cdecl)]
    private static extern IntPtr sj_active_implementation();

    // ── Reusable parser handle ───────────────────────────────────────

    private static readonly IntPtr Parser = sj_parser_create(4096);

    // ── Padded buffer helper ─────────────────────────────────────────
    // simdjson requires SIMDJSON_PADDING (128 bytes) beyond the JSON length.
    // We pre-allocate padded copies for benchmark payloads.

    public static byte[] MakePadded(byte[] json)
    {
        const int SIMDJSON_PADDING = 128;
        var padded = new byte[json.Length + SIMDJSON_PADDING];
        Array.Copy(json, padded, json.Length);
        return padded;
    }

    // ── Public API matching AttributeExtractor contract ──────────────

    public static unsafe ExprToken ExtractField(byte[] paddedJson, int jsonLen, byte[] fieldNameUtf8)
    {
        fixed (byte* jsonPtr = paddedJson)
        fixed (byte* keyPtr = fieldNameUtf8)
        {
            var r = sj_extract_field(
                Parser, jsonPtr, (nuint)jsonLen, (nuint)paddedJson.Length,
                keyPtr, (nuint)fieldNameUtf8.Length);

            return r.Type switch
            {
                1 => ExprToken.NewNum(r.NumValue),                               // SJ_NUM
                2 => ExprToken.NewStr(Marshal.PtrToStringUTF8(r.StrPtr, (int)r.StrLen) ?? ""), // SJ_STR
                4 => ExprToken.NewNull(),                                         // SJ_NULL
                _ => default,
            };
        }
    }

    public static unsafe int ExtractFields(
        byte[] paddedJson, int jsonLen,
        byte[][] fieldNamesUtf8, ExprToken[] results)
    {
        var count = fieldNamesUtf8.Length;
        var sjResults = stackalloc SjFieldResult[count];
        var namesPtrs = stackalloc byte*[count];
        var namesLens = stackalloc nuint[count];

        // Pin all field name arrays
        var handles = new GCHandle[count];
        try
        {
            for (var i = 0; i < count; i++)
            {
                handles[i] = GCHandle.Alloc(fieldNamesUtf8[i], GCHandleType.Pinned);
                namesPtrs[i] = (byte*)handles[i].AddrOfPinnedObject();
                namesLens[i] = (nuint)fieldNamesUtf8[i].Length;
            }

            int found;
            fixed (byte* jsonPtr = paddedJson)
            {
                found = sj_extract_fields(
                    Parser, jsonPtr, (nuint)jsonLen, (nuint)paddedJson.Length,
                    namesPtrs, namesLens, (nuint)count, sjResults);
            }

            for (var i = 0; i < count; i++)
            {
                results[i] = sjResults[i].Type switch
                {
                    1 => ExprToken.NewNum(sjResults[i].NumValue),
                    2 => ExprToken.NewStr(Marshal.PtrToStringUTF8(sjResults[i].StrPtr, (int)sjResults[i].StrLen) ?? ""),
                    4 => ExprToken.NewNull(),
                    _ => default,
                };
            }
            return found;
        }
        finally
        {
            for (var i = 0; i < count; i++)
                if (handles[i].IsAllocated) handles[i].Free();
        }
    }

    // ── Info ─────────────────────────────────────────────────────────

    public static string GetVersion()
        => Marshal.PtrToStringUTF8(sj_simdjson_version()) ?? "?";

    public static string GetImplementation()
        => Marshal.PtrToStringUTF8(sj_active_implementation()) ?? "?";
}
