// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// SimdJsonSharp.Bindings helper — wraps the native simdjson full-DOM parser
// to produce ExprToken output, matching the contract of AttributeExtractor
// for apples-to-apples benchmarking.
//
// Each call does: native ParseJson → CreateIterator → MoveToKey → convert to ExprToken.
// This is the slowest of the three approaches because:
//   1. Full DOM parse (Stage 1 + Stage 2) on every call via P/Invoke
//   2. No multi-key API — must re-parse the entire document for each field
//   3. Strings allocate a managed string via GetUtf16String()

#nullable enable

using System.Text;
using Garnet.server.Vector.Filter;
using SimdJsonSharp;

/// <summary>
/// Wraps <see cref="SimdJsonN"/> (SimdJsonSharp.Bindings v1.7.0) to produce
/// <see cref="ExprToken"/> results identical to <see cref="AttributeExtractor"/>.
/// </summary>
internal static unsafe class SimdJsonHelper
{
    /// <summary>Create a null-terminated ASCII key for SimdJsonSharp's MoveToKey API.</summary>
    public static byte[] NullTerminated(string key)
    {
        var bytes = Encoding.ASCII.GetBytes(key);
        var buf = new byte[bytes.Length + 1];
        Array.Copy(bytes, buf, bytes.Length);
        return buf;
    }

    /// <summary>
    /// Extract a single top-level field and return an ExprToken.
    /// Returns default (IsNone) when the key is not found.
    /// </summary>
    public static ExprToken ExtractField(byte[] json, byte[] nullTerminatedKey)
    {
        using var doc = SimdJsonN.ParseJson(json);
        if (!doc.IsValid) return default;
        using var it = doc.CreateIterator();
        fixed (byte* p = nullTerminatedKey)
        {
            if (!it.MoveToKey((sbyte*)p))
                return default;

            if (it.IsInteger) return ExprToken.NewNum(it.GetInteger());
            if (it.IsDouble) return ExprToken.NewNum(it.GetDouble());
            if (it.IsString) return ExprToken.NewStr(it.GetUtf16String());
            if (it.IsTrue) return ExprToken.NewNum(1);
            if (it.IsFalse) return ExprToken.NewNum(0);
        }
        return default;
    }

    /// <summary>
    /// Extract multiple fields. SimdJsonSharp has no multi-key API, so each
    /// field requires a full re-parse (N × ParseJson + MoveToKey).
    /// Returns the number of fields found.
    /// </summary>
    public static int ExtractFields(byte[] json, byte[][] nullTerminatedKeys, ExprToken[] results)
    {
        var found = 0;
        for (var i = 0; i < nullTerminatedKeys.Length; i++)
        {
            results[i] = ExtractField(json, nullTerminatedKeys[i]);
            if (!results[i].IsNone) found++;
        }
        return found;
    }
}
