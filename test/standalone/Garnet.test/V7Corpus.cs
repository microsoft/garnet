// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Text;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// The single source of truth for the data written into, and read back from, a genuinely-downlevel (cv7) checkpoint. One
    /// definition drives both the generator (which populates a running cv7 server before it SAVEs) and the verifier (which reads
    /// the recovered store back and compares byte-for-byte), so the two can never drift and the verification is exact rather than
    /// a weaker "something survived" check.
    /// </summary>
    /// <remarks>
    /// <para>
    /// WHAT IT SWEEPS. Every record here has at least one out-of-line component, because a fully-inline record has nothing the
    /// cv7-&gt;cv8 object-log up-conversion would touch. The five <see cref="Shape"/>s cover the overflow-key, overflow-value,
    /// both-overflow, object-value, and object-value-with-overflow-key permutations; one object type (Hash) is sufficient, as the
    /// framing is per out-of-line component and does not depend on the object kind. Component sizes bracket the format seams: the
    /// 511-byte exact-size cutoff (dense/headerless at or below, chunk-framed above), object-log page and multi-page spans, and the
    /// object-log segment boundary (the artifacts are produced and recovered with a 4 MB object-log segment, so the multi-megabyte
    /// entries cross it).
    /// </para>
    /// <para>
    /// INLINE vs OVERFLOW. "Inline" components are a handful of bytes and "overflow" components are 510 bytes or more, so a single
    /// moderate inline cutoff (the generator launches with <c>--max-inline-key-size 256 --max-inline-value-size 256</c>) places each
    /// one deterministically: inline below the cutoff, overflow above it. A cutoff of zero would be wrong, forcing even the "inline"
    /// side out-of-line and destroying the inline-key / inline-value permutations.
    /// </para>
    /// <para>
    /// DETERMINISM. Keys and payloads are a pure function of the entry's label via a process-stable hash (not
    /// <see cref="string.GetHashCode()"/>, which is randomized per run), so the generator and the verifier compute identical bytes
    /// without sharing any state.
    /// </para>
    /// </remarks>
    internal static class V7Corpus
    {
        /// <summary>How thoroughly to sweep component sizes.</summary>
        internal enum Profile
        {
            /// <summary>Every format seam, including multi-megabyte and object-log-segment-crossing components. For the external cv7 generator.</summary>
            Full,

            /// <summary>A reduced sweep that still crosses the dense/chunk cutoff and one object-log segment. For the in-process current-binary round-trip that runs in CI.</summary>
            Smoke,
        }

        /// <summary>The out-of-line record shapes. A fully-inline record is intentionally absent: it has no out-of-line component.</summary>
        internal enum Shape
        {
            /// <summary>Overflow key, small inline raw value.</summary>
            OverflowKeyOnly,

            /// <summary>Small inline key, overflow raw value.</summary>
            OverflowValueOnly,

            /// <summary>Overflow key and overflow raw value.</summary>
            BothOverflow,

            /// <summary>Small inline key, Hash value.</summary>
            ObjectValueOnly,

            /// <summary>Overflow key, Hash value.</summary>
            ObjectValueOverflowKey,
        }

        /// <summary>Bytes short enough to stay inline under the cutoff the generator is launched with.</summary>
        const int InlineLen = 8;

        /// <summary>A representative overflow-key length for the object-with-overflow-key shape: just past the 511-byte dense/chunk cutoff.</summary>
        const int OverflowKeyForObject = 600;

        /// <summary>One entry of the corpus: a single key holding either a raw string or a Hash.</summary>
        internal sealed class Entry
        {
            /// <summary>A short, unique, human-readable tag used in assertion messages and as the seed of the deterministic bytes.</summary>
            internal string Label { get; init; }

            /// <summary>The record key. Long when the shape overflows the key, short otherwise.</summary>
            internal byte[] Key { get; init; }

            /// <summary>True when the value is a Hash; false when it is a raw string.</summary>
            internal bool IsHash { get; init; }

            /// <summary>The raw value, when <see cref="IsHash"/> is false.</summary>
            internal byte[] RawValue { get; init; }

            /// <summary>The hash fields, when <see cref="IsHash"/> is true.</summary>
            internal (byte[] Field, byte[] Value)[] Fields { get; init; }
        }

        /// <summary>Build the corpus for a profile. Deterministic and side-effect free.</summary>
        internal static IReadOnlyList<Entry> Build(Profile profile)
        {
            var entries = new List<Entry>();

            foreach (var size in RawKeySizes(profile))
                entries.Add(MakeRaw(Shape.OverflowKeyOnly, keyLen: size, valueLen: InlineLen, sizeTag: size));

            foreach (var size in RawValueSizes(profile))
                entries.Add(MakeRaw(Shape.OverflowValueOnly, keyLen: InlineLen, valueLen: size, sizeTag: size));

            foreach (var size in BothOverflowSizes(profile))
                entries.Add(MakeRaw(Shape.BothOverflow, keyLen: size, valueLen: size, sizeTag: size));

            foreach (var (fieldCount, fieldValueLen) in ObjectSpecs(profile))
                entries.Add(MakeHash(Shape.ObjectValueOnly, keyLen: InlineLen, fieldCount, fieldValueLen));

            foreach (var (fieldCount, fieldValueLen) in ObjectWithOverflowKeySpecs(profile))
                entries.Add(MakeHash(Shape.ObjectValueOverflowKey, keyLen: OverflowKeyForObject, fieldCount, fieldValueLen));

            return entries;
        }

        /// <summary>Write every entry of the corpus into the database.</summary>
        internal static void Populate(IDatabase db, IReadOnlyList<Entry> corpus)
        {
            foreach (var e in corpus)
            {
                RedisKey key = e.Key;
                if (e.IsHash)
                {
                    var hashEntries = new HashEntry[e.Fields.Length];
                    for (var i = 0; i < e.Fields.Length; i++)
                        hashEntries[i] = new HashEntry(e.Fields[i].Field, e.Fields[i].Value);
                    db.HashSet(key, hashEntries);
                }
                else
                {
                    _ = db.StringSet(key, e.RawValue);
                }
            }
        }

        /// <summary>Read every entry back and assert it survived byte-for-byte.</summary>
        internal static void Verify(IDatabase db, IReadOnlyList<Entry> corpus)
        {
            foreach (var e in corpus)
            {
                RedisKey key = e.Key;
                if (e.IsHash)
                {
                    ClassicAssert.AreEqual(RedisType.Hash, db.KeyType(key), $"{e.Label}: recovered as the wrong type");
                    ClassicAssert.AreEqual(e.Fields.Length, db.HashLength(key), $"{e.Label}: field count changed across recovery");
                    foreach (var (field, value) in e.Fields)
                    {
                        var got = db.HashGet(key, field);
                        ClassicAssert.IsTrue(got.HasValue, $"{e.Label}: lost field across recovery");
                        CollectionAssert.AreEqual(value, (byte[])got, $"{e.Label}: field value differs after recovery");
                    }
                }
                else
                {
                    ClassicAssert.AreEqual(RedisType.String, db.KeyType(key), $"{e.Label}: recovered as the wrong type");
                    var got = db.StringGet(key);
                    ClassicAssert.IsTrue(got.HasValue, $"{e.Label}: lost key across recovery");
                    CollectionAssert.AreEqual(e.RawValue, (byte[])got, $"{e.Label}: value differs after recovery");
                }
            }
        }

        static Entry MakeRaw(Shape shape, int keyLen, int valueLen, int sizeTag)
        {
            var label = $"{shape}:{sizeTag}";
            return new Entry
            {
                Label = label,
                Key = MakeBytes(label + ":key", keyLen),
                IsHash = false,
                RawValue = MakeBytes(label + ":val", valueLen),
            };
        }

        static Entry MakeHash(Shape shape, int keyLen, int fieldCount, int fieldValueLen)
        {
            var label = $"{shape}:n{fieldCount}:v{fieldValueLen}";
            var fields = new (byte[] Field, byte[] Value)[fieldCount];
            for (var i = 0; i < fieldCount; i++)
                fields[i] = (Encoding.ASCII.GetBytes($"f{i:D6}"), MakeBytes($"{label}:{i}", fieldValueLen));
            return new Entry
            {
                Label = label,
                Key = MakeBytes(label + ":key", keyLen),
                IsHash = true,
                Fields = fields,
            };
        }

        /// <summary>
        /// A deterministic byte buffer of the requested length, unique to <paramref name="seed"/>. The buffer is filled from a
        /// per-seed pseudo-random stream, so two buffers of the same length built from different seeds differ across their whole
        /// length. A short, legible ASCII tag is overlaid at the front only when the buffer is long enough to retain pseudo-random
        /// tail bytes; an 8-byte inline key is therefore entirely pseudo-random rather than a shared ASCII prefix, which is what
        /// keeps distinct inline keys from colliding.
        /// </summary>
        static byte[] MakeBytes(string seed, int length)
        {
            var buf = new byte[length];

            // Fill the whole buffer from a 64-bit xorshift stream seeded by a stable per-seed hash.
            var state = StableSeed(seed);
            for (var i = 0; i < length; i++)
            {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                buf[i] = (byte)state;
            }

            // Overlay a readable prefix for dumps, but keep at least 8 pseudo-random tail bytes so distinct seeds cannot collide.
            var prefix = Encoding.ASCII.GetBytes(seed);
            var overlay = Math.Min(prefix.Length, Math.Max(0, length - 8));
            Array.Copy(prefix, buf, overlay);

            return buf;
        }

        /// <summary>FNV-1a (64-bit) over the seed. Stable across processes, unlike <see cref="string.GetHashCode()"/>.</summary>
        static ulong StableSeed(string seed)
        {
            ulong hash = 14695981039346656037;
            foreach (var c in seed)
                hash = (hash ^ c) * 1099511628211;
            // Keep the xorshift state non-zero.
            return hash == 0 ? 1ul : hash;
        }

        const int Mb = 1 << 20;

        /// <summary>Overflow-key lengths. All exceed the 256-byte inline cutoff, so each is stored out-of-line.</summary>
        static int[] RawKeySizes(Profile profile) => profile == Profile.Full
            ? [510, 511, 512, 513, 1024, 4095, 4096, 4097, 8192, 65536, 4 * Mb - 1, 4 * Mb, 4 * Mb + 1, 5 * Mb]
            : [510, 511, 512, 513, 8192, 5 * Mb];

        /// <summary>Overflow raw-value lengths, bracketing the same seams as the keys.</summary>
        static int[] RawValueSizes(Profile profile) => RawKeySizes(profile);

        /// <summary>Both-overflow sizes, applied to key and value together; the multi-megabyte entry is kept to one to bound the data.</summary>
        static int[] BothOverflowSizes(Profile profile) => profile == Profile.Full
            ? [510, 511, 512, 513, 4096, 65536, 5 * Mb]
            : [512, 5 * Mb];

        /// <summary>
        /// Hash specs as (field count, per-field value length). The serialized object spans grow from a single tiny field through
        /// multi-field and multi-page objects to one that crosses a 4 MB object-log segment. A zero-field hash is not representable
        /// (an empty hash is a deletion), so the low end is a single smallest-non-empty field.
        /// </summary>
        static (int FieldCount, int FieldValueLen)[] ObjectSpecs(Profile profile) => profile == Profile.Full
            ?
            [
                (1, 1),            // smallest non-empty
                (1, 500),          // just under the 511 cutoff
                (1, 512),          // just over the cutoff -> chunk-framed
                (1, 8192),         // multi-page value
                (8, 1024),         // several fields
                (256, 1024),       // many fields, multi-page object
                (1, 5 * Mb),       // single field crossing a 4 MB object-log segment
                (16, 512 * 1024),  // 8 MB across many fields, also crossing a segment
            ]
            :
            [
                (1, 1),
                (1, 512),
                (8, 1024),
                (1, 5 * Mb),       // crosses one object-log segment
            ];

        /// <summary>Hash specs for the overflow-key shape; a representative subset, since the key-size sweep is covered by the raw shapes.</summary>
        static (int FieldCount, int FieldValueLen)[] ObjectWithOverflowKeySpecs(Profile profile) => profile == Profile.Full
            ?
            [
                (1, 1),
                (1, 512),
                (256, 1024),
                (1, 5 * Mb),
            ]
            :
            [
                (1, 512),
            ];
    }
}