// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using Tsavorite.core;

namespace Garnet.test
{
    [TestFixture]
    public class RecordUpsertInputTests : TestBase
    {
        [Test]
        public unsafe void RecordOptionalsFollowUnchangedLegacyInput([Values] bool expiry, [Values] bool tagged)
        {
            UnifiedInput input = new(RespCommand.RENAME, RespInputFlags.Deterministic)
            {
                isRecordImport = true,
                importedETag = tagged ? 9876543210 : 0,
                importedExpiration = expiry ? DateTime.UtcNow.AddMinutes(5).Ticks : 0,
            };
            RecordUpsertInput recordInput = new() { input = input };
            byte[] legacyBytes = new byte[input.SerializedLength];
            byte[] recordBytes = new byte[recordInput.SerializedLength];
            fixed (byte* legacyPointer = legacyBytes)
            fixed (byte* recordPointer = recordBytes)
            {
                Assert.That(input.CopyTo(legacyPointer, legacyBytes.Length), Is.EqualTo(legacyBytes.Length));
                Assert.That(recordInput.CopyTo(recordPointer, recordBytes.Length), Is.EqualTo(recordBytes.Length));
                Assert.That(recordBytes.AsSpan(0, legacyBytes.Length).SequenceEqual(legacyBytes), Is.True);
                Assert.That(recordBytes.Length, Is.EqualTo(legacyBytes.Length + 16));
                Assert.That(BitConverter.ToInt64(recordBytes, legacyBytes.Length), Is.EqualTo(input.importedETag));
                Assert.That(BitConverter.ToInt64(recordBytes, legacyBytes.Length + 8), Is.EqualTo(input.importedExpiration));

                UnifiedInput legacyInput = new();
                Assert.That(legacyInput.DeserializeFrom(legacyPointer), Is.EqualTo(legacyBytes.Length));
                Assert.That(legacyInput.isRecordImport, Is.False);
                Assert.That(legacyInput.header.cmd, Is.EqualTo(input.header.cmd));

                RecordUpsertInput recovered = new();
                Assert.That(recovered.DeserializeFrom(recordPointer), Is.EqualTo(recordBytes.Length));
                Assert.That(recovered.input.isRecordImport, Is.True);
                Assert.That(recovered.input.importedETag, Is.EqualTo(input.importedETag));
                Assert.That(recovered.input.importedExpiration, Is.EqualTo(input.importedExpiration));
                Assert.That(recovered.input.header.cmd, Is.EqualTo(input.header.cmd));
                Assert.That(recovered.input.header.flags, Is.EqualTo(input.header.flags));
            }
        }

        [Test]
        public void RecordUpsertHasVersionedKeyValueAndInputShape()
        {
            Assert.That((byte)AofEntryType.UnifiedStoreStringRecordUpsert, Is.EqualTo(0x74));
            Assert.That(AofHeader.AofHeaderVersion, Is.EqualTo(6));
            Assert.That(AofEntryType.UnifiedStoreStringRecordUpsert.HasKey(), Is.True);
            Assert.That(AofEntryType.UnifiedStoreStringRecordUpsert.HasChunkValue(), Is.True);
            Assert.That(AofEntryType.UnifiedStoreStringRecordUpsert.HasChunkInput(), Is.True);
            Assert.That(AofEntryType.UnifiedStoreStringRecordUpsert.HasChunkObjectValue(), Is.False);
        }

        static unsafe void DeserializeRecordInput(byte[] bytes, ref RecordUpsertInput input)
        {
            fixed (byte* source = bytes)
                _ = input.DeserializeFrom(source, bytes.Length);
        }

        [Test]
        public unsafe void NegativeArgumentCountIsRejectedBeforeAllocation()
        {
            RecordUpsertInput original = new() { input = new UnifiedInput(RespCommand.RENAME) };
            byte[] bytes = new byte[original.SerializedLength];
            fixed (byte* destination = bytes)
            {
                _ = original.CopyTo(destination, bytes.Length);
                *(int*)(destination + original.input.header.SpanByte.TotalSize + sizeof(long)) = -1;
            }
            RecordUpsertInput recovered = new();
            Assert.Throws<GarnetException>(() => DeserializeRecordInput(bytes, ref recovered));
            Assert.That(recovered.input.parseState.RootBufferLength, Is.Zero);
        }

        [Test]
        public unsafe void BoundedInputPreservesEmptyAndNonemptyArguments()
        {
            ReadOnlySpan<byte> argument = "arg"u8;
            fixed (byte* argumentPointer = argument)
            {
                UnifiedInput input = new(RespCommand.RENAME) { arg1 = 42, importedETag = 3 };
                input.parseState.InitializeWithArguments(PinnedSpanByte.FromPinnedPointer(argumentPointer, 0),
                    PinnedSpanByte.FromPinnedPointer(argumentPointer, argument.Length));
                RecordUpsertInput original = new() { input = input };
                byte[] bytes = new byte[original.SerializedLength];
                fixed (byte* pointer = bytes)
                {
                    _ = original.CopyTo(pointer, bytes.Length);
                    RecordUpsertInput recovered = new();
                    Assert.That(recovered.DeserializeFrom(pointer, bytes.Length), Is.EqualTo(bytes.Length));
                    Assert.That(recovered.input.arg1, Is.EqualTo(42));
                    Assert.That(recovered.input.importedETag, Is.EqualTo(3));
                    Assert.That(recovered.input.parseState.Count, Is.EqualTo(2));
                    Assert.That(recovered.input.parseState.Parameters[0].Length, Is.Zero);
                    Assert.That(recovered.input.parseState.Parameters[1].ReadOnlySpan.SequenceEqual(argument), Is.True);
                }
            }
        }

        [TestCase("header", -1)]
        [TestCase("header", 0)]
        [TestCase("header", 2)]
        [TestCase("header", 4)]
        [TestCase("header", int.MaxValue)]
        [TestCase("count", -1)]
        [TestCase("count", int.MaxValue)]
        [TestCase("argument", -1)]
        [TestCase("argument", 4)]
        [TestCase("argument", int.MaxValue)]
        public unsafe void CorruptInputPrefixesAreRejectedBeforeAllocation(string field, int length)
        {
            ReadOnlySpan<byte> argument = "arg"u8;
            fixed (byte* argumentPointer = argument)
            {
                UnifiedInput input = new(RespCommand.RENAME);
                input.parseState.InitializeWithArgument(PinnedSpanByte.FromPinnedPointer(argumentPointer, argument.Length));
                RecordUpsertInput original = new() { input = input };
                byte[] bytes = new byte[original.SerializedLength];
                int countOffset = input.header.SpanByte.TotalSize + sizeof(long);
                int prefixOffset = field switch
                {
                    "header" => 0,
                    "count" => countOffset,
                    _ => countOffset + sizeof(int),
                };
                fixed (byte* destination = bytes)
                {
                    _ = original.CopyTo(destination, bytes.Length);
                    *(int*)(destination + prefixOffset) = length;
                }
                RecordUpsertInput recovered = new();
                Assert.Throws<GarnetException>(() => DeserializeRecordInput(bytes, ref recovered));
                Assert.That(recovered.input.parseState.RootBufferLength, Is.Zero);
            }
        }
    }
}