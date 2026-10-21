// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.server;
using NUnit.Framework;

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
    }
}
