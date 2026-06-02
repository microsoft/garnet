// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet.test.cluster
{
    // Write-only custom raw-string command: overwrites the value with the first input arg
    sealed class TestClusterRawStringCmd : CustomRawStringFunctions
    {
        public override bool Reader(ReadOnlySpan<byte> key, ref StringInput input, ReadOnlySpan<byte> value, ref RespMemoryWriter writer, ref ReadInfo readInfo)
            => throw new InvalidOperationException();

        public override bool NeedInitialUpdate(scoped ReadOnlySpan<byte> key, ref StringInput input, ref RespMemoryWriter writer)
            => true;

        public override int GetInitialLength(ref StringInput input)
            => GetFirstArg(ref input).Length;

        public override bool InitialUpdater(ReadOnlySpan<byte> key, ref StringInput input, Span<byte> value, ref RespMemoryWriter writer, ref RMWInfo rmwInfo)
        {
            GetFirstArg(ref input).CopyTo(value);
            return true;
        }

        public override bool InPlaceUpdater(ReadOnlySpan<byte> key, ref StringInput input, Span<byte> value, ref int valueLength, ref RespMemoryWriter writer, ref RMWInfo rmwInfo)
        {
            var newVal = GetFirstArg(ref input);
            if (newVal.Length > value.Length)
                return false; // fall back to CopyUpdater

            newVal.CopyTo(value);
            valueLength = newVal.Length;
            return true;
        }

        public override bool NeedCopyUpdate(ReadOnlySpan<byte> key, ref StringInput input, ReadOnlySpan<byte> oldValue, ref RespMemoryWriter writer)
            => true;

        public override int GetLength(ReadOnlySpan<byte> value, ref StringInput input)
            => GetFirstArg(ref input).Length;

        public override bool CopyUpdater(ReadOnlySpan<byte> key, ref StringInput input, ReadOnlySpan<byte> oldValue, Span<byte> newValue, ref RespMemoryWriter writer, ref RMWInfo rmwInfo)
        {
            GetFirstArg(ref input).CopyTo(newValue);
            return true;
        }
    }

    // Read-only custom raw-string command for the readOnly=true slot-verification branch
    sealed class TestClusterRawStringReadCmd : CustomRawStringFunctions
    {
        public override bool Reader(ReadOnlySpan<byte> key, ref StringInput input, ReadOnlySpan<byte> value, ref RespMemoryWriter writer, ref ReadInfo readInfo)
        {
            writer.WriteBulkString(value);
            return true;
        }

        // The dispatcher only invokes write methods for ReadModifyWrite commands, so throws
        // here surface accidental write-path invocations as test failures instead of silent state corruption
        public override bool NeedInitialUpdate(scoped ReadOnlySpan<byte> key, ref StringInput input, ref RespMemoryWriter writer)
            => throw new InvalidOperationException();

        public override int GetInitialLength(ref StringInput input)
            => throw new InvalidOperationException();

        public override bool InitialUpdater(ReadOnlySpan<byte> key, ref StringInput input, Span<byte> value, ref RespMemoryWriter writer, ref RMWInfo rmwInfo)
            => throw new InvalidOperationException();

        public override bool InPlaceUpdater(ReadOnlySpan<byte> key, ref StringInput input, Span<byte> value, ref int valueLength, ref RespMemoryWriter writer, ref RMWInfo rmwInfo)
            => throw new InvalidOperationException();

        public override int GetLength(ReadOnlySpan<byte> value, ref StringInput input)
            => throw new InvalidOperationException();

        public override bool CopyUpdater(ReadOnlySpan<byte> key, ref StringInput input, ReadOnlySpan<byte> oldValue, Span<byte> newValue, ref RespMemoryWriter writer, ref RMWInfo rmwInfo)
            => throw new InvalidOperationException();
    }

    sealed class TestClusterObjFactory : CustomObjectFactory
    {
        public override CustomObjectBase Create(byte type)
            => new TestClusterObj(type);

        public override CustomObjectBase Deserialize(byte type, BinaryReader reader)
            => new TestClusterObj(type);
    }

    // Empty stub used only to satisfy the CustomObjectBase contract; tests assert dispatch routing only
    sealed class TestClusterObj : CustomObjectBase
    {
        public TestClusterObj(byte type) : base(type) { }

        public TestClusterObj(TestClusterObj obj) : base(obj) { }

        public override CustomObjectBase CloneObject() => new TestClusterObj(this);

        public override void SerializeObject(BinaryWriter writer) { }

        public override void Dispose() { }

        public override unsafe void Scan(long start, out List<byte[]> items, out long cursor, int count = 10, byte* pattern = null, int patternLength = 0, bool isNoValue = false)
        {
            items = [];
            cursor = 0;
        }
    }

    sealed class TestClusterObjSet : CustomObjectFunctions
    {
        public override bool NeedInitialUpdate(scoped ReadOnlySpan<byte> key, ref ObjectInput input, ref RespMemoryWriter writer)
            => true;

        // No-op; returning true emits the default +OK reply
        public override bool Updater(ReadOnlySpan<byte> key, ref ObjectInput input, IGarnetObject value, ref RespMemoryWriter writer, ref RMWInfo rmwInfo)
            => true;
    }

    // Read-only custom object command for the readOnly=true slot-verification branch
    sealed class TestClusterObjGet : CustomObjectFunctions
    {
        public override bool Reader(ReadOnlySpan<byte> key, ref ObjectInput input, IGarnetObject value, ref RespMemoryWriter writer, ref ReadInfo readInfo)
        {
            writer.WriteNull();
            return true;
        }
    }

    internal class CLUSTERTESTRAWCMD : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(CLUSTERTESTRAWCMD);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], "value"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class CLUSTERTESTOBJCMD : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(CLUSTERTESTOBJCMD);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], "value"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    // Read-mode custom raw-string command: only the key argument is needed
    internal class CLUSTERTESTRAWREADCMD : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(CLUSTERTESTRAWREADCMD);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0]];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    // Read-mode custom object command: only the key argument is needed
    internal class CLUSTERTESTOBJREADCMD : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(CLUSTERTESTOBJREADCMD);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0]];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }
}