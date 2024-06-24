// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Text;
using Garnet.common;

namespace Garnet.test.cluster
{
    public static class RandomUtils
    {
        static readonly byte[] asciiChars = Encoding.ASCII.GetBytes("abcdefghijklmnopqrstvuwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789");
        static readonly Random random = new(674386);

        public static void RandomBytes(ref byte[] data, int startOffset = -1, int endOffset = -1)
        {
            startOffset = startOffset == -1 ? 0 : startOffset;
            endOffset = endOffset == -1 ? data.Length : endOffset;
            for (var i = startOffset; i < endOffset; i++)
                data[i] = asciiChars[random.Next(asciiChars.Length)];
        }
    }

    public abstract class BaseCommand
    {
        public static ReadOnlySpan<byte> HashTag => "{1234}"u8;

        public abstract bool IsReadOnly { get; }
        public abstract bool IsArrayCommand { get; }
        public abstract bool ArrayResponse { get; }
        public abstract ReadOnlySpan<byte> Command { get; }

        public BaseCommand()
        {
            GetSingleSlotKeys = singleSlotKeys();
            GetCrossSlotKeys = crossSlotKeys();
        }

        public new string ToString => Encoding.ASCII.GetString(Command);

        public int GetSlot => HashSlotUtils.HashSlot(Encoding.ASCII.GetBytes(GetSingleSlotKeys[0]));

        public List<string> GetSingleSlotKeys { get; }
        public List<string> GetCrossSlotKeys { get; }

        public abstract string[] GetSingleSlotRequest();
        public abstract string[] GetCrossSlotRequest();
        public abstract ArraySegment<string> SetupSingleSlotRequest();

        private List<string> singleSlotKeys(int klen = 16, int kcount = 32, int kEndTag = 4)
        {
            var ssk = new List<string>();
            var key = new byte[klen];
            RandomUtils.RandomBytes(ref key);
            HashTag.CopyTo(key.AsSpan());

            for (var i = 0; i < kcount; i++)
            {
                RandomUtils.RandomBytes(ref key, startOffset: HashTag.Length);
                ssk.Add(Encoding.ASCII.GetString(key));
            }

            return ssk;
        }

        private List<string> crossSlotKeys(int klen = 16, int kcount = 32)
        {
            var csk = new List<string>();
            var key = new byte[klen];
            for (var i = 0; i < kcount; i++)
            {
                RandomUtils.RandomBytes(ref key);
                csk.Add(Encoding.ASCII.GetString(key));
            }
            return csk;
        }

        public string[] GetSingleSlotRequestWithCommand
        {
            get
            {
                var ssr = GetSingleSlotRequest();
                var args = new string[ssr.Length + 1];
                args[0] = ToString;
                var count = 1;
                foreach (var arg in ssr)
                    args[count++] = arg;
                return args;
            }
        }
        public string[] GetCrossslotRequestWithCommand
        {
            get
            {
                var csr = GetCrossSlotRequest();
                var args = new string[csr.Length + 1];
                args[0] = ToString;
                var count = 1;
                foreach (var arg in csr)
                    args[count++] = arg;
                return args;
            }
        }
    }

    internal class GET : BaseCommand
    {
        public override bool IsReadOnly => true;
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override ReadOnlySpan<byte> Command => "GET"u8;

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0]];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string> SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class SET : BaseCommand
    {
        public override bool IsReadOnly => false;
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override ReadOnlySpan<byte> Command => "SET"u8;

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], "value1"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string> SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class MGET : BaseCommand
    {
        public override bool IsReadOnly => true;
        public override bool IsArrayCommand => true;
        public override bool ArrayResponse => true;
        public override ReadOnlySpan<byte> Command => "MGET"u8;

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], ssk[1], ssk[2]];
        }

        public override string[] GetCrossSlotRequest()
        {
            var ssk = GetCrossSlotKeys;
            return [ssk[0], ssk[1], ssk[2]];
        }

        public override ArraySegment<string> SetupSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            var setup = new ArraySegment<string>(["MSET", ssk[1], "value1", ssk[2], "value", ssk[3], "value2"]);
            return setup;
        }
    }

    internal class MSET : BaseCommand
    {
        public override bool IsReadOnly => false;
        public override bool IsArrayCommand => true;
        public override bool ArrayResponse => false;
        public override ReadOnlySpan<byte> Command => "MSET"u8;

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], "value1", ssk[1], "value", ssk[2], "value2"];
        }

        public override string[] GetCrossSlotRequest()
        {
            var ssk = GetCrossSlotKeys;
            return [ssk[0], "value1", ssk[1], "value", ssk[2], "value2"];
        }

        public override ArraySegment<string> SetupSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            var setup = new ArraySegment<string>(["MSET", ssk[1], "value1", ssk[2], "value", ssk[3], "value2"]);
            return setup;
        }
    }
}