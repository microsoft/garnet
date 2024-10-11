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

        public abstract bool IsArrayCommand { get; }
        public abstract bool ArrayResponse { get; }
        public virtual bool RequiresExistingKey => false;
        public abstract string Command { get; }

        public BaseCommand()
        {
            GetSingleSlotKeys = singleSlotKeys();
            GetCrossSlotKeys = crossSlotKeys();
        }

        public int GetSlot => HashSlotUtils.HashSlot(Encoding.ASCII.GetBytes(GetSingleSlotKeys[0]));

        public List<string> GetSingleSlotKeys { get; }
        public List<string> GetCrossSlotKeys { get; }

        public abstract string[] GetSingleSlotRequest();
        public abstract string[] GetCrossSlotRequest();
        public abstract ArraySegment<string>[] SetupSingleSlotRequest();

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
                args[0] = Command;
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
                args[0] = Command;
                var count = 1;
                foreach (var arg in csr)
                    args[count++] = arg;
                return args;
            }
        }
    }

    public class DummyCommand : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => commandName;

        readonly string commandName;
        public DummyCommand(string commandName)
        {
            this.commandName = commandName;
        }

        public override string[] GetSingleSlotRequest() => throw new NotImplementedException();

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    #region BasicCommands
    internal class GET : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(GET);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0]];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class SET : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(SET);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], "value1"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class MGET : BaseCommand
    {
        public override bool IsArrayCommand => true;
        public override bool ArrayResponse => true;
        public override string Command => nameof(MGET);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], ssk[1], ssk[2]];
        }

        public override string[] GetCrossSlotRequest()
        {
            var csk = GetCrossSlotKeys;
            return [csk[0], csk[1], csk[2]];
        }

        public override ArraySegment<string>[] SetupSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            var setup = new ArraySegment<string>[] { new ArraySegment<string>(["MSET", ssk[1], "value1", ssk[2], "value", ssk[3], "value2"]) };
            return setup;
        }
    }

    internal class MSET : BaseCommand
    {
        public override bool IsArrayCommand => true;
        public override bool ArrayResponse => false;
        public override string Command => nameof(MSET);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], "value1", ssk[1], "value", ssk[2], "value2"];
        }

        public override string[] GetCrossSlotRequest()
        {
            var csk = GetCrossSlotKeys;
            return [csk[0], "value1", csk[1], "value", csk[2], "value2"];
        }

        public override ArraySegment<string>[] SetupSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            var setup = new ArraySegment<string>[] { new ArraySegment<string>(["MSET", ssk[1], "value1", ssk[2], "value", ssk[3], "value2"]) };
            return setup;
        }
    }

    internal class GETEX : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(GETEX);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0]];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class SETRANGE : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(SETRANGE);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], "0", "value1"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class GETRANGE : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(GETRANGE);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], "0", "-1"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class INCR : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(INCR);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0]];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class APPEND : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(APPEND);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], "value1"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class STRLEN : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(STRLEN);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0]];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class RENAME : BaseCommand
    {
        public override bool IsArrayCommand => true;
        public override bool ArrayResponse => false;
        public override bool RequiresExistingKey => true;
        public override string Command => nameof(RENAME);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], ssk[1]];
        }

        public override string[] GetCrossSlotRequest()
        {
            var csk = GetCrossSlotKeys;
            return [csk[0], csk[1]];
        }

        public override ArraySegment<string>[] SetupSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            var setup = new ArraySegment<string>[] { new(["MSET", ssk[1], "value1", ssk[2], "value", ssk[3], "value2"]) };
            return setup;
        }
    }

    internal class DEL : BaseCommand
    {
        public override bool IsArrayCommand => true;
        public override bool ArrayResponse => false;
        public override string Command => nameof(DEL);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], ssk[1], ssk[2]];
        }

        public override string[] GetCrossSlotRequest()
        {
            var csk = GetCrossSlotKeys;
            return [csk[0], csk[1], csk[2]];
        }

        public override ArraySegment<string>[] SetupSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            var setup = new ArraySegment<string>[] { new(["MSET", ssk[1], "value1", ssk[2], "value", ssk[3], "value2"]) };
            return setup;
        }
    }

    internal class GETDEL : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(GETDEL);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0]];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class EXISTS : BaseCommand
    {
        public override bool IsArrayCommand => true;
        public override bool ArrayResponse => false;
        public override string Command => nameof(EXISTS);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], ssk[1], ssk[2]];
        }

        public override string[] GetCrossSlotRequest()
        {
            var csk = GetCrossSlotKeys;
            return [csk[0], csk[1], csk[2]];
        }

        public override ArraySegment<string>[] SetupSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            var setup = new ArraySegment<string>[] { new(["MSET", ssk[1], "value1", ssk[2], "value", ssk[3], "value2"]) };
            return setup;
        }
    }

    internal class PERSIST : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(PERSIST);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0]];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class EXPIRE : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(EXPIRE);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], "10"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class TTL : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(TTL);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0]];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }
    #endregion

    #region BitmapCommands
    internal class GETBIT : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(GETBIT);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], "15"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class SETBIT : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(SETBIT);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], "15", "1"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class BITCOUNT : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(BITCOUNT);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], "15"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class BITPOS : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(BITPOS);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], "1"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class BITOP : BaseCommand
    {
        public override bool IsArrayCommand => true;
        public override bool ArrayResponse => false;
        public override string Command => nameof(BITOP);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return ["AND", ssk[0], ssk[1], ssk[2]];
        }

        public override string[] GetCrossSlotRequest()
        {
            var csk = GetCrossSlotKeys;
            return ["AND", csk[0], csk[1], csk[2]];
        }

        public override ArraySegment<string>[] SetupSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            var setup = new ArraySegment<string>[3];
            setup[0] = new ArraySegment<string>(["SET", ssk[1], "value1"]);
            setup[1] = new ArraySegment<string>(["SET", ssk[2], "value4"]);
            setup[2] = new ArraySegment<string>(["SET", ssk[3], "value7"]);
            return setup;
        }
    }

    internal class BITFIELD : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => true;
        public override string Command => nameof(BITFIELD);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], "INCRBY", "i5", "100", "1", "GET", "u4", "0"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class BITFIELD_RO : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => true;
        public override string Command => nameof(BITFIELD_RO);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], "GET", "u4", "0"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    #endregion

    #region HLLCommands

    internal class PFADD : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(PFADD);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], "a", "b", "c", "d"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class PFCOUNT : BaseCommand
    {
        public override bool IsArrayCommand => true;
        public override bool ArrayResponse => false;
        public override string Command => nameof(PFCOUNT);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], ssk[1], ssk[2]];
        }

        public override string[] GetCrossSlotRequest()
        {
            var csk = GetCrossSlotKeys;
            return [csk[0], csk[1], csk[2]];
        }

        public override ArraySegment<string>[] SetupSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            var setup = new ArraySegment<string>[3];
            setup[0] = new ArraySegment<string>(["PFADD", ssk[1], "value1", "value2", "value3"]);
            setup[1] = new ArraySegment<string>(["PFADD", ssk[2], "value4", "value5", "value6"]);
            setup[2] = new ArraySegment<string>(["PFADD", ssk[3], "value7", "value8", "value9"]);
            return setup;
        }
    }

    internal class PFMERGE : BaseCommand
    {
        public override bool IsArrayCommand => true;
        public override bool ArrayResponse => false;
        public override string Command => nameof(PFMERGE);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], ssk[1], ssk[2]];
        }

        public override string[] GetCrossSlotRequest()
        {
            var csk = GetCrossSlotKeys;
            return [csk[0], csk[1], csk[2]];
        }

        public override ArraySegment<string>[] SetupSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            var setup = new ArraySegment<string>[3];
            setup[0] = new ArraySegment<string>(["PFADD", ssk[1], "value1", "value2", "value3"]);
            setup[1] = new ArraySegment<string>(["PFADD", ssk[2], "value4", "value5", "value6"]);
            setup[2] = new ArraySegment<string>(["PFADD", ssk[3], "value7", "value8", "value9"]);
            return setup;
        }
    }

    #endregion

    #region SetCommands
    internal class SDIFFSTORE : BaseCommand
    {
        public override bool IsArrayCommand => true;
        public override bool ArrayResponse => false;
        public override string Command => nameof(SDIFFSTORE);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], ssk[1], ssk[2]];
        }

        public override string[] GetCrossSlotRequest()
        {
            var csk = GetCrossSlotKeys;
            return [csk[0], csk[1], csk[2]];
        }

        public override ArraySegment<string>[] SetupSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            var setup = new ArraySegment<string>[3];
            setup[0] = new ArraySegment<string>(["SADD", ssk[1], "a", "b", "c"]);
            setup[1] = new ArraySegment<string>(["SADD", ssk[2], "d", "e", "f"]);
            setup[2] = new ArraySegment<string>(["SADD", ssk[3], "g", "h", "i"]);
            return setup;
        }
    }

    internal class SDIFF : BaseCommand
    {
        public override bool IsArrayCommand => true;
        public override bool ArrayResponse => true;
        public override string Command => nameof(SDIFF);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], ssk[1], ssk[2]];
        }

        public override string[] GetCrossSlotRequest()
        {
            var csk = GetCrossSlotKeys;
            return [csk[0], csk[1], csk[2]];
        }

        public override ArraySegment<string>[] SetupSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            var setup = new ArraySegment<string>[3];
            setup[0] = new ArraySegment<string>(["SADD", ssk[1], "a", "b", "c"]);
            setup[1] = new ArraySegment<string>(["SADD", ssk[2], "d", "e", "f"]);
            setup[2] = new ArraySegment<string>(["SADD", ssk[3], "g", "h", "i"]);
            return setup;
        }
    }

    internal class SMOVE : BaseCommand
    {
        public override bool IsArrayCommand => true;
        public override bool ArrayResponse => false;
        public override string Command => nameof(SMOVE);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], ssk[1], "a"];
        }

        public override string[] GetCrossSlotRequest()
        {
            var csk = GetCrossSlotKeys;
            return [csk[0], csk[1], "a"];
        }

        public override ArraySegment<string>[] SetupSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            var setup = new ArraySegment<string>[3];
            setup[0] = new ArraySegment<string>(["SADD", ssk[1], "a", "b", "c"]);
            setup[1] = new ArraySegment<string>(["SADD", ssk[2], "d", "e", "f"]);
            setup[2] = new ArraySegment<string>(["SADD", ssk[3], "g", "h", "i"]);
            return setup;
        }
    }

    internal class SUNIONSTORE : BaseCommand
    {
        public override bool IsArrayCommand => true;
        public override bool ArrayResponse => false;
        public override string Command => nameof(SUNIONSTORE);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], ssk[1], ssk[2]];
        }

        public override string[] GetCrossSlotRequest()
        {
            var csk = GetCrossSlotKeys;
            return [csk[0], csk[1], csk[2]];
        }

        public override ArraySegment<string>[] SetupSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            var setup = new ArraySegment<string>[3];
            setup[0] = new ArraySegment<string>(["SADD", ssk[1], "a", "b", "c"]);
            setup[1] = new ArraySegment<string>(["SADD", ssk[2], "d", "e", "f"]);
            setup[2] = new ArraySegment<string>(["SADD", ssk[3], "g", "h", "i"]);
            return setup;
        }
    }

    internal class SUNION : BaseCommand
    {
        public override bool IsArrayCommand => true;
        public override bool ArrayResponse => true;
        public override string Command => nameof(SUNION);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], ssk[1], ssk[2]];
        }

        public override string[] GetCrossSlotRequest()
        {
            var csk = GetCrossSlotKeys;
            return [csk[0], csk[1], csk[2]];
        }

        public override ArraySegment<string>[] SetupSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            var setup = new ArraySegment<string>[3];
            setup[0] = new ArraySegment<string>(["SADD", ssk[1], "a", "b", "c"]);
            setup[1] = new ArraySegment<string>(["SADD", ssk[2], "d", "e", "f"]);
            setup[2] = new ArraySegment<string>(["SADD", ssk[3], "g", "h", "i"]);
            return setup;
        }
    }

    internal class SINTERSTORE : BaseCommand
    {
        public override bool IsArrayCommand => true;
        public override bool ArrayResponse => false;
        public override string Command => nameof(SINTERSTORE);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], ssk[1], ssk[2]];
        }

        public override string[] GetCrossSlotRequest()
        {
            var csk = GetCrossSlotKeys;
            return [csk[0], csk[1], csk[2]];
        }

        public override ArraySegment<string>[] SetupSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            var setup = new ArraySegment<string>[3];
            setup[0] = new ArraySegment<string>(["SADD", ssk[1], "a", "b", "c"]);
            setup[1] = new ArraySegment<string>(["SADD", ssk[2], "d", "e", "f"]);
            setup[2] = new ArraySegment<string>(["SADD", ssk[3], "g", "h", "i"]);
            return setup;
        }
    }

    internal class SINTER : BaseCommand
    {
        public override bool IsArrayCommand => true;
        public override bool ArrayResponse => true;
        public override string Command => nameof(SINTER);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], ssk[1], ssk[2]];
        }

        public override string[] GetCrossSlotRequest()
        {
            var csk = GetCrossSlotKeys;
            return [csk[0], csk[1], csk[2]];
        }

        public override ArraySegment<string>[] SetupSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            var setup = new ArraySegment<string>[3];
            setup[0] = new ArraySegment<string>(["SADD", ssk[1], "a", "b", "c"]);
            setup[1] = new ArraySegment<string>(["SADD", ssk[2], "d", "e", "f"]);
            setup[2] = new ArraySegment<string>(["SADD", ssk[3], "g", "h", "i"]);
            return setup;
        }
    }
    #endregion

    #region ListCommands
    internal class LMOVE : BaseCommand
    {
        public override bool IsArrayCommand => true;
        public override bool ArrayResponse => false;
        public override string Command => nameof(LMOVE);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], ssk[1], "LEFT", "RIGHT"];
        }

        public override string[] GetCrossSlotRequest()
        {
            var csk = GetCrossSlotKeys;
            return [csk[0], csk[1], "LEFT", "RIGHT"];
        }

        public override ArraySegment<string>[] SetupSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            var setup = new ArraySegment<string>[3];
            setup[0] = new ArraySegment<string>(["LPUSH", ssk[1], "a", "b", "c"]);
            setup[1] = new ArraySegment<string>(["LPUSH", ssk[2], "d", "e", "f"]);
            setup[2] = new ArraySegment<string>(["LPUSH", ssk[3], "g", "h", "i"]);
            return setup;
        }
    }
    #endregion
}