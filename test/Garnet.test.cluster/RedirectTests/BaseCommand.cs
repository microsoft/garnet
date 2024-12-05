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

        /// <summary>
        /// Indicates if command is a multi-key operation
        /// </summary>
        public abstract bool IsArrayCommand { get; }

        /// <summary>
        /// Indicates if command response is a single value or an array of values.
        /// NOTE: used only for GarnetClientSession that requires to differentiate between string and string[]
        /// </summary>
        public abstract bool ArrayResponse { get; }

        /// <summary>
        /// Command requires an existing key to be inserted before the command executes
        /// Example: RENAME,LSET
        /// NOTE: The example command throw an error if key is not set which is not relevant when testing OK operation.
        /// </summary>
        public virtual bool RequiresExistingKey => false;

        /// <summary>
        /// Command name
        /// </summary>
        public abstract string Command { get; }

        public BaseCommand()
        {
            GetSingleSlotKeys = singleSlotKeys();
            GetCrossSlotKeys = crossSlotKeys();
        }

        /// <summary>
        /// Get slot value for keys from <see cref="GetSingleSlotKeys"/>
        /// </summary>
        public int GetSlot => HashSlotUtils.HashSlot(Encoding.ASCII.GetBytes(GetSingleSlotKeys[0]));

        /// <summary>
        /// Get a list of keys that are guaranteed to hash to same slot
        /// </summary>
        public List<string> GetSingleSlotKeys { get; }

        /// <summary>
        /// Get a list of keys where at least one hashes to the same slot
        /// </summary>
        public List<string> GetCrossSlotKeys { get; }

        /// <summary>
        /// Generate a request for this command that references a single slot.
        /// NOTE: available for both single and multi-key operations
        /// </summary>
        /// <returns></returns>
        public abstract string[] GetSingleSlotRequest();

        /// <summary>
        /// Generate a request for this command that references at least two slots
        /// NOTE: available only for multi-key operations
        /// </summary>
        /// <returns></returns>
        public abstract string[] GetCrossSlotRequest();

        /// <summary>
        /// Setup for a given command that references a single slot
        /// NOTE: Used for TRYAGAIN test to simulate a key MOVED to another node
        /// </summary>
        /// <returns></returns>
        public abstract ArraySegment<string>[] SetupSingleSlotRequest();

        /// <summary>
        /// Generate a list of keys that hash to a single slot
        /// </summary>
        /// <param name="klen"></param>
        /// <param name="kcount"></param>
        /// <param name="kEndTag"></param>
        /// <returns></returns>
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

        /// <summary>
        /// Generate a list of keys that hash to multi slots
        /// </summary>
        /// <param name="klen"></param>
        /// <param name="kcount"></param>
        /// <returns></returns>
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

        /// <summary>
        /// Get command with parameters containing keys that hash to a single slot
        /// </summary>
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

        /// <summary>
        /// Get command with parameters containing keys that hash to at least two slots
        /// </summary>
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
        /// <inheritdoc />
        public override bool IsArrayCommand => false;
        /// <inheritdoc />
        public override bool ArrayResponse => false;
        /// <inheritdoc />
        public override string Command => commandName;

        readonly string commandName;
        public DummyCommand(string commandName)
        {
            this.commandName = commandName;
        }

        /// <inheritdoc />
        public override string[] GetSingleSlotRequest() => throw new NotImplementedException();

        /// <inheritdoc />
        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        /// <inheritdoc />
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

    internal class GETSET : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(GETSET);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], "value1"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class SETNX : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(SETNX);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], "value1"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class SUBSTR : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(SUBSTR);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], "0", "-1"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
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

    internal class GEOSEARCHSTORE : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(GEOSEARCHSTORE);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], ssk[1], "FROMMEMBER", "bar", "BYBOX", "800", "800", "km", "STOREDIST"];
        }

        public override string[] GetCrossSlotRequest()
        {
            var csk = GetCrossSlotKeys;
            return [csk[0], csk[1], "FROMMEMBER", "bar", "BYBOX", "800", "800", "km", "STOREDIST"];
        }

        public override ArraySegment<string>[] SetupSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            var setup = new ArraySegment<string>[] { new ArraySegment<string>([ssk[0], ssk[1], "FROMMEMBER", "bar", "BYBOX", "800", "800", "km", "STOREDIST"]) };
            return setup;
        }
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

    internal class INCRBYFLOAT : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(INCRBYFLOAT);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], "1.5"];
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

    internal class WATCH : BaseCommand
    {
        public override bool IsArrayCommand => true;
        public override bool ArrayResponse => false;
        public override string Command => nameof(WATCH);

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

    internal class WATCHMS : BaseCommand
    {
        public override bool IsArrayCommand => true;
        public override bool ArrayResponse => false;
        public override string Command => nameof(WATCHMS);

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

    internal class WATCHOS : BaseCommand
    {
        public override bool IsArrayCommand => true;
        public override bool ArrayResponse => false;
        public override string Command => nameof(WATCHOS);

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

    internal class SADD : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(SADD);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], "a", "b", "c"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class SREM : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(SREM);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], "a", "b", "c"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class SCARD : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(SCARD);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0]];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class SMEMBERS : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => true;
        public override string Command => nameof(SMEMBERS);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0]];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class SISMEMBER : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(SISMEMBER);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], "0"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class SMISMEMBER : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => true;
        public override string Command => nameof(SMISMEMBER);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], "0", "1"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class SPOP : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(SPOP);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0]];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class SRANDMEMBER : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(SRANDMEMBER);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0]];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
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

    internal class LPUSH : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(LPUSH);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], "a", "b", "c"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class LPOP : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(LPOP);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0]];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class LPOS : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(LPOS);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], "0"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class LMPOP : BaseCommand
    {
        public override bool IsArrayCommand => true;
        public override bool ArrayResponse => true;
        public override string Command => nameof(LMPOP);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return ["3", ssk[0], ssk[1], ssk[2], "LEFT"];
        }

        public override string[] GetCrossSlotRequest()
        {
            var csk = GetCrossSlotKeys;
            return ["3", csk[0], csk[1], csk[2], "LEFT"];
        }

        public override ArraySegment<string>[] SetupSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            var setup = new ArraySegment<string>[3];
            setup[0] = new ArraySegment<string>(["LPUSH", ssk[1], "value1", "value2", "value3"]);
            setup[1] = new ArraySegment<string>(["LPUSH", ssk[2], "value4", "value5", "value6"]);
            setup[2] = new ArraySegment<string>(["LPUSH", ssk[3], "value7", "value8", "value9"]);
            return setup;
        }
    }

    internal class BLPOP : BaseCommand
    {
        public override bool IsArrayCommand => true;
        public override bool ArrayResponse => true;
        public override string Command => nameof(BLPOP);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], ssk[1], ssk[2], "1"];
        }

        public override string[] GetCrossSlotRequest()
        {
            var csk = GetCrossSlotKeys;
            return [csk[0], csk[1], csk[2], "1"];
        }

        public override ArraySegment<string>[] SetupSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            var setup = new ArraySegment<string>[3];
            setup[0] = new ArraySegment<string>(["LPUSH", ssk[1], "value1", "value2", "value3"]);
            setup[1] = new ArraySegment<string>(["LPUSH", ssk[2], "value4", "value5", "value6"]);
            setup[2] = new ArraySegment<string>(["LPUSH", ssk[3], "value7", "value8", "value9"]);
            return setup;
        }
    }

    internal class BLMOVE : BaseCommand
    {
        public override bool IsArrayCommand => true;
        public override bool ArrayResponse => false;
        public override string Command => nameof(BLMOVE);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], ssk[1], "LEFT", "LEFT", "1"];
        }

        public override string[] GetCrossSlotRequest()
        {
            var csk = GetCrossSlotKeys;
            return [csk[0], csk[1], "LEFT", "LEFT", "1"];
        }

        public override ArraySegment<string>[] SetupSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            var setup = new ArraySegment<string>[3];
            setup[0] = new ArraySegment<string>(["LPUSH", ssk[1], "value1", "value2", "value3"]);
            setup[1] = new ArraySegment<string>(["LPUSH", ssk[2], "value4", "value5", "value6"]);
            setup[2] = new ArraySegment<string>(["LPUSH", ssk[3], "value7", "value8", "value9"]);
            return setup;
        }
    }

    internal class BRPOPLPUSH : BaseCommand
    {
        public override bool IsArrayCommand => true;
        public override bool ArrayResponse => false;
        public override string Command => nameof(BRPOPLPUSH);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], ssk[1], "1"];
        }

        public override string[] GetCrossSlotRequest()
        {
            var csk = GetCrossSlotKeys;
            return [csk[0], csk[1], "1"];
        }

        public override ArraySegment<string>[] SetupSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            var setup = new ArraySegment<string>[3];
            setup[0] = new ArraySegment<string>(["LPUSH", ssk[1], "value1", "value2", "value3"]);
            setup[1] = new ArraySegment<string>(["LPUSH", ssk[2], "value4", "value5", "value6"]);
            setup[2] = new ArraySegment<string>(["LPUSH", ssk[3], "value7", "value8", "value9"]);
            return setup;
        }
    }

    internal class LLEN : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(LLEN);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0]];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class LTRIM : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(LTRIM);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], "0", "100"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class LRANGE : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => true;
        public override string Command => nameof(LRANGE);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], "0", "100"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class LINDEX : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(LINDEX);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], "0"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class LINSERT : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(LINSERT);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], "BEFORE", "aaa", "bbb"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class LREM : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(LREM);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], "0", "10"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class RPOPLPUSH : BaseCommand
    {
        public override bool IsArrayCommand => true;
        public override bool ArrayResponse => false;
        public override string Command => nameof(RPOPLPUSH);

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
            var setup = new ArraySegment<string>[3];
            setup[0] = new ArraySegment<string>(["LPUSH", ssk[1], "a", "b", "c"]);
            setup[1] = new ArraySegment<string>(["LPUSH", ssk[2], "d", "e", "f"]);
            setup[2] = new ArraySegment<string>(["LPUSH", ssk[3], "g", "h", "i"]);
            return setup;
        }
    }

    internal class LSET : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(LSET);

        public override bool RequiresExistingKey => true;

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], "0", "d"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    #endregion

    #region LuaCommands
    internal class EVAL : BaseCommand
    {
        public override bool IsArrayCommand => true;
        public override bool ArrayResponse => false;
        public override string Command => nameof(EVAL);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return ["return 'OK'", "3", ssk[0], ssk[1], ssk[2]];
        }

        public override string[] GetCrossSlotRequest()
        {
            var csk = GetCrossSlotKeys;
            return ["return 'OK'", "3", csk[0], csk[1], csk[2]];
        }

        public override ArraySegment<string>[] SetupSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            var setup = new ArraySegment<string>[] { new ArraySegment<string>(["EVAL", "return 'OK'", "3", ssk[1], ssk[2], ssk[3]]) };
            return setup;
        }
    }
    #endregion

    #region GeoCommands
    internal class GEOADD : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(GEOADD);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0], "13.361389", "38.115556", "city"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class GEOHASH : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => true;
        public override string Command => nameof(GEOHASH);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[0]];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }
    #endregion

    #region SortedSetCommands
    internal class ZADD : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(ZADD);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            // ZADD x 1 a
            return [ssk[0], "1", "a"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class ZREM : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(ZREM);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            // ZREM x a b c
            return [ssk[0], "a", "b", "c"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class ZCARD : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(ZCARD);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            // ZCARD x
            return [ssk[0]];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class ZRANGE : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => true;
        public override string Command => nameof(ZRANGE);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            // ZRANGE x 0 -1
            return [ssk[0], "0", "-1"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class ZREVRANGEBYLEX : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => true;
        public override string Command => nameof(ZREVRANGEBYLEX);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            // ZREVRANGEBYLEX x 0 -1
            return [ssk[0], "0", "-1"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class ZSCORE : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(ZSCORE);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            // ZSCORE x a
            return [ssk[0], "a"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class ZMSCORE : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => true;
        public override string Command => nameof(ZMSCORE);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            // ZMSCORE x a
            return [ssk[0], "a"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class ZPOPMAX : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => true;
        public override string Command => nameof(ZPOPMAX);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            // ZPOPMAX a
            return [ssk[0]];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class ZCOUNT : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(ZCOUNT);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            // ZCOUNT x 0 100
            return [ssk[0], "0", "100"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class ZLEXCOUNT : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(ZLEXCOUNT);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            // ZLEXCOUNT x 0 100
            return [ssk[0], "0", "100"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class ZINCRBY : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(ZINCRBY);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            // ZINCRBY x 20 a
            return [ssk[0], "20", "a"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class ZRANK : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(ZRANK);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            // ZRANK x a
            return [ssk[0], "20"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class ZREMRANGEBYRANK : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(ZREMRANGEBYRANK);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            // ZREMRANGEBYRANK x 0 -1
            return [ssk[0], "0", "-1"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class ZRANDMEMBER : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(ZRANDMEMBER);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            // ZRANDMEMBER x
            return [ssk[0]];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class ZDIFF : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => true;
        public override string Command => nameof(ZDIFF);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            // ZDIFF 2 a b
            return ["2", ssk[0], ssk[1]];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class ZDIFFSTORE : BaseCommand
    {
        public override bool IsArrayCommand => true;
        public override bool ArrayResponse => false;
        public override string Command => nameof(ZDIFFSTORE);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            // ZDIFFSTORE c 2 a b
            return [ssk[0], "2", ssk[1], ssk[2]];
        }

        public override string[] GetCrossSlotRequest()
        {
            var csk = GetCrossSlotKeys;
            return [csk[0], "2", csk[1], csk[2]];
        }

        public override ArraySegment<string>[] SetupSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            var setup = new ArraySegment<string>[3];
            setup[0] = new ArraySegment<string>(["ZADD", ssk[1], "1", "a"]);
            setup[1] = new ArraySegment<string>(["ZADD", ssk[2], "2", "b"]);
            setup[2] = new ArraySegment<string>(["ZADD", ssk[3], "3", "c"]);
            return setup;
        }
    }

    internal class ZMPOP : BaseCommand
    {
        public override bool IsArrayCommand => true;
        public override bool ArrayResponse => false;
        public override string Command => nameof(ZMPOP);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return ["3", ssk[0], ssk[1], ssk[2], "MIN", "COUNT", "1"];
        }

        public override string[] GetCrossSlotRequest()
        {
            var csk = GetCrossSlotKeys;
            return ["3", csk[0], csk[1], csk[2], "MIN", "COUNT", "1"];
        }

        public override ArraySegment<string>[] SetupSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            var setup = new ArraySegment<string>[3];
            setup[0] = new ArraySegment<string>(["ZADD", ssk[1], "1", "a"]);
            setup[1] = new ArraySegment<string>(["ZADD", ssk[2], "2", "b"]);
            setup[2] = new ArraySegment<string>(["ZADD", ssk[3], "3", "c"]);
            return setup;
        }
    }

    #endregion

    #region HashCommands
    internal class HSET : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(HSET);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            //HSET x a 1 b 2
            return [ssk[0], "a", "1", "b", "2"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class HGET : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(HGET);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            // HGET x a
            return [ssk[0], "a"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class HGETALL : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => true;
        public override string Command => nameof(HGETALL);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            // HGETALL x
            return [ssk[0]];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class HMGET : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => true;
        public override string Command => nameof(HMGET);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            // HMGET x a
            return [ssk[0], "a"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class HRANDFIELD : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(HRANDFIELD);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            // HRANDFIELD x
            return [ssk[0]];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class HLEN : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(HLEN);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            // HLEN x
            return [ssk[0]];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class HSTRLEN : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(HSTRLEN);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            // HSTRLEN x a
            return [ssk[0], "a"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class HDEL : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(HDEL);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            // HDEL x a
            return [ssk[0], "a"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class HEXISTS : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(HEXISTS);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            // HEXISTS x a
            return [ssk[0], "a"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class HKEYS : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => true;
        public override string Command => nameof(HKEYS);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            // HKEYS x
            return [ssk[0]];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }

    internal class HINCRBY : BaseCommand
    {
        public override bool IsArrayCommand => false;
        public override bool ArrayResponse => false;
        public override string Command => nameof(HINCRBY);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            // HINCRBY x a 10
            return [ssk[0], "a", "10"];
        }

        public override string[] GetCrossSlotRequest() => throw new NotImplementedException();

        public override ArraySegment<string>[] SetupSingleSlotRequest() => throw new NotImplementedException();
    }
    #endregion
}