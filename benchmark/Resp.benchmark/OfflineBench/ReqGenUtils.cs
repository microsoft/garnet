// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text;

namespace Resp.benchmark
{
    public unsafe partial class ReqGen
    {
        static readonly bool invalidateHLL = false;

        private bool WriteHeader(byte[] headerData, ref byte* curr, byte* vend)
        {
            byte* save = curr;
            if (curr + headerData.Length >= vend)
                return false;
            for (int i = 0; i < headerData.Length; i++)
                *curr++ = headerData[i];
            return true;
        }

        private bool WriteOp(int bufferOffset, ref byte* curr, byte* vend, OpType opType)
        {
            int n;

            var bitopType = opType switch
            {
                OpType.BITOP_AND => Encoding.ASCII.GetBytes("AND"),
                OpType.BITOP_OR => Encoding.ASCII.GetBytes("OR"),
                OpType.BITOP_XOR => Encoding.ASCII.GetBytes("XOR"),
                OpType.BITOP_NOT => Encoding.ASCII.GetBytes("NOT"),
                _ => null
            };

            byte[] keyData = null;

            //key
            switch (opType)
            {
                case OpType.ZADD:
                case OpType.ZREM:
                case OpType.ZCARD:
                case OpType.GEOADD:
                case OpType.PFADD:
                    if (!WriteKey(ref curr, vend))
                        return false;
                    break;
                case OpType.PFCOUNT:
                    if (!WriteKey(bufferOffset, ref curr, vend, out keyData))
                        return false;

                    if (invalidateHLL)
                    {
                        //Try to invalidate PFCOUNT
                        if (!WriteHeader(Encoding.ASCII.GetBytes($"*3\r\n$5\r\nPFADD\r\n"), ref curr, vend))
                            return false;

                        if (!WriteStringBytes(ref curr, vend, keyData))
                            return false;

                        RandomString();
                        if (!WriteStringBytes(ref curr, vend, valueBuffer))
                            return false;
                    }

                    break;
                case OpType.PFMERGE:
                    int key = keyRandomGen.Next(0, hllDstMergeKeyCount);
                    if (!WriteKey(ref curr, vend, key))
                        return false;
                    if (invalidateHLL)
                    {
                        //Try to delete merge HLL
                        if (!WriteHeader(Encoding.ASCII.GetBytes($"*2\r\n$3\r\nDEL\r\n"), ref curr, vend))
                            return false;

                        if (!WriteKey(ref curr, vend, key))
                            return false;
                    }
                    break;
                case OpType.MSET:
                case OpType.INCR:
                case OpType.GET:
                case OpType.SET:
                case OpType.SETEX:
                case OpType.MGET:
                case OpType.SETBIT:
                case OpType.GETBIT:
                case OpType.BITCOUNT:
                case OpType.BITPOS:
                case OpType.SETIFPM:
                case OpType.MYDICTSET:
                case OpType.MYDICTGET:
                case OpType.SCRIPTSET:
                case OpType.SCRIPTGET:
                case OpType.SCRIPTRETKEY:
                case OpType.PUBLISH:
                case OpType.SPUBLISH:
                    if (!WriteKey(bufferOffset, ref curr, vend, out keyData))
                        return false;
                    break;
                case OpType.BITOP_AND:
                case OpType.BITOP_OR:
                case OpType.BITOP_XOR:
                case OpType.BITOP_NOT:
                    if (!WriteStringBytes(ref curr, vend, bitopType))
                        return false;
                    break;
                case OpType.BITFIELD:
                case OpType.BITFIELD_GET:
                case OpType.BITFIELD_SET:
                case OpType.BITFIELD_INCR:
                    if (!WriteKey(ref curr, vend))
                        return false;
                    break;
                case OpType.PING:
                    return true;
                default:
                    break;
            }

            //arg1
            switch (opType)
            {
                case OpType.ZADD:
                case OpType.ZREM:
                    n = Start + r.Next(DbSize);
                    if (!WriteInteger(n, ref curr, vend))
                        return false;
                    break;
                case OpType.PFADD:
                case OpType.MSET:
                    if (valueLen == 0)
                    {
                        if (!WriteStringBytes(ref curr, vend, keyData))
                            return false;
                    }
                    else
                    {
                        RandomString();
                        if (!WriteStringBytes(ref curr, vend, valueBuffer))
                            return false;
                    }
                    break;

                case OpType.MYDICTSET:
                    if (!WriteStringBytes(ref curr, vend, keyData))
                        return false;
                    if (valueLen == 0)
                    {
                        if (!WriteStringBytes(ref curr, vend, keyData))
                            return false;
                    }
                    else
                    {
                        RandomString();
                        if (!WriteStringBytes(ref curr, vend, valueBuffer))
                            return false;
                    }
                    break;

                case OpType.MYDICTGET:
                    if (!WriteStringBytes(ref curr, vend, keyData))
                        return false;
                    break;

                case OpType.SETIFPM:
                    if (valueLen == 0)
                    {
                        if (!WriteStringBytes(ref curr, vend, keyData))
                            return false;
                    }
                    else
                    {
                        RandomString();
                        if (!WriteStringBytes(ref curr, vend, valueBuffer))
                            return false;
                    }
                    if (valueLen == 0)
                    {
                        if (!WriteStringBytes(ref curr, vend, keyData))
                            return false;
                    }
                    else
                    {
                        RandomString();
                        if (!WriteStringBytes(ref curr, vend, valueBuffer))
                            return false;
                    }
                    break;
                case OpType.MPFADD:
                case OpType.SET:
                case OpType.SCRIPTSET:
                case OpType.PUBLISH:
                case OpType.SPUBLISH:
                    RandomString();
                    if (!WriteStringBytes(ref curr, vend, valueBuffer))
                        return false;
                    break;
                case OpType.SETEX:
                    if (!WriteInteger(ttl, ref curr, vend))
                        return false;
                    break;
                case OpType.PFMERGE:
                    if (!WriteKey(ref curr, vend))
                        return false;
                    break;
                case OpType.INCR:
                case OpType.GET:
                case OpType.MGET:
                case OpType.SCRIPTGET:
                case OpType.SCRIPTRETKEY:
                    break;
                case OpType.SETBIT:
                case OpType.GETBIT:
                    n = valueRandomGen.Next(0, (valueLen << 3) - 1);
                    if (!WriteInteger(n, ref curr, vend))
                        return false;
                    break;
                case OpType.BITCOUNT:
                    break;
                case OpType.BITPOS:
                    if (!WriteInteger(valueRandomGen.Next(0, 1), ref curr, vend))
                        return false;
                    break;
                case OpType.BITOP_AND:
                case OpType.BITOP_OR:
                case OpType.BITOP_XOR:
                case OpType.BITOP_NOT:
                    if (!WriteKey(ref curr, vend))
                        return false;
                    break;
                case OpType.BITFIELD:
                    bitfieldOpCount = 3;
                    if (!WriteBitfieldArgs(ref curr, vend, Encoding.ASCII.GetBytes("SET")))
                        return false;
                    if (!WriteBitfieldArgs(ref curr, vend, Encoding.ASCII.GetBytes("INCRBY")))
                        return false;
                    if (!WriteBitfieldArgs(ref curr, vend, Encoding.ASCII.GetBytes("GET")))
                        return false;
                    break;
                case OpType.BITFIELD_GET:
                    if (!WriteBitfieldArgs(ref curr, vend, Encoding.ASCII.GetBytes("GET")))
                        return false;
                    break;
                case OpType.BITFIELD_SET:
                    if (!WriteBitfieldArgs(ref curr, vend, Encoding.ASCII.GetBytes("SET")))
                        return false;
                    break;
                case OpType.BITFIELD_INCR:
                    if (!WriteBitfieldArgs(ref curr, vend, Encoding.ASCII.GetBytes("INCRBY")))
                        return false;
                    break;
                case OpType.GEOADD:
                    if (!WriteStringBytes(ref curr, vend, Encoding.ASCII.GetBytes(GeoUtils.GetValidGeo().lng)))
                        return false;
                    break;
                default:
                    break;
            }

            //arg2
            switch (opType)
            {
                case OpType.ZADD:
                    n = Start + r.Next(DbSize);
                    if (!WriteInteger(n, ref curr, vend))
                        return false;
                    break;
                case OpType.ZREM:
                case OpType.PFADD:
                case OpType.MSET:
                case OpType.INCR:
                case OpType.GET:
                case OpType.MGET:
                    break;
                case OpType.SETBIT:
                    n = valueRandomGen.Next(0, 1);
                    if (!WriteInteger(n, ref curr, vend))
                        return false;
                    break;
                case OpType.GETBIT:
                case OpType.BITCOUNT:
                case OpType.BITPOS:
                    break;
                case OpType.BITOP_AND:
                case OpType.BITOP_OR:
                case OpType.BITOP_XOR:
                    for (int i = 0; i < bitOpSrckeyCount; i++)
                        if (!WriteKey(ref curr, vend))
                            return false;
                    break;
                case OpType.BITOP_NOT:
                    if (!WriteKey(ref curr, vend))
                        return false;
                    break;
                case OpType.BITFIELD:
                case OpType.BITFIELD_GET:
                case OpType.BITFIELD_SET:
                case OpType.BITFIELD_INCR:
                case OpType.GEOADD:
                    if (!WriteStringBytes(ref curr, vend, Encoding.ASCII.GetBytes(GeoUtils.GetValidGeo().lat)))
                        return false;
                    break;
                case OpType.SETEX:
                    RandomString();
                    if (!WriteStringBytes(ref curr, vend, valueBuffer))
                        return false;
                    break;
                default:
                    break;
            }

            // arg3
            switch (opType)
            {
                case OpType.GEOADD:
                    n = Start + r.Next(DbSize);
                    if (!WriteInteger(n, ref curr, vend))
                        return false;
                    break;
                default:
                    break;
            }

            return true;
        }

        private bool WriteBitfieldArgs(ref byte* curr, byte* vend, byte[] bitfieldOpType)
        {
            int offset = valueRandomGen.Next(0, (valueBuffer.Length << 3) - 64);
            int bitCount = valueRandomGen.Next(1, 64);
            long vset = RandomIntBitRange(bitCount, true);
            byte[] typeData = Encoding.ASCII.GetBytes("i" + bitCount.ToString());

            WriteStringBytes(ref curr, vend, bitfieldOpType);
            WriteStringBytes(ref curr, vend, typeData);
            WriteInteger(offset, ref curr, vend);

            if (bitfieldOpType[0] == 'G') return true;

            WriteInteger(vset, ref curr, vend);

            return true;
        }

        private bool WriteKey(ref byte* curr, byte* vend)
        {
            int key = randomGen ? keyRandomGen.Next(Start, DbSize) : (Start + keyIndex++);
            byte[] keyData = Encoding.ASCII.GetBytes(key.ToString().PadLeft(keyLen, 'X'));
            return WriteStringBytes(ref curr, vend, keyData);
        }

        private bool WriteKey(int bufferOffset, ref byte* curr, byte* vend, out byte[] keyData)
        {
            if (shardedKeys > 0)
            {
                keyData = slotKeys[bufferOffset % slotKeys.Count];
                return WriteStringBytes(ref curr, vend, keyData);
            }
            else
            {
                int key;
                if (randomGen)
                {
                    if (zipf)
                        key = Start + zipfg.Next();
                    else
                        key = Start + keyRandomGen.Next(DbSize);
                }
                else
                    key = Start + keyIndex++;
                keyData = Encoding.ASCII.GetBytes(key.ToString().PadLeft(keyLen, numericValue ? '1' : 'X'));
                return WriteStringBytes(ref curr, vend, keyData);
            }
        }

        private bool WriteKey(ref byte* curr, byte* vend, int key)
        {
            byte[] keyData = Encoding.ASCII.GetBytes(key.ToString().PadLeft(keyLen, 'X'));
            return WriteStringBytes(ref curr, vend, keyData);
        }

        private bool WriteInteger(int n, ref byte* curr, byte* vend)
        {
            int nd = NumUtils.NumDigits(n);
            int sign = ((n < 0) ? 1 : 0);

            int ndSize = NumUtils.NumDigits(nd + sign);
            int totalLen = 1 + ndSize + 2 + (nd + sign) + 2;
            if (curr + totalLen >= vend)
                return false;

            *curr++ = (byte)'$';
            NumUtils.IntToBytes(nd + sign, ref curr);
            *curr++ = (byte)'\r';
            *curr++ = (byte)'\n';
            NumUtils.IntToBytes(n, nd, ref curr);
            *curr++ = (byte)'\r';
            *curr++ = (byte)'\n';
            return true;
        }

        private bool WriteInteger(long n, ref byte* curr, byte* vend)
        {
            int nd = NumUtils.NumDigitsInLong(n);
            int sign = ((n < 0) ? 1 : 0);

            int ndSize = NumUtils.NumDigits(nd + sign);
            int totalLen = 1 + ndSize + 2 + (nd + sign) + 2;
            if (curr + totalLen >= vend)
                return false;

            *curr++ = (byte)'$';
            NumUtils.IntToBytes(nd + sign, ref curr);
            *curr++ = (byte)'\r';
            *curr++ = (byte)'\n';
            NumUtils.LongToBytes(n, nd, ref curr);
            *curr++ = (byte)'\r';
            *curr++ = (byte)'\n';

            return true;
        }

        private bool WriteStringBytes(ref byte* curr, byte* vend, byte[] data)
        {
            int digits = NumUtils.NumDigits(data.Length);
            int totalLen = 1 + digits + 2 + data.Length + 2;
            if (curr + totalLen >= vend)
                return false;

            *curr++ = (byte)'$';
            NumUtils.IntToBytes(data.Length, ref curr);
            *curr++ = (byte)'\r';
            *curr++ = (byte)'\n';

            for (int i = 0; i < data.Length; i++) *curr++ = (byte)data[i];
            *curr++ = (byte)'\r';
            *curr++ = (byte)'\n';
            return true;
        }
    }
}