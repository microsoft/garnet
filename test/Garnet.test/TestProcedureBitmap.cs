// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    /// <summary>
    /// Test procedure for bitmap commands: SETBIT, GETBIT, BITCOUNT, BITOP, BITFIELD
    /// 
    /// Format: BITPROC keyname offset bitvalue keynameDestination bitmapB
    /// 
    /// Description: Exercise the SETBIT and GETBIT commands using garnet api
    /// </summary>

    sealed class TestProcedureBitmap : CustomTransactionProcedure
    {
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ArgSlice input)
        {
            int offset = 0;
            var bitmapA = GetNextArg(input, ref offset);
            GetNextArg(input, ref offset);
            GetNextArg(input, ref offset);
            var destinationKey = GetNextArg(input, ref offset);
            var bitmapB = GetNextArg(input, ref offset);

            if (bitmapA.Length == 0)
                return false;
            if (destinationKey.Length == 0)
                return false;
            if (bitmapB.Length == 0)
                return false;

            AddKey(bitmapA, LockType.Exclusive, false);
            AddKey(destinationKey, LockType.Exclusive, false);
            AddKey(bitmapB, LockType.Exclusive, false);

            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ArgSlice input, ref MemoryResult<byte> output)
        {
            int offset = 0;
            bool result = true;
            BitmapOperation[] bitwiseOps = new BitmapOperation[] { BitmapOperation.AND, BitmapOperation.OR, BitmapOperation.XOR };

            //get paramaters
            var bitmapA = GetNextArg(input, ref offset);
            var offsetArgument = GetNextArg(input, ref offset);
            var bitValueArgument = GetNextArg(input, ref offset);
            var destinationKeyBitOp = GetNextArg(input, ref offset);
            var bitmapB = GetNextArg(input, ref offset);

            //simple set and get for bitmaps
            api.StringSetBit(bitmapA, offsetArgument, bitValueArgument.Bytes[0] == '1', out _);
            api.StringGetBit(bitmapA, offsetArgument, out bool storedBitValue);
            if (storedBitValue != (bitValueArgument.Bytes[0] == '1'))
            {
                result = false;
                goto returnTo;
            }

            // bitcount command
            api.StringBitCount(bitmapA, 0, DateTime.Now.Day + 1, out long resultBitCount);
            if (resultBitCount != 1)
            {
                result = false;
                goto returnTo;
            }

            //bitop command
            var src = Int64.MaxValue;
            var data = BitConverter.GetBytes(src);
            api.SET(bitmapA, data);

            //Not operator
            api.StringBitOperation(BitmapOperation.NOT, destinationKeyBitOp, new ArgSlice[] { bitmapA }, out long size);
            if (size != 8)
            {
                result = false;
                goto returnTo;
            }
            api.GET(destinationKeyBitOp, out var valueData);
            var actualResultBitOp = BitConverter.ToInt64(valueData.Bytes, 0);

            long expectedResultBitOp = ~src;
            if (expectedResultBitOp != actualResultBitOp)
            {
                result = false;
                goto returnTo;
            }

            var srcB = Int64.MaxValue - 1234;
            data = BitConverter.GetBytes(srcB);
            api.SET(bitmapB, data);

            //apply operators
            for (int i = 0; i < bitwiseOps.Length; i++)
            {
                api.StringBitOperation(bitwiseOps[i], destinationKeyBitOp, new ArgSlice[] { bitmapA, bitmapB }, out size);
                if (size != 8)
                {
                    result = false;
                    goto returnTo;
                }
                api.GET(destinationKeyBitOp, out valueData);
                actualResultBitOp = BitConverter.ToInt64(valueData.Bytes, 0);
                switch (bitwiseOps[i])
                {
                    case BitmapOperation.AND:
                        expectedResultBitOp = src & srcB;
                        break;
                    case BitmapOperation.OR:
                        expectedResultBitOp = src | srcB;
                        break;
                    case BitmapOperation.XOR:
                        expectedResultBitOp = src ^ srcB;
                        break;
                }
                if (expectedResultBitOp != actualResultBitOp)
                {
                    result = false;
                    goto returnTo;
                }
            }

            //bitfield command
            data = new byte[1] { (byte)'P' };
            api.SET(bitmapA, data);
            var listCommands = new List<BitFieldCmdArgs>();

            var bitFieldArguments = new BitFieldCmdArgs((byte)RespCommand.GET, ((byte)BitFieldSign.UNSIGNED | 8), 0, 0, (byte)BitFieldOverflow.WRAP);
            listCommands.Add(bitFieldArguments);

            bitFieldArguments = new BitFieldCmdArgs((byte)RespCommand.INCRBY, ((byte)BitFieldSign.UNSIGNED | 4), 4, 1, (byte)BitFieldOverflow.WRAP);
            listCommands.Add(bitFieldArguments);

            api.StringBitField(bitmapA, listCommands, out var resultBitField);
            if (resultBitField.Count != 2)
            {
                result = false;
                goto returnTo;
            }
            else
            {
                if ((char)resultBitField[0] != 'P' || resultBitField[1] != 1)
                    result = false;
            }
        returnTo:
            WriteSimpleString(ref output, result ? "SUCCESS" : "ERROR");
        }
    }
}