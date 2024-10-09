// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Garnet.server;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Garnet.test.cluster
{
    sealed class TestClusterReadOnlyCustomTxn : CustomTransactionProcedure
    {
        /// <summary>
        /// Parameters including command
        /// </summary>
        public const int Arity = 4;

        /// <summary>
        ///  CLUSTERGETPROC key1 key2 key3
        /// </summary>
        /// <typeparam name="TGarnetReadApi"></typeparam>
        /// <param name="api"></param>
        /// <param name="input"></param>
        /// <returns></returns>
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ArgSlice input)
        {
            var offset = 0;
            var getA = GetNextArg(input, ref offset);
            var getB = GetNextArg(input, ref offset);
            var getC = GetNextArg(input, ref offset);

            AddKey(getA, LockType.Shared, isObject: false);
            AddKey(getB, LockType.Shared, isObject: false);
            AddKey(getC, LockType.Shared, isObject: false);

            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ArgSlice input, ref MemoryResult<byte> output)
        {
            var offset = 0;
            var getA = GetNextArg(input, ref offset);
            var getB = GetNextArg(input, ref offset);
            var getC = GetNextArg(input, ref offset);

            var status = api.GET(getA, out _);
            ClassicAssert.AreEqual(GarnetStatus.NOTFOUND, status);
            _ = api.GET(getB, out _);
            ClassicAssert.AreEqual(GarnetStatus.NOTFOUND, status);
            _ = api.GET(getC, out _);
            ClassicAssert.AreEqual(GarnetStatus.NOTFOUND, status);
            WriteSimpleString(ref output, "SUCCESS");
        }
    }

    internal class CLUSTERGETPROC : BaseCommand
    {
        public override bool IsArrayCommand => true;
        public override bool ArrayResponse => false;
        public override string Command => nameof(CLUSTERGETPROC);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            // Order matters here for migrated keys
            // key1 will result in OK slot verification
            // key0 will result in ASK
            // Response will be TRYAGAIN
            return [ssk[1], ssk[0], ssk[2]];
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

    sealed class TestClusterReadWriteCustomTxn : CustomTransactionProcedure
    {
        /// <summary>
        /// Parameters including command
        /// </summary>
        public const int Arity = 4;

        /// <summary>
        ///  CLUSTERSETPROC key1 key2 key3
        /// </summary>
        /// <typeparam name="TGarnetReadApi"></typeparam>
        /// <param name="api"></param>
        /// <param name="input"></param>
        /// <returns></returns>
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ArgSlice input)
        {
            var offset = 0;
            var getA = GetNextArg(input, ref offset);
            var setB = GetNextArg(input, ref offset);
            var setC = GetNextArg(input, ref offset);

            AddKey(getA, LockType.Shared, isObject: false);
            AddKey(setB, LockType.Exclusive, isObject: false);
            AddKey(setC, LockType.Exclusive, isObject: false);

            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ArgSlice input, ref MemoryResult<byte> output)
        {
            var offset = 0;
            var getA = GetNextArg(input, ref offset);
            var setB = GetNextArg(input, ref offset).SpanByte;
            var setC = GetNextArg(input, ref offset).SpanByte;

            _ = api.GET(getA, out _);
            var status = api.SET(ref setB, ref setB);
            ClassicAssert.AreEqual(GarnetStatus.OK, status);
            status = api.SET(ref setC, ref setC);
            ClassicAssert.AreEqual(GarnetStatus.OK, status);
            WriteSimpleString(ref output, "SUCCESS");
        }
    }

    internal class CLUSTERSETPROC : BaseCommand
    {
        public override bool IsArrayCommand => true;
        public override bool ArrayResponse => false;
        public override string Command => nameof(CLUSTERSETPROC);

        public override string[] GetSingleSlotRequest()
        {
            var ssk = GetSingleSlotKeys;
            return [ssk[1], ssk[0], ssk[2]];
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
}