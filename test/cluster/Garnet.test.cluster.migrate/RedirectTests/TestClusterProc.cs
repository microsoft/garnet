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
        /// <param name="procInput"></param>
        /// <returns></returns>
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput procInput)
        {
            var offset = 0;
            var getA = GetNextArg(ref procInput, ref offset);
            var getB = GetNextArg(ref procInput, ref offset);
            var getC = GetNextArg(ref procInput, ref offset);

            AddKey(getA, LockType.Shared, StoreType.Main);
            AddKey(getB, LockType.Shared, StoreType.Main);
            AddKey(getC, LockType.Shared, StoreType.Main);

            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            var offset = 0;
            var getA = GetNextArg(ref procInput, ref offset);
            var getB = GetNextArg(ref procInput, ref offset);
            var getC = GetNextArg(ref procInput, ref offset);

            var status = api.GET(getA, out PinnedSpanByte _);
            ClassicAssert.AreEqual(GarnetStatus.NOTFOUND, status);
            _ = api.GET(getB, out PinnedSpanByte _);
            ClassicAssert.AreEqual(GarnetStatus.NOTFOUND, status);
            _ = api.GET(getC, out PinnedSpanByte _);
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
        /// <returns></returns>
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput procInput)
        {
            var offset = 0;
            var getA = GetNextArg(ref procInput, ref offset);
            var setB = GetNextArg(ref procInput, ref offset);
            var setC = GetNextArg(ref procInput, ref offset);

            AddKey(getA, LockType.Shared, StoreType.Main);
            AddKey(setB, LockType.Exclusive, StoreType.Main);
            AddKey(setC, LockType.Exclusive, StoreType.Main);

            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            var offset = 0;
            var getA = GetNextArg(ref procInput, ref offset);
            var setB = GetNextArg(ref procInput, ref offset);
            var setC = GetNextArg(ref procInput, ref offset);

            _ = api.GET(getA, out PinnedSpanByte _);
            var status = api.SET(setB, setB);
            ClassicAssert.AreEqual(GarnetStatus.OK, status);
            status = api.SET(setC, setC);
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