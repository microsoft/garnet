// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using Tsavorite.core;

namespace Tsavorite.test.recovery.objects
{
    public class AdIdObj
    {
        public long adId;

        public partial struct Comparer : IKeyComparer<AdIdObj>
        {
            public readonly long GetHashCode64(ref AdIdObj key) => Utility.GetHashCode(key.adId);

            public readonly bool Equals(ref AdIdObj k1, ref AdIdObj k2) => k1.adId == k2.adId;
        }

        public class Serializer : BinaryObjectSerializer<AdIdObj>
        {
            public override void Deserialize(out AdIdObj obj) => obj = new AdIdObj { adId = reader.ReadInt64() };

            public override void Serialize(ref AdIdObj obj) => writer.Write(obj.adId);
        }
    }

    public class NumClicksObj
    {
        public long numClicks;

        public class Serializer : BinaryObjectSerializer<NumClicksObj>
        {
            public override void Deserialize(out NumClicksObj obj) => obj = new NumClicksObj { numClicks = reader.ReadInt64() };

            public override void Serialize(ref NumClicksObj obj) => writer.Write(obj.numClicks);
        }
    }

    public class Input
    {
        public AdIdObj adId;
        public NumClicksObj numClicks;
    }

    public class Output
    {
        public NumClicksObj value;
    }

    public class Functions : SessionFunctionsBase<AdIdObj, NumClicksObj, Input, Output, Empty>
    {
        // Read functions
        public override bool SingleReader(ref AdIdObj key, ref Input input, ref NumClicksObj value, ref Output dst, ref ReadInfo readInfo)
        {
            dst.value = value;
            return true;
        }

        public override bool ConcurrentReader(ref AdIdObj key, ref Input input, ref NumClicksObj value, ref Output dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
        {
            dst.value = value;
            return true;
        }

        // RMW functions
        public override bool InitialUpdater(ref AdIdObj key, ref Input input, ref NumClicksObj value, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            value = input.numClicks;
            return true;
        }

        public override bool InPlaceUpdater(ref AdIdObj key, ref Input input, ref NumClicksObj value, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            _ = Interlocked.Add(ref value.numClicks, input.numClicks.numClicks);
            return true;
        }

        public override bool NeedCopyUpdate(ref AdIdObj key, ref Input input, ref NumClicksObj oldValue, ref Output output, ref RMWInfo rmwInfo) => true;

        public override bool CopyUpdater(ref AdIdObj key, ref Input input, ref NumClicksObj oldValue, ref NumClicksObj newValue, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            newValue = new NumClicksObj { numClicks = oldValue.numClicks + input.numClicks.numClicks };
            return true;
        }
    }
}