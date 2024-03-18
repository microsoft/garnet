// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using Tsavorite.core;

namespace Tsavorite.test.recovery.objectstore
{
    public class AdId : ITsavoriteEqualityComparer<AdId>
    {
        public long adId;

        public long GetHashCode64(ref AdId key)
        {
            return Utility.GetHashCode(key.adId);
        }
        public bool Equals(ref AdId k1, ref AdId k2)
        {
            return k1.adId == k2.adId;
        }
    }

    public class AdIdSerializer : BinaryObjectSerializer<AdId>
    {
        public override void Deserialize(out AdId obj)
        {
            obj = new AdId
            {
                adId = reader.ReadInt64()
            };
        }

        public override void Serialize(ref AdId obj)
        {
            writer.Write(obj.adId);
        }
    }

    public class Input
    {
        public AdId adId;
        public NumClicks numClicks;
    }

    public class NumClicks
    {
        public long numClicks;
    }

    public class NumClicksSerializer : BinaryObjectSerializer<NumClicks>
    {
        public override void Deserialize(out NumClicks obj)
        {
            obj = new NumClicks
            {
                numClicks = reader.ReadInt64()
            };
        }

        public override void Serialize(ref NumClicks obj)
        {
            writer.Write(obj.numClicks);
        }
    }


    public class Output
    {
        public NumClicks value;
    }

    public class Functions : FunctionsBase<AdId, NumClicks, Input, Output, Empty>
    {
        // Read functions
        public override bool SingleReader(ref AdId key, ref Input input, ref NumClicks value, ref Output dst, ref ReadInfo readInfo)
        {
            dst.value = value;
            return true;
        }

        public override bool ConcurrentReader(ref AdId key, ref Input input, ref NumClicks value, ref Output dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
        {
            dst.value = value;
            return true;
        }

        // RMW functions
        public override bool InitialUpdater(ref AdId key, ref Input input, ref NumClicks value, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            value = input.numClicks;
            return true;
        }

        public override bool InPlaceUpdater(ref AdId key, ref Input input, ref NumClicks value, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            Interlocked.Add(ref value.numClicks, input.numClicks.numClicks);
            return true;
        }

        public override bool NeedCopyUpdate(ref AdId key, ref Input input, ref NumClicks oldValue, ref Output output, ref RMWInfo rmwInfo) => true;

        public override bool CopyUpdater(ref AdId key, ref Input input, ref NumClicks oldValue, ref NumClicks newValue, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            newValue = new NumClicks { numClicks = oldValue.numClicks + input.numClicks.numClicks };
            return true;
        }
    }
}