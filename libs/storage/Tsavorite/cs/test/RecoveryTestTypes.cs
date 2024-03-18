// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using Tsavorite.core;

namespace Tsavorite.test.recovery.sumstore
{
    public struct AdId : ITsavoriteEqualityComparer<AdId>
    {
        public long adId;

        public long GetHashCode64(ref AdId key) => Utility.GetHashCode(key.adId);

        public bool Equals(ref AdId k1, ref AdId k2) => k1.adId == k2.adId;

        public override string ToString() => adId.ToString();
    }

    public struct AdInput
    {
        public AdId adId;
        public NumClicks numClicks;

        public override string ToString() => $"id = {adId.adId}, clicks = {numClicks.numClicks}";
    }

    public struct NumClicks
    {
        public long numClicks;

        public override string ToString() => numClicks.ToString();
    }

    public struct Output
    {
        public NumClicks value;

        public override string ToString() => value.ToString();
    }

    public class Functions : FunctionsBase<AdId, NumClicks, AdInput, Output, Empty>
    {
        // Read functions
        public override bool SingleReader(ref AdId key, ref AdInput input, ref NumClicks value, ref Output dst, ref ReadInfo readInfo)
        {
            dst.value = value;
            return true;
        }

        public override bool ConcurrentReader(ref AdId key, ref AdInput input, ref NumClicks value, ref Output dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
        {
            dst.value = value;
            return true;
        }

        // RMW functions
        public override bool InitialUpdater(ref AdId key, ref AdInput input, ref NumClicks value, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            value = input.numClicks;
            return true;
        }

        public override bool InPlaceUpdater(ref AdId key, ref AdInput input, ref NumClicks value, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            Interlocked.Add(ref value.numClicks, input.numClicks.numClicks);
            return true;
        }

        public override bool NeedCopyUpdate(ref AdId key, ref AdInput input, ref NumClicks oldValue, ref Output output, ref RMWInfo rmwInfo) => true;

        public override bool CopyUpdater(ref AdId key, ref AdInput input, ref NumClicks oldValue, ref NumClicks newValue, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            newValue.numClicks += oldValue.numClicks + input.numClicks.numClicks;
            return true;
        }
    }
}