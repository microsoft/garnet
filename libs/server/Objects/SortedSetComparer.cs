// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;

namespace Garnet.server
{
    class SortedSetComparer : IComparer<(double, byte[])>
    {
        public int Compare((double, byte[]) x, (double, byte[]) y)
        {
            var ret = x.Item1.CompareTo(y.Item1);
            if (ret == 0)
                return new ReadOnlySpan<byte>(x.Item2).SequenceCompareTo(y.Item2);
            return ret;
        }
    }
}