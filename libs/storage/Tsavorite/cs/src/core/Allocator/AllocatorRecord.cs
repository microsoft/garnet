// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Tsavorite.core
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct AllocatorRecord<TKey, TValue>
    {
        public RecordInfo info;
        public TKey key;
        public TValue value;

        public override string ToString()
        {
            var keyString = key?.ToString() ?? "null";
            if (keyString.Length > 20)
                keyString = keyString.Substring(0, 20) + "...";
            var valueString = value?.ToString() ?? "null"; ;
            if (valueString.Length > 20)
                valueString = valueString.Substring(0, 20) + "...";
            return $"{keyString} | {valueString} | {info}";
        }
    }
}