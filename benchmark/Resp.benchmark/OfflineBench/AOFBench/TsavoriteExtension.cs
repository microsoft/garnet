// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;
using System.Reflection;

namespace Resp.benchmark
{
    internal static class TsavoriteExtension
    {
        private static readonly MethodInfo AlignMethod;
        private static readonly FieldInfo HeaderSizeField;
        private static readonly MethodInfo SetHeaderMethod;

        static TsavoriteExtension()
        {
            var type = typeof(TsavoriteLog);
            var flags = BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static;

            AlignMethod = type.GetMethod("Align", BindingFlags.NonPublic | BindingFlags.Static);
            HeaderSizeField = type.GetField("headerSize", flags);
            SetHeaderMethod = type.GetMethod("SetHeader", flags);
        }

        public static unsafe bool DummyEnqueue<THeader, TInput>(
            this TsavoriteLog log,
            ref byte* beginPageAddress,
            byte* endPageAddress,
            THeader userHeader,
            ref SpanByte item1,
            ref SpanByte item2,
            ref TInput input)
            where THeader : unmanaged where TInput : IStoreInput
        {
            var headerSize = (int)HeaderSizeField.GetValue(log);
            var length = sizeof(THeader) + item1.TotalSize + item2.TotalSize + input.SerializedLength;
            var allocatedLength = headerSize + (int)AlignMethod.Invoke(null, [length]);

            if (beginPageAddress + allocatedLength > endPageAddress)
                return false;

            *(THeader*)(beginPageAddress + headerSize) = userHeader;
            item1.CopyTo(beginPageAddress + headerSize + sizeof(THeader));
            item2.CopyTo(beginPageAddress + headerSize + sizeof(THeader) + item1.TotalSize);
            input.CopyTo(beginPageAddress + headerSize + sizeof(THeader) + item1.TotalSize + item2.TotalSize, input.SerializedLength);

            SetHeaderMethod.Invoke(log, [length, (IntPtr)beginPageAddress]);
            beginPageAddress += allocatedLength;
            return true;
        }
    }
}