// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Garnet.Diagnostics
{
    [InterpolatedStringHandler]
    public ref struct AssertMessageHandler
    {
        private DefaultInterpolatedStringHandler handler;

        public AssertMessageHandler(int literalLength, int formattedCount, bool condition, out bool shouldAppend)
        {
            shouldAppend = !condition;
            handler = shouldAppend ? new DefaultInterpolatedStringHandler(literalLength, formattedCount) : default;
        }

        public void AppendLiteral(string value) => handler.AppendLiteral(value);

        public void AppendFormatted<T>(T value) => handler.AppendFormatted(value);

        public void AppendFormatted<T>(T value, string format) => handler.AppendFormatted(value, format);

        public void AppendFormatted<T>(T value, int alignment) => handler.AppendFormatted(value, alignment);

        public void AppendFormatted<T>(T value, int alignment, string format) => handler.AppendFormatted(value, alignment, format);

        public void AppendFormatted(ReadOnlySpan<char> value, int alignment = 0, string format = null) => handler.AppendFormatted(value, alignment, format);

        public void AppendFormatted(string value) => handler.AppendFormatted(value);

        public void AppendFormatted(string value, int alignment = 0, string format = null) => handler.AppendFormatted(value, alignment, format);

        internal string GetFormattedText() => handler.ToStringAndClear();
    }
}