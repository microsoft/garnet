// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.common
{
    public class RespStrings
    {
        public static ReadOnlySpan<byte> EMPTYARRAY => "*0\r\n"u8;
        public static ReadOnlySpan<byte> INTEGERONE => ":1\r\n"u8;
        public static ReadOnlySpan<byte> INTEGERZERO => ":0\r\n"u8;
        public static ReadOnlySpan<byte> RESP2_NULLBULK => "$-1\r\n"u8;
        public static ReadOnlySpan<byte> RESP2_NULLARRAY => "*-1\r\n"u8;
        public static ReadOnlySpan<byte> RESP3_NULL => "_\r\n"u8;

        public static ReadOnlySpan<byte> VerbatimMarkdown => "mkd"u8;
        public static ReadOnlySpan<byte> VerbatimTxt => "txt"u8;
    }
}