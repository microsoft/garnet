// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace GarnetJSON
{
    /// <summary>
    /// Json Command strings for RESP protocol
    /// </summary>
    public static class JsonCmdStrings
    {
        public static ReadOnlySpan<byte> INDENT => "INDENT"u8;
        public static ReadOnlySpan<byte> NEWLINE => "NEWLINE"u8;
        public static ReadOnlySpan<byte> SPACE => "SPACE"u8;

        public static ReadOnlySpan<byte> RESP_NEW_OBJECT_AT_ROOT => "ERR new objects must be created at the root"u8;
        public static ReadOnlySpan<byte> RESP_WRONG_STATIC_PATH => "Err wrong static path"u8;
    }
}