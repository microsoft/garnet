// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.common;

/// <summary>
/// Utilites for ASCII parsing and manipulation.
/// </summary>
public static class AsciiUtils
{
    public static byte ToLower(byte value)
    {
        if ((uint)(value - 'A') <= (uint)('Z' - 'A')) // Is in [A-Z]
            value = (byte)(value | 0x20);
        return value;
    }
}