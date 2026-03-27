// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    internal enum ExpirationOption : byte
    {
        None,
        EX,
        PX,
        EXAT,
        PXAT,
        KEEPTTL
    }

    internal enum ETagOption : byte
    {
        None,
        WithETag,
    }

    public enum ExistOptions : byte
    {
        None,
        NX,
        XX
    }
}