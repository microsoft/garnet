// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#nullable disable

namespace Garnet.server
{
    public struct Request
    {
        public byte[] buffer;
        public unsafe byte* bufferPtr;
    }
}