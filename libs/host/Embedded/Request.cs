// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#nullable disable

namespace Embedded.server
{
    public struct Request
    {
        public byte[] buffer;
        public unsafe byte* bufferPtr;
    }
}