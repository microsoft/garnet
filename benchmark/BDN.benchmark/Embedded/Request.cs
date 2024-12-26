// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Embedded.server
{
    public struct Request
    {
        public byte[] buffer;
        public unsafe byte* bufferPtr;
    }
}