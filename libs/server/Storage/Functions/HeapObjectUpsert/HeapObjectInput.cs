// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Input type for heap object upsert operations. The heapObject field carries the
    /// IGarnetObject to be inserted into the store.
    /// </summary>
    public struct HeapObjectInput
    {
        public IHeapObject heapObject;
    }
}
