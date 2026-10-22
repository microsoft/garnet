// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if DEBUG
using Garnet.common;

namespace Garnet.server
{
    public sealed partial class VectorManager
    {
        internal ReadOptimizedLock VectorSetLocksForTest => vectorSetLocks;
    }
}
#endif
