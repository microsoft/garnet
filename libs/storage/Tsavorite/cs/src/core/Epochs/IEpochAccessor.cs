// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    public interface IEpochAccessor
    {
        bool TrySuspend();
        void Resume();
    }
}