// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    class SessionInfo
    {
        public bool isActive;
        public IClientSession session;
    }

    internal interface IClientSession
    {
        void MergeRevivificationStatsTo(ref RevivificationStats globalStats, bool reset);

        void ResetRevivificationStats();
    }
}