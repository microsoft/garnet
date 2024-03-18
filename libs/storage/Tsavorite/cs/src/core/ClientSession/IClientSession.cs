// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    class SessionInfo
    {
        public string sessionName;
        public bool isActive;
        public IClientSession session;
    }

    internal interface IClientSession
    {
        void AtomicSwitch(long version);

        void MergeRevivificationStatsTo(ref RevivificationStats globalStats, bool reset);

        void ResetRevivificationStats();
    }
}