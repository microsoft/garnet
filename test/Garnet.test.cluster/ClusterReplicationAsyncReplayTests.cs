// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;

namespace Garnet.test.cluster
{
    [TestFixture(false, true), NonParallelizable]
    public unsafe class ClusterReplicationAsyncReplayTests(bool UseTLS = false, bool asyncReplay = false) : ClusterReplicationTests(UseTLS, asyncReplay) { }
}