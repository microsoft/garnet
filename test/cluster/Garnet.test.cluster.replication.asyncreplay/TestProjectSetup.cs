// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.test.cluster;
using NUnit.Framework;

[SetUpFixture]
public class TestProjectSetup
{
    [OneTimeSetUp]
    public void SetPort() => ClusterTestContext.Port = (int)ClusterPortAssignment.ClusterReplicationAsync;
}
