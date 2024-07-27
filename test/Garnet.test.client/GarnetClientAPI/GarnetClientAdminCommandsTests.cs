// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Garnet.client;
using NUnit.Framework;

namespace Garnet.test.client.GarnetClientAPI;

[TestFixture]
internal class GarnetClientAdminCommandsTests
{
    private GarnetClient context;

    [SetUp]
    public void Setup()
    {
        context = new GarnetClient("garnet", 9999);
    }

    [TearDown]
    public void TearDown()
    {
        context.Dispose();
    }

    [Test]
    public void ShouldThrowArgumentNullExceptionForSaveWhenCallbackIsNull()
    {
        const Action<long, string> callback = null!;

        var exception = Assert.Throws<ArgumentNullException>(
            () => context.Save(callback, 0));

        Assert.AreEqual(nameof(callback), exception!.ParamName);
    }

    [Test]
    public void ShouldThrowArgumentNullExceptionForReplicaOfWhenAddressIsNull()
    {
        const string address = null!;
        var port = 100;

        var exception = Assert.ThrowsAsync<ArgumentNullException>(
            async () => await context.ReplicaOf(address, port, default(CancellationToken)));

        Assert.AreEqual(nameof(address), exception!.ParamName);
    }
}