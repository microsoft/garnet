// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Garnet.client;
using NUnit.Framework;

namespace Garnet.test.client.GarnetClientAPI;

[TestFixture]
internal class GarnetClientBasicRespCommandsTests
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
    public void ShouldThrowArgumentNullExceptionForQuitWhenCallbackIsNull()
    {
        const Action<long, string> callback = null!;

        var exception = Assert.Throws<ArgumentNullException>(
            () => context.Quit(callback, 0));

        Assert.AreEqual(nameof(callback), exception!.ParamName);
    }
}