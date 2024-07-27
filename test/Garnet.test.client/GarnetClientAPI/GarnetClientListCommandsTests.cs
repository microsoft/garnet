// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.client;
using NUnit.Framework;

namespace Garnet.test.client.GarnetClientAPI;

[TestFixture]
internal class GarnetClientListCommandsTests
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
    public void ShouldThrowArgumentNullExceptionForListLeftPushWhenKeyIsNull()
    {
        const string key = null!;
        const string element = nameof(element);
        Action<long, long, string> callback = (_, _, _) => { };

        var exception = Assert.Throws<ArgumentNullException>(
            () => context.ListLeftPush(key, element, callback, 0));

        Assert.AreEqual(nameof(key), exception!.ParamName);
    }

    [Test]
    public void ShouldThrowArgumentNullExceptionForListLeftPushWhenElementIsNull()
    {
        const string key = nameof(key);
        const string element = null!;
        Action<long, long, string> callback = (_, _, _) => { };

        var exception = Assert.Throws<ArgumentNullException>(
            () => context.ListLeftPush(key, element, callback, 0));

        Assert.AreEqual(nameof(element), exception!.ParamName);
    }

    [Test]
    public void ShouldThrowArgumentNullExceptionForListLeftPushWhenCallbackIsNull()
    {
        const string key = nameof(key);
        const string element = nameof(element);
        const Action<long, long, string> callback = null!;

        var exception = Assert.Throws<ArgumentNullException>(
            () => context.ListLeftPush(key, element, callback, 0));

        Assert.AreEqual(nameof(callback), exception!.ParamName);
    }
}