// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.client;
using NUnit.Framework;

namespace Garnet.test.client.GarnetClientAPI;

[TestFixture]
internal class GarnetClientSortedSetCommandsTests
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
    public void ShouldThrowArgumentNullExceptionForSortedSetAddWhenKeyIsNull()
    {
        const string key = null!;
        const string member = nameof(member);
        const double score = 0d;
        Action<long, long, string> callback = (_, _, _) => { };

        var exception = Assert.Throws<ArgumentNullException>(
            () => context.SortedSetAdd(key, member, score, callback, 0));

        Assert.AreEqual(nameof(key), exception!.ParamName);
    }

    [Test]
    public void ShouldThrowArgumentNullExceptionForSortedSetAddWhenMemberIsNull()
    {
        const string key = nameof(key);
        const string member = null!;
        const double score = 0d;
        Action<long, long, string> callback = (_, _, _) => { };

        var exception = Assert.Throws<ArgumentNullException>(
            () => context.SortedSetAdd(key, member, score, callback, 0));

        Assert.AreEqual(nameof(member), exception!.ParamName);
    }

    [Test]
    public void ShouldThrowArgumentNullExceptionForSortedSetAddWhenCallbackIsNull()
    {
        const string key = nameof(key);
        const string member = nameof(member);
        const double score = 0d;
        const Action<long, long, string> callback = null!;

        var exception = Assert.Throws<ArgumentNullException>(
            () => context.SortedSetAdd(key, member, score, callback, 0));

        Assert.AreEqual(nameof(callback), exception!.ParamName);
    }
}