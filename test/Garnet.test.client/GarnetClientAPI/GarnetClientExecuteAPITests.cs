// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.client;
using NUnit.Framework;

namespace Garnet.test.client.GarnetClientAPI;

[TestFixture]
internal class GarnetClientExecuteAPITests
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
    public void ShouldThrowArgumentNullExceptionForExecuteForStringResultAsyncWhenOpIsNull()
    {
        const string op = null!;

        var exception = Assert.ThrowsAsync<ArgumentNullException>(
            async () => await context.ExecuteForStringResultAsync(op, default(ICollection<string>)));

        Assert.AreEqual(nameof(op), exception!.ParamName);
    }
}