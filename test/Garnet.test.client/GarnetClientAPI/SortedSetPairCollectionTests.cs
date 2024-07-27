// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.client.GarnetClientAPI;
using NUnit.Framework;

namespace Garnet.test.client.GarnetClientAPI;

[TestFixture]
internal class SortedSetPairCollectionTests
{
    private SortedSetPairCollection context;

    [SetUp]
    public void Setup()
    {
        context = new SortedSetPairCollection();
    }

    [Test]
    public void ShouldThrowArgumentNullExceptionForAddSortedSetEntryWhenMemberIsNull()
    {
        const byte[] member = null!;
        const double score = 0d;

        var exception = Assert.Throws<ArgumentNullException>(
            () => context.AddSortedSetEntry(member, score));

        Assert.AreEqual(nameof(member), exception!.ParamName);
    }
}