// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.server;
using NUnit.Framework;
using System;

namespace Garnet.test;

public class GeoHashTests
{
    [Test]
    [TestCase(0.0, 0.0, 0xC000000000000)]
    [TestCase(30.5388942218, 104.0555758833, 4024744861876082)]
    [TestCase(27.988056, 86.925278, 3636631039000829)]
    [TestCase(37.502669, 15.087269, 3476216502357864)]
    [TestCase(38.115556, 13.361389, 3476004292229755)]
    [TestCase(38.918250, -77.427944, 1787100258949719)]
    [TestCase(-89.0, -179.0, 111258114535)]
    // TODO: Investigate corner-cases
    // [TestCase(-90.0, -180.0, ?)]
    // [TestCase(90.0, 180.0, 0)]
    [TestCase(-90.0, -180.0, 0)]
    public void CanEncodeAndDecodeCoordinates(
        double latitude,
        double longitude,
        long expectedHashInteger)
    {
        var hashinteger = GeoHash.GeoToLongValue(latitude, longitude);
        Assert.AreEqual(expectedHashInteger, hashinteger);

        var (actualLatitude, actualLongitude) = GeoHash.GetCoordinatesFromLong(hashinteger);

        const double Epsilon = 0.00001;
        Assert.IsTrue(Math.Abs(latitude - actualLatitude) <= Epsilon);
        Assert.IsTrue(Math.Abs(longitude - actualLongitude) <= Epsilon);
    }

    [Test]
    [TestCase(27.988056, 86.925278, 3636631039000829, "tuvz4p141z8")]
    [TestCase(37.502669, 15.087269, 3476216502357864, "sqdtr74hyu0")]
    [TestCase(38.115556, 13.361389, 3476004292229755, "sqc8b49rnys")]
    [TestCase(38.918250, -77.427944, 1787100258949719, "dqbvqhfenps")]
    [TestCase(-89.0, -179.0, 111258114535, "000twy01mts")]
    // TODO: Investigate corner-cases
    // [TestCase(-90.0, -180.0, ?, ")]
    // [TestCase(0.0, 0.0, 0xC000000000000, "7zzzzzzzzzz")]
    [TestCase(90.0, 180.0, 0, "00000000000")]
    public void CanEncodeAndDecodeCoordinatesWithGeoHashCode(
        double latitude,
        double longitude,
        long expectedHashInteger,
        string expectedHash)
    {
        var hashInteger = GeoHash.GeoToLongValue(latitude, longitude);
        var hash = GeoHash.GetGeoHashCode(hashInteger);

        Assert.AreEqual(expectedHashInteger, hashInteger);
        // Only first 10 characters = 50-bits
        Assert.AreEqual(expectedHash[..10], hash[..10]);
    }
}