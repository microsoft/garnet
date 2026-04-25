// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Globalization;
using Allure.NUnit;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    [AllureNUnit]
    [TestFixture]
    public class GeoHashTests : AllureTestBase
    {
        [Test]
        [TestCase(30.5388942218, 104.0555758833)]
        [TestCase(27.988056, 86.925278)]
        [TestCase(37.502669, 15.087269)]
        [TestCase(38.115556, 13.361389)]
        [TestCase(38.918250, -77.427944)]
        [TestCase(-90.0, -180.0)]
        [TestCase(0.0, 0.0)]
        [TestCase(double.Epsilon, double.Epsilon)]
        [TestCase(-double.Epsilon, -double.Epsilon)]
        [TestCase(90.0, 180.0)]
        [TestCase(89.99999999999999, 179.99999999999997)] // double.BitDecrement((Lat/Long)Max)
        public void CanEncodeAndDecodeCoordinates(double latitude, double longitude)
        {
            const double Epsilon = 0.00001;

            var hashinteger = GeoHash.GeoToLongValue(latitude, longitude);

            var (actualLatitude, actualLongitude) = GeoHash.GetCoordinatesFromLong(hashinteger);

            var latError = Math.Abs(latitude - actualLatitude);
            var lonError = Math.Abs(longitude - actualLongitude);

            ClassicAssert.IsTrue(latError <= Epsilon, "Math.Abs(latError)=" + latError.ToString("F16", CultureInfo.InvariantCulture));
            ClassicAssert.IsTrue(lonError <= Epsilon, "Math.Abs(lonError)=" + latError.ToString("F16", CultureInfo.InvariantCulture));
        }

        [Test]
        [TestCase(30.5388942218, 104.0555758833, 4024744861876082, "wm3vxz6vyw0")]
        [TestCase(27.988056, 86.925278, 3636631039000829, "tuvz4p141z0")]
        [TestCase(37.502669, 15.087269, 3476216502357864, "sqdtr74hyu0")]
        [TestCase(38.115556, 13.361389, 3476004292229755, "sqc8b49rny0")]
        [TestCase(38.918250, -77.427944, 1787100258949719, "dqbvqhfenp0")]
        [TestCase(0.0, 0.0, 0xC000000000000, "s0000000000")]
        [TestCase(-90.0, -180.0, 0, "00000000000")]
        [TestCase(90.0, 180.0, 0xFFFFFFFFFFFFF, "zzzzzzzzzz0")]
        [TestCase(89.99999999999999, 179.99999999999997, 0xFFFFFFFFFFFFF, "zzzzzzzzzz0")]
        public void CanEncodeAndDecodeCoordinatesWithGeoHashCode(
            double latitude,
            double longitude,
            long expectedHashInteger,
            string expectedHash)
        {
            var hashInteger = GeoHash.GeoToLongValue(latitude, longitude);
            var hash = GeoHash.GetGeoHashCode(hashInteger);

            ClassicAssert.AreEqual(expectedHashInteger, hashInteger);

            // Note: while we are comparing the entire textual representation of geohash (11 characters),
            // the data is stored in 52-bit precision (not 55-bit as required by the GeoHash standard).
            // For compatibility with Redis, the last character is always '0'.
            ClassicAssert.AreEqual(expectedHash, hash);
        }
    }
}