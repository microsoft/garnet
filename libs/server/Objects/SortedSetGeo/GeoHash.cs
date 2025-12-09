// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.X86;

namespace Garnet.server
{
    /// <summary>
    /// Encoding and decoding methods for Geospatial
    /// </summary>
    public static class GeoHash
    {
        // Constraints from WGS 84 / Pseudo-Mercator (EPSG:3857)

        /// <summary>
        /// Minimum allowed longitude.
        /// </summary>
        public const double LongitudeMin = -180.0;
        /// <summary>
        /// Maximum allowed longitude.
        /// </summary>
        public const double LongitudeMax = 180.0;

        // TODO: These are "wrong" in a sense that according to EPSG:3857 latitude should be from -85.05112878 to 85.05112878

        /// <summary>
        /// Minimum allowed latitude.
        /// </summary>
        public const double LatitudeMin = -90.0;
        /// <summary>
        /// Maximum allowed latitude.
        /// </summary>
        public const double LatitudeMax = 90.0;

        /// <summary>
        /// The number of bits used for the precision of the geohash.
        /// </summary>
        public const int BitsOfPrecision = 52;

        /// <summary>
        /// The length of the geohash "standard textual representation".
        /// </summary>
        public const int CodeLength = 11;

        /// <summary>
        /// Encodes the tuple of (<paramref name="latitude"/>, <paramref name="longitude"/>) coordinates to a unique 52-bit integer
        /// </summary>
        public static long GeoToLongValue(double latitude, double longitude)
        {
            if (!(LatitudeMin <= latitude && latitude <= LatitudeMax) ||
                !(LongitudeMin <= longitude && longitude <= LongitudeMax))
            {
                return -1L;
            }

            // Credits to https://mmcloughlin.com/posts/geohash-assembly for the quantization approach!

            // The coordinates are quantized by first mapping them to the unit interval [0.0, 1.0] and
            // then multiplying the 2^32. For example, to get 32-bit quantized integer representation of the latitude
            // which is in range [-90.0, 90.0] we would do:
            //
            // latQuantized = floor(2.0^32 * (latitude + 90.0) / 180.0)
            //
            // However, some with clever math it is shown that the result of above calculation can be read
            // from the IEEE-754 double-precision binary representation of x + 1.0

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static uint Quantize(double value, double rangeReciprocal)
            {
                // In other words; we need to first map value to unit range [0.0, 1.0].
                // We achieve this by multiplying [-value, value] by rangeReciprocal, giving us value in range [-0.5, 0.5]
                // Then by adding 1.5, we shift the value range to [1.0, 2.0],
                // for which the IEEE-754 double-precision representation is as follows:
                //
                // (-1)^sign * 2^(exp-1023) * (1.0 + significand/2^52)
                // where sign=0, exp=1023, significand=floor(2^52 * x), for x in [1.0, 2.0)
                //
                // Now we can read value of floor(2^52 * x) directly from binary representation of y = 1.0 + x,
                // where the now "quantized" value is stored as the 32 most significant bits of the signicand!
                var y = BitConverter.DoubleToUInt64Bits(Math.FusedMultiplyAdd(value, rangeReciprocal, 1.5)) >> 20;

                // But we need to handle the corner-case where value rounds to the maximum of the range: 2.0
                // We handle this by comparing the shifted 64-bit binary representation
                // to the shifted representation of 2.0 (JIT folds it as constant).
                if (y == (BitConverter.DoubleToUInt64Bits(2.0) >> 20))
                {
                    return uint.MaxValue;
                }
                else return (uint)y;
            }

            const double LatToUnitRangeReciprocal = 1 / 180.0;
            const double LonToUnitRangeReciprocal = 1 / 360.0;

            var latQuantized = Quantize(latitude, LatToUnitRangeReciprocal);
            var lonQuantized = Quantize(longitude, LonToUnitRangeReciprocal);

            // Morton encode the quantized values
            var result = MortonEncode(x: latQuantized, y: lonQuantized);

            // Shift to 52-bit precision.
            return (long)(result >> ((sizeof(ulong) * 8) - BitsOfPrecision));
        }

        /// <summary>
        /// Gets pair (latitude, longitude) of GPS coordinates
        /// latitude comes before longitude in the ISO 6709 standard
        /// https://en.wikipedia.org/wiki/ISO_6709#Order,_sign,_and_units
        /// Latitude refers to the Y-values and are between -90 and +90 degrees.
        /// Longitude refers to the X-coordinates and are between -180 and +180 degrees.
        /// </summary>
        public static (double Latitude, double Longitude) GetCoordinatesFromLong(long hash)
        {
            // Credits to https://github.com/georust/geohash for the hash de-quantization method!
            static double Dequantize(uint quantizedValue, double rangeMax)
            {
                // Construct the IEEE-754 double-precision representation of the value, which is in range [1.0, 2.0)
                var value = BitConverter.UInt64BitsToDouble(((ulong)quantizedValue << 20) | (1023UL << 52));

                // Now:
                // (2*rangeMax) * ([1.0, 2.0) - 1.0) = [0.0, 2*rangeMax)
                // [0.0, 2*rangeMax) - rangeMax = [-rangeMax, rangeMax)
                return Math.FusedMultiplyAdd(rangeMax + rangeMax, value - 1.0, -rangeMax);
            }

            var fullHash = (ulong)hash << ((sizeof(ulong) * 8) - BitsOfPrecision);
            var (latQuantized, lonQuantized) = MortonDecode(fullHash);

            // The de-quantization gives us the lower-bounds of the bounding box.
            var minLatitude = Dequantize(latQuantized, LatitudeMax);
            var minLongitude = Dequantize(lonQuantized, LongitudeMax);

            // We get the bounding box upper-bounds by calculating the maximum error per given precision.
            var (latitudeError, longitudeError) = GetGeoErrorByPrecision();

            // We consider the center of the bounding box to be our "coordinate" for given hash
            return (
                Latitude: minLatitude + (latitudeError / 2.0),
                Longitude: minLongitude + (longitudeError / 2.0));
        }


        /// <summary>
        /// Encodes the given x- and y-coordinates into a single 64-bit value using Morton encoding (also known as Z-order curve).
        /// <para />
        /// This is essentially a bit interleaving where <paramref name="x"/> and <paramref name="y"/> are "spread" on even and odd bits respectively.
        /// </summary>
        /// <param name="x">The x-coordinate to encode.</param>
        /// <param name="y">The y-coordinate to encode.</param>
        /// <returns>
        /// A 64-bit value representing the Morton encoding of the given coordinates.
        /// i.e. in binary representation, for:
        /// <code>
        /// x = xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
        /// y = yyyyyyyy yyyyyyyy yyyyyyyy yyyyyyyy
        /// </code>
        /// Method returns:
        /// <code>
        /// yxyxyxyx yxyxyxyx yxyxyxyx yxyxyxyx
        /// yxyxyxyx yxyxyxyx yxyxyxyx yxyxyxyx
        /// </code>
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ulong MortonEncode(uint x, uint y)
        {
            static ulong Spread(uint x)
            {
                ulong y = x;
                y = (y | (y << 16)) & 0x0000FFFF0000FFFF;
                y = (y | (y << 8)) & 0x00FF00FF00FF00FF;
                y = (y | (y << 4)) & 0x0F0F0F0F0F0F0F0F;
                y = (y | (y << 2)) & 0x3333333333333333;
                y = (y | (y << 1)) & 0x5555555555555555;
                return y;
            }

            // One may ask: Why is this also guarded behind AVX512F in addition to BMI2?
            // The answer is that on AMD platforms before Zen 3, the PDEP (and PEXT) are implemented in microcode
            // and work bit-by-bit basis. It has been measured[^1] that for every bit set in the mask operand,
            // there is 8~ uops issued, meaning that we would do 32 bits per mask * 8 uops * 2 = 512~ uops in total for the encoding instead of just 1 uop.
            //
            // By guarding with AVX512F support, we avoid going to this code path for Zen 3 and older platforms. Avx512F.IsSupported check is the lowest possible 
            // check we can (as of .NET 8) to allow largest possible set of CPUs to utilize accelerated PDEP and PEXT code-path.
            //
            // [1]: https://twitter.com/uops_info/status/1202984196739870722
            if (Bmi2.X64.IsSupported && Avx512F.IsSupported)
            {
                return Bmi2.X64.ParallelBitDeposit(x, 0x5555555555555555)
                    | Bmi2.X64.ParallelBitDeposit(y, 0xAAAAAAAAAAAAAAAA);
            }

            return Spread(x) | (Spread(y) << 1);
        }

        /// <summary>
        /// Decodes the given 64-bit value into a pair of x- and y-coordinates using Morton decoding (also known as Z-order curve).
        /// <para />
        /// This is essentially a bit de-interleaving operation where the even and odd bits of <paramref name="x"/> are "squashed" into separate 32-bit values representing the x- and y-coordinates respectively.
        /// </summary>
        /// <param name="x">The 64-bit value to decode.</param>
        /// <returns>A tuple of 32-bit values representing the x- and y-coordinates decoded from the given Morton code.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static (uint X, uint Y) MortonDecode(ulong x)
        {
            static uint Squash(ulong x)
            {
                var y = x & 0x5555555555555555;
                y = (y | (y >> 1)) & 0x3333333333333333;
                y = (y | (y >> 2)) & 0x0F0F0F0F0F0F0F0F;
                y = (y | (y >> 4)) & 0x00FF00FF00FF00FF;
                y = (y | (y >> 8)) & 0x0000FFFF0000FFFF;
                y = (y | (y >> 16)) & 0x00000000FFFFFFFF;
                return (uint)y;
            }

            // See the rationale for the AVX512F guard in the MortonEncode method
            if (Bmi2.X64.IsSupported && Avx512F.IsSupported)
            {
                return (
                    X: (uint)Bmi2.X64.ParallelBitExtract(x, 0x5555555555555555),
                    Y: (uint)Bmi2.X64.ParallelBitExtract(x, 0xAAAAAAAAAAAAAAAA));
            }

            return (Squash(x), Squash(x >> 1));
        }

        /// <summary>
        /// Encodes the given integer hash value using base-32 to the "standard textual representation".
        /// </summary>
        /// <param name="hash">The 52-bit geohash integer to encode.</param>
        /// <returns>The standard textual representation of the given 52-bit GeoHash integer</returns>
        public static string GetGeoHashCode(long hash)
        {
            return string.Create(CodeLength, state: hash, static (chars, hash) =>
            {
                var base32Chars = "0123456789bcdefghjkmnpqrstuvwxyz"u8;

                for (var i = 0; i < chars.Length; i++)
                {
                    // Shift and mask the five most significant bits for index to the base-32 table.
                    chars[i] = (char)base32Chars[(int)(hash >> (BitsOfPrecision - 5)) & 0x1F];

                    // Shift the encoded bits out.
                    hash <<= 5;
                }
            });
        }

        /// <summary>
        /// Gets the distance in meters using Haversine Formula
        /// https://en.wikipedia.org/wiki/Haversine_formula
        /// </summary>
        public static double Distance(double sourceLat, double sourceLon, double targetLat, double targetLon)
        {
            static double DegreesToRadians(double degrees) => degrees * Math.PI / 180;

            //Measure based on WGS-84 system
            const double EarthRadiusInMeters = 6372797.560856;

            var lonRadians = DegreesToRadians(sourceLon - targetLon);
            var lonHaversine = Math.Pow(Math.Sin(lonRadians / 2), 2);

            var latRadians = DegreesToRadians(sourceLat - targetLat);
            var latHaversine = Math.Pow(Math.Sin(latRadians / 2), 2);

            var tmp = Math.Cos(DegreesToRadians(sourceLat)) * Math.Cos(DegreesToRadians(targetLat));

            return 2 * Math.Asin(Math.Sqrt(latHaversine + (tmp * lonHaversine))) * EarthRadiusInMeters;
        }

        /// <summary>
        /// Find if a point is within radius of the given center point.
        /// <paramref name="radius">Radius</paramref>
        /// <paramref name="latCenterPoint">Center point latitude</paramref>
        /// <paramref name="lonCenterPoint">Center point longitude</paramref>
        /// <paramref name="lat">Point latitude</paramref>
        /// <paramref name="lon">Point longitude</paramref>
        /// <paramref name="distance">Distance</paramref>
        /// </summary>
        public static bool IsPointWithinRadius(double radius, double latCenterPoint, double lonCenterPoint, double lat, double lon, ref double distance)
        {
            distance = Distance(latCenterPoint, lonCenterPoint, lat, lon);
            if (distance >= radius)
            {
                return false;
            }

            return true;
        }

        /// <summary>
        /// Find if a point is in the axis-aligned rectangle.
        /// when the distance between the searched point and the center point is less than or equal to 
        /// height/2 or width/2,
        /// the point is in the rectangle.
        /// </summary>
        public static bool GetDistanceWhenInRectangle(double widthMts, double heightMts, double latCenterPoint, double lonCenterPoint, double lat2, double lon2, ref double distance)
        {
            var lonDistance = Distance(lat2, lon2, lat2, lonCenterPoint);
            var latDistance = Distance(lat2, lon2, latCenterPoint, lon2);
            if (lonDistance > widthMts / 2 || latDistance > heightMts / 2)
            {
                return false;
            }

            distance = Distance(latCenterPoint, lonCenterPoint, lat2, lon2);
            return true;
        }

        /// <summary>
        /// Calculates the error in latitude and longitude based on <see cref="BitsOfPrecision">the number of bits used for precision</see>.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static (double LatError, double LonError) GetGeoErrorByPrecision()
        {
            const int LatBits = BitsOfPrecision / 2;
            const int LongBits = BitsOfPrecision - LatBits;

            var latError = 180.0 * Math.Pow(2, -LatBits);
            var longError = 360.0 * Math.Pow(2, -LongBits);

            return (latError, longError);
        }

        /// <summary>
        /// Helper to convert kilometers, feet, or miles to meters.
        /// </summary>
        public static double ConvertValueToMeters(double value, GeoDistanceUnitType unit)
        {
            return unit switch
            {
                GeoDistanceUnitType.KM => value / 0.001,
                GeoDistanceUnitType.FT => value / 3.28084,
                GeoDistanceUnitType.MI => value / 0.000621371,
                _ => value
            };
        }

        /// <summary>
        /// Helper to convert meters to kilometers, feet, or miles
        /// </summary>
        public static double ConvertMetersToUnits(double value, GeoDistanceUnitType unit)
        {
            return unit switch
            {
                GeoDistanceUnitType.KM => value * 0.001,
                GeoDistanceUnitType.FT => value * 3.28084,
                GeoDistanceUnitType.MI => value * 0.000621371,
                _ => value
            };
        }
    }
}