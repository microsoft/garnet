// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// Enconding and decoding methods for Geospatial
    /// </summary>
    public static class GeoHash
    {
        // Constraints from EPSG:900913 / EPSG:3785 / OSGEO:41001
        private const double LongitudeMin = -180.0;
        private const double LongitudeMax = 180.0;

        private const double LatitudeMin = -90.0;
        private const double LatitudeMax = 90.0;

        /// <summary>
        /// The number of bits used for the precision of the geohash.
        /// </summary>
        public const int BitsOfPrecision = 52;

        /// <summary>
        /// The length of the geohash "standard textual representation" given the <see cref="BitsOfPrecision">bit precision</see> is 52.
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

            const double LatToUnitRangeReciprocal = 1 / 180.0;
            const double LonToUnitRangeReciprocal = 1 / 360.0;

            // Credits to https://mmcloughlin.com/posts/geohash-assembly for the quantization approach!

            // The coordinates are quantized by first mapping them to the unit interval [0, 1] and
            // then multiplying the 2^32. For example, to get 32-bit quantized integer representation of the latitude
            // which is in range [-90, 90] we would do:
            //
            // latQuantized = floor(2.0^32 * (latitude + 90.0) / 180.0)
            //
            // However, some with clever math (See McLoughlin's great article above for the details!)
            // and by abusing the IEEE-754 representation of double-precision floating-point numbers
            // it is shown that the above calculation is equivalent to:
            //
            // floor(2^52 * x) where x = (latitude + 90.0) / 180.0.
            //
            // and further that the value of this can be read from binary representation of x+1.

            // In otherwords to quantize, we need to first map value to unit range [0, 1].
            // We achieve this by multiplying: [-value, value] * rangeReciprocal = [-0.5, 0.5]
            // Then by adding 1.5, we shift the value range to [1, 2],
            // for which the IEEE-754 double-precision representation is as follows:
            //
            // (-1)^sign * 2^(exp-1023) * (1.0 + significand/2^52)
            // where s=0, exp=0, significand=floor(2^52 * x)
            // 
            // Now we can read value of floor(2^52 * x) directly from binary representation of 1.0 + x,
            // where the now "quantized" value is stored as the 32 most significant bits of the signicand!
            static uint Quantize(double value, double rangeReciprocal) => (uint)(BitConverter.DoubleToUInt64Bits((value * rangeReciprocal) + 1.5) >> 20);

            var latQuantized = Quantize(latitude, LatToUnitRangeReciprocal);
            var lonQuantized = Quantize(longitude, LonToUnitRangeReciprocal);

            // Morton encode the quantized values, i.e.
            // latQuantBits = xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
            // lonQuantBits = yyyyyyyy yyyyyyyy yyyyyyyy yyyyyyyy

            var result = MortonEncode(x: latQuantized, y: lonQuantized);

            // Now:
            // resultBits   = xyxyxyxy xyxyxyxy xyxyxyxy xyxyxyxy
            //                xyxyxyxy xyxyxyxy xyxyxyxy xyxyxyxy

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
                // Construct the IEEE-754 double-precision representation of the value, which is in range [1, 2]
                var value = BitConverter.UInt64BitsToDouble(((ulong)quantizedValue << 20) | (1023UL << 52));

                // Now:
                // (2*rangeMax) * ([1, 2]-1) = [0, 2*rangeMax]
                // [0, 2*rangeMax] - rangeMax = [-rangeMax, rangeMax]
                return ((rangeMax + rangeMax) * (value - 1.0)) - rangeMax;
            }

            var fullHash = (ulong)hash << ((sizeof(ulong) * 8) - BitsOfPrecision);
            var (latQuantized, lonQuantized) = MortonDecode(fullHash);

            // The de-quantization gives us the lower-bounds of the bounding box.
            var minLatitude = Dequantize(latQuantized, LatitudeMax);
            var minLongitude = Dequantize(lonQuantized, LongitudeMax);

            // We get the bounding box upper-bounds by calculating the maximum error per given precision.
            var (latitudeError, longitudeError) = GetGeoErrorByPrecision();

            // We consider the center of the bounding-box to be our "coordinate" for given hash
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
        /// <returns>A ulong value representing the Morton encoding of the given coordinates.</returns>
        private static ulong MortonEncode(uint x, uint y)
        {
            // Note: This method could be implemented using 2x Bmi2.ParallelBitDeposit,
            // but the PDEP is emulated on AMD platforms Pre-Zen 3 so the perf. would fall from a cliff for those CPUs.
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

            return Spread(x) | (Spread(y) << 1);
        }

        /// <summary>
        /// Decodes the given 64-bit value into a pair of x- and y-coordinates using Morton decoding (also known as Z-order curve).
        /// <para />
        /// This is essentially a bit de-interleaving operation where the even and odd bits of <paramref name="x"/> are "squashed" into separate 32-bit values representing the x- and y-coordinates respectively.
        /// </summary>
        /// <param name="x">The 64-bit value to decode.</param>
        /// <returns>A tuple of values representing the x- and y-coordinates decoded from the given Morton code.</returns>
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

            return (Squash(x), Squash(x >> 1));
        }

        /// <summary>
        /// Encodes the given integer hash value using base-32 to the "standard textual representation".
        /// </summary>
        /// <param name="hash">The 52-bit geohash integer to encode.</param>
        /// <returns>The standard textual representation of the given 52-bit GeoHash integer</returns>
        public static string GetGeoHashCode(long hash)
        {
            return string.Create(CodeLength, state: hash, static (chars, hashState) =>
            {
                // Reference to the start of the base-32 char table, which is stored as constant data.
                ref var base32CharsBase = ref MemoryMarshal.GetReference("0123456789bcdefghjkmnpqrstuvwxyz"u8);

                for (var i = 0; i < chars.Length; i++)
                {
                    // Shift and mask the five most significant bits.
                    var tableIndex = (nuint)(hashState >> (BitsOfPrecision - 5)) & 0x1F;

                    // By masking the five bits, the tableIndex is now guaranteed to be <= 31 so this is safe.
                    chars[i] = (char)Unsafe.Add(ref base32CharsBase, tableIndex);

                    // Shift the encoded bits out.
                    hashState <<= 5;
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
        /// Find if a point is in the axis-aligned rectangle.
        /// when the distance between the searched point and the center point is less than or equal to 
        /// height/2 or width/2,
        /// the point is in the rectangle.
        /// </summary>
        public static bool GetDistanceWhenInRectangle(double widthMts, double heightMts, double latCenterPoint, double lonCenterPoint, double lat2, double lon2, ref double distance)
        {
            var lonDistance = Distance(lat2, lon2, latCenterPoint, lon2);
            var latDistance = Distance(lat2, lon2, lat2, lonCenterPoint);
            if (lonDistance > widthMts / 2 || latDistance > heightMts / 2)
            {
                return false;
            }

            distance = Distance(latCenterPoint, lonCenterPoint, lat2, lon2);
            return true;
        }

        /// <summary>
        /// Calculates the error in latitude and longitude based on <see cref="BitsOfPrecision">the number of bits used for precision</see> (52).
        /// </summary>
        public static (double LatError, double LonError) GetGeoErrorByPrecision()
        {
            const int LatBits = BitsOfPrecision / 2;
            const int LongBits = BitsOfPrecision - LatBits;

            // Equivalent to Math.Pow(2, e) but avoids calling into C runtime implementation.
            static double Pow2(long exponent) => BitConverter.Int64BitsToDouble((exponent + 1023L) << 52);

            var latError = 180.0 * Pow2(-LatBits);
            var longError = 360.0 * Pow2(-LongBits);

            return (latError, longError);
        }

        public static double ConvertValueToMeters(double value, ReadOnlySpan<byte> units)
        {
            if (units.Length == 2)
            {
                // Case-insensitive "km"
                if (AsciiUtils.ToLower(units[0]) == (byte)'k' && AsciiUtils.ToLower(units[1]) == (byte)'m')
                {
                    return value / 0.001;
                }
                // Case-insensitive "ft"
                if (AsciiUtils.ToLower(units[0]) == (byte)'f' && AsciiUtils.ToLower(units[1]) == (byte)'t')
                {
                    return value / 3.28084;
                }
                // Case-insensitive "mi"
                if (AsciiUtils.ToLower(units[0]) == (byte)'m' && AsciiUtils.ToLower(units[1]) == (byte)'i')
                {
                    return value / 0.000621371;
                }
            }

            return value;
        }


        /// <summary>
        /// Helper to convert meters to kilometers, feet, or miles
        /// </summary>
        public static double ConvertMetersToUnits(double value, ReadOnlySpan<byte> units)
        {
            if (units.Length == 2)
            {
                // Case-insensitive "km"
                if (AsciiUtils.ToLower(units[0]) == (byte)'k' && AsciiUtils.ToLower(units[1]) == (byte)'m')
                {
                    return value * 0.001;
                }
                // Case-insensitive "ft"
                if (AsciiUtils.ToLower(units[0]) == (byte)'f' && AsciiUtils.ToLower(units[1]) == (byte)'t')
                {
                    return value * 3.28084;
                }
                // Case-insensitive "mi"
                if (AsciiUtils.ToLower(units[0]) == (byte)'m' && AsciiUtils.ToLower(units[1]) == (byte)'i')
                {
                    return value * 0.000621371;
                }
            }

            return value;
        }
    }
}