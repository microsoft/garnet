// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// Enconding and decoding methods for Geospatial
    /// </summary>
    public static class GeoHash
    {
        // Constraints from EPSG:900913 / EPSG:3785 / OSGEO:41001
        private const double GeoLongitudeMin = -180.0;
        private const double GeoLongitudeMax = 180.0;

        private const double GeoLatitudeMin = -90.0;
        private const double GeoLatitudeMax = 90.0;

        private const int Precision = 52;

        //The "Geohash alphabet" (32ghs) uses all digits 0-9 and almost all lower case letters except "a", "i", "l" and "o".
        //This table is used for getting the "standard textual representation" of a pair of lat and long.
        static readonly char[] base32chars = new char[] { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z' };


        /// <summary>
        /// Encodes the tuple of (<paramref name="latitude"/>, <paramref name="longitude"/>) coords to a unique 52-bit integer
        /// </summary>
        public static long GeoToLongValue(double latitude, double longitude)
        {
            if (!(GeoLatitudeMin <= latitude && latitude <= GeoLatitudeMax) ||
                !(GeoLongitudeMin <= longitude && longitude <= GeoLongitudeMax))
            {
                return -1;
            }

            double latitudeMin = GeoLatitudeMin, latitudeMax = GeoLatitudeMax;
            double longitudeMin = GeoLongitudeMin, longitudeMax = GeoLongitudeMax;

            long result = 0;
            for (int i = 0; i < Precision; i++)
            {
                result <<= 1;
                if (i % 2 == 0)
                {
                    Encode(longitude, ref longitudeMin, ref longitudeMax, ref result);
                }
                else
                {
                    Encode(latitude, ref latitudeMin, ref latitudeMax, ref result);
                }
            }

            return result;
        }

        /// <summary>
        /// Gets pair (latitude, longitude) of GPS coordinates
        /// latitude comes before longitude in the ISO 6709 standard
        /// https://en.wikipedia.org/wiki/ISO_6709#Order,_sign,_and_units
        /// Latitude refers to the Y-values and are between -90 and +90 degrees.
        /// Longitude refers to the X-coordinates and are between -180 and +180 degrees.
        /// </summary>
        public static (double Latitude, double Longitude) GetCoordinatesFromLong(long longValue)
        {
            double latitudeMin = GeoLatitudeMin, latitudeMax = GeoLatitudeMax;
            double longitudeMin = GeoLongitudeMin, longitudeMax = GeoLongitudeMax;

            for (int i = Precision - 1; i >= 0; i--)
        {
                bool bit = ((longValue >> i) & 1) == 1;

                if (i % 2 == 0)
            {
                    Decode(ref latitudeMin, ref latitudeMax, bit);
            }
                else
            {
                    Decode(ref longitudeMin, ref longitudeMax, bit);
                }
            }

            var latitude = (latitudeMin + latitudeMax) / 2;
            var longitude = (longitudeMin + longitudeMax) / 2;
            return (latitude, longitude);
        }

        /// <summary>
        /// Gets the base32 value
        /// </summary>
        /// <param name="longEncodedValue"></param>
        /// <returns>The GeoHash representation of the 52bit</returns>
        public static string GetGeoHashCode(long longEncodedValue)
        {
            // Length for the GeoHash
            const int CodeLength = 11;

            string result = string.Empty;
            long hashValue = 0;

            double latitudeMin = GeoLatitudeMin, latitudeMax = GeoLatitudeMax;
            double longitudeMin = GeoLongitudeMin, longitudeMax = GeoLongitudeMax;

            var (latitude, longitude) = GetCoordinatesFromLong(longEncodedValue);

            // check for invalid values
            if (!(GeoLatitudeMin <= latitude && latitude <= GeoLatitudeMax) || !(GeoLongitudeMin <= longitude && longitude <= GeoLongitudeMax))
                return null;

            int bits = 0;

            while (result.Length < CodeLength)
            {
                hashValue <<= 1;
                if (bits % 2 == 0)
                {
                    Encode(longitude, ref longitudeMin, ref longitudeMax, ref hashValue);
                }
                else
            {
                    Encode(latitude, ref latitudeMin, ref latitudeMax, ref hashValue);
                }

                bits++;
                if (bits != 5)
                {
                    continue;
                }
                var code = base32chars[hashValue];
                result += code;
                bits = 0;
                hashValue = 0;
            }

            return result;
        }

        private static void Encode(double value, ref double min, ref double max, ref long result)
        {
            var mid = (min + max) / 2;
            if (value > mid)
            {
                min = mid;
                result |= 1;
            }
            else
            {
                max = mid;
            }
        }

        private static void Decode(ref double min, ref double max, bool isOnBit)
        {
            var mid = (min + max) / 2;
            if (isOnBit)
            {
                min = mid;
            }
            else
            {
                max = mid;
            }
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

            return 2 * Math.Asin(Math.Sqrt(latHaversine + tmp * lonHaversine)) * EarthRadiusInMeters;
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