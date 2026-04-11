// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.server
{
    [Flags]
    internal enum GeoAddOptions : byte
    {
        None = 0,
        NX = 1 << 0,
        XX = 1 << 1,
        CH = 1 << 2
    }

    /// <summary>
    /// Distance Unit for GeoSearch
    /// </summary>
    public enum GeoDistanceUnitType : byte
    {
        /// <summary>
        /// Meters
        /// </summary>
        M = 0,

        /// <summary>
        /// Kilometers
        /// </summary>
        KM,

        /// <summary>
        /// Miles
        /// </summary>
        MI,

        /// <summary>
        /// Foot
        /// </summary>
        FT
    }

    /// <summary>
    /// The direction in which to sequence elements.
    /// </summary>
    internal enum GeoOrder : byte
    {
        /// <summary>
        /// No defined order.
        /// </summary>
        None = 0,

        /// <summary>
        /// Order from low values to high values.
        /// </summary>
        Ascending,

        /// <summary>
        /// Order from high values to low values.
        /// </summary>
        Descending
    }

    internal enum GeoOriginType : byte
    {
        /// <summary>
        /// Not defined.
        /// </summary>
        Undefined = 0,

        /// <summary>
        /// From explicit lon lat coordinates.
        /// </summary>
        FromLonLat,

        /// <summary>
        /// From member key
        /// </summary>
        FromMember
    }

    /// <summary>
    /// Type of GeoSearch
    /// </summary>
    internal enum GeoSearchType : byte
    {
        /// <summary>
        /// No defined order.
        /// </summary>
        Undefined = 0,

        /// <summary>
        /// Search inside circular area
        /// </summary>
        ByRadius,

        /// <summary>
        /// Search inside an axis-aligned rectangle
        /// </summary>
        ByBox
    }

    /// <summary>
    /// Small struct to store options for GEOSEARCH command
    /// </summary>
    public ref struct GeoSearchOptions
    {
        internal GeoSearchType searchType;
        internal GeoDistanceUnitType unit;
        internal double radius;
        internal double boxWidth;
        internal double boxHeight
        {
            get
            {
                return radius;
            }
            set
            {
                radius = value;
            }
        }

        internal int countValue;

        internal GeoOriginType origin;

        internal byte[] fromMember;
        internal double lon, lat;

        internal bool withCoord;
        internal bool withHash;
        internal bool withCountAny;
        internal bool withDist;
        internal GeoOrder sort;
    }
}