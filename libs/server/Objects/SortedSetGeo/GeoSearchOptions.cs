using System;
using System.Text;
using Garnet.common;

namespace Garnet.server
{
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

        public static unsafe ReadOnlySpan<byte> Parse(RespCommand command, 
                                                      ref GeoSearchOptions searchOpts,
                                                      ref ObjectInput input,
                                                      bool readOnly,
                                                      out int destIdx)
        {
            destIdx = command == RespCommand.GEOSEARCHSTORE ? 0 : -1;

            string errorMessage = default;
            var argNumError = false;
            var storeDist = false;
            var currTokenIdx = 0;

            if (command == RespCommand.GEORADIUS || command == RespCommand.GEORADIUS_RO ||
                command == RespCommand.GEORADIUSBYMEMBER || command == RespCommand.GEORADIUSBYMEMBER_RO)
            {
                // Read coordinates, note we already checked the number of arguments earlier.
                if (command == RespCommand.GEORADIUSBYMEMBER || command == RespCommand.GEORADIUSBYMEMBER_RO)
                {
                    // From Member
                    searchOpts.fromMember = input.parseState.GetArgSliceByRef(currTokenIdx++).SpanByte.ToByteArray();
                    searchOpts.origin = GeoOriginType.FromMember;
                }
                else
                {
                    if (!input.parseState.TryGetDouble(currTokenIdx++, out searchOpts.lon) ||
                        !input.parseState.TryGetDouble(currTokenIdx++, out searchOpts.lat))
                    {
                        return CmdStrings.RESP_ERR_NOT_VALID_FLOAT;
                    }

                    if ((searchOpts.lon < GeoHash.LongitudeMin) ||
                        (searchOpts.lat < GeoHash.LatitudeMin) ||
                        (searchOpts.lon > GeoHash.LongitudeMax) ||
                        (searchOpts.lat > GeoHash.LatitudeMax))
                    {
                        errorMessage = string.Format(CmdStrings.GenericErrLonLat,
                                                     searchOpts.lon, searchOpts.lat);
                        return Encoding.ASCII.GetBytes(errorMessage);
                    }

                    searchOpts.origin = GeoOriginType.FromLonLat;
                }

                // Radius
                if (!input.parseState.TryGetDouble(currTokenIdx++, out searchOpts.radius))
                {
                    return CmdStrings.RESP_ERR_NOT_VALID_RADIUS;
                }

                if (searchOpts.radius < 0)
                {
                    return CmdStrings.RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE;
                }

                searchOpts.searchType = GeoSearchType.ByRadius;
                if (!input.parseState.TryGetGeoDistanceUnit(currTokenIdx++, out searchOpts.unit))
                {
                    return CmdStrings.RESP_ERR_NOT_VALID_GEO_DISTANCE_UNIT;
                }
            }

            // Read the options
            while (currTokenIdx < input.parseState.Count)
            {
                // Read token
                var tokenBytes = input.parseState.GetArgSliceByRef(currTokenIdx++).ReadOnlySpan;

                if (command == RespCommand.GEOSEARCH || command == RespCommand.GEOSEARCHSTORE)
                {
                    if (tokenBytes.EqualsUpperCaseSpanIgnoringCase(CmdStrings.FROMMEMBER))
                    {
                        if (searchOpts.origin != GeoOriginType.Undefined)
                        {
                            return CmdStrings.RESP_SYNTAX_ERROR;
                        }

                        if (input.parseState.Count == currTokenIdx)
                        {
                            argNumError = true;
                            break;
                        }

                        searchOpts.fromMember = input.parseState.GetArgSliceByRef(currTokenIdx++).SpanByte.ToByteArray();
                        searchOpts.origin = GeoOriginType.FromMember;
                        continue;
                    }

                    if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("FROMLONLAT"u8))
                    {
                        if (searchOpts.origin != GeoOriginType.Undefined)
                        {
                            return CmdStrings.RESP_SYNTAX_ERROR;
                        }

                        if (input.parseState.Count - currTokenIdx < 2)
                        {
                            argNumError = true;
                            break;
                        }

                        // Read coordinates
                        if (!input.parseState.TryGetDouble(currTokenIdx++, out searchOpts.lon) ||
                            !input.parseState.TryGetDouble(currTokenIdx++, out searchOpts.lat)
                           )
                        {
                            return CmdStrings.RESP_ERR_NOT_VALID_FLOAT;
                        }

                        if ((searchOpts.lon < GeoHash.LongitudeMin) ||
                            (searchOpts.lat < GeoHash.LatitudeMin) ||
                            (searchOpts.lon > GeoHash.LongitudeMax) ||
                            (searchOpts.lat > GeoHash.LatitudeMax))
                        {
                            errorMessage = string.Format(CmdStrings.GenericErrLonLat,
                                                         searchOpts.lon, searchOpts.lat);
                            break;
                        }

                        searchOpts.origin = GeoOriginType.FromLonLat;
                        continue;
                    }

                    if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("BYRADIUS"u8))
                    {
                        if (searchOpts.searchType != GeoSearchType.Undefined)
                        {
                            return CmdStrings.RESP_SYNTAX_ERROR;
                        }

                        if (input.parseState.Count - currTokenIdx < 2)
                        {
                            argNumError = true;
                            break;
                        }

                        // Read radius and units
                        if (!input.parseState.TryGetDouble(currTokenIdx++, out searchOpts.radius))
                        {
                            return CmdStrings.RESP_ERR_NOT_VALID_RADIUS;
                        }

                        if (searchOpts.radius < 0)
                        {
                            return CmdStrings.RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE;
                        }

                        searchOpts.searchType = GeoSearchType.ByRadius;
                        if (!input.parseState.TryGetGeoDistanceUnit(currTokenIdx++, out searchOpts.unit))
                        {
                            return CmdStrings.RESP_ERR_NOT_VALID_GEO_DISTANCE_UNIT;
                        }
                        continue;
                    }

                    if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("BYBOX"u8))
                    {
                        if (searchOpts.searchType != GeoSearchType.Undefined)
                        {
                            return  CmdStrings.RESP_SYNTAX_ERROR;
                        }
                        searchOpts.searchType = GeoSearchType.ByBox;

                        if (input.parseState.Count - currTokenIdx < 3)
                        {
                            argNumError = true;
                            break;
                        }

                        // Read width, height
                        if (!input.parseState.TryGetDouble(currTokenIdx++, out searchOpts.boxWidth))
                        {
                            return CmdStrings.RESP_ERR_NOT_VALID_WIDTH;
                        }

                        if (!input.parseState.TryGetDouble(currTokenIdx++, out var height))
                        {
                            return CmdStrings.RESP_ERR_NOT_VALID_HEIGHT;
                        }

                        searchOpts.boxHeight = height;

                        if (searchOpts.boxWidth < 0 || searchOpts.boxHeight < 0)
                        {
                            return CmdStrings.RESP_ERR_HEIGHT_OR_WIDTH_NEGATIVE;
                        }

                        // Read units
                        if (!input.parseState.TryGetGeoDistanceUnit(currTokenIdx++, out searchOpts.unit))
                        {
                            return CmdStrings.RESP_ERR_NOT_VALID_GEO_DISTANCE_UNIT;
                        }
                        continue;
                    }
                }

                if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("ASC"u8))
                {
                    searchOpts.sort = GeoOrder.Ascending;
                    continue;
                }

                if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("DESC"u8))
                {
                    searchOpts.sort = GeoOrder.Descending;
                    continue;
                }

                if (tokenBytes.EqualsUpperCaseSpanIgnoringCase(CmdStrings.COUNT))
                {
                    if (input.parseState.Count == currTokenIdx)
                    {
                        argNumError = true;
                        break;
                    }

                    if (!input.parseState.TryGetInt(currTokenIdx++, out var countValue))
                    {
                        return CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER;
                    }

                    if (countValue <= 0)
                    {
                        return CmdStrings.RESP_ERR_COUNT_IS_NOT_POSITIVE;
                    }

                    searchOpts.countValue = countValue;

                    if (input.parseState.Count > currTokenIdx)
                    {
                        var peekArg = input.parseState.GetArgSliceByRef(currTokenIdx).ReadOnlySpan;
                        if (peekArg.EqualsUpperCaseSpanIgnoringCase("ANY"u8))
                        {
                            searchOpts.withCountAny = true;
                            currTokenIdx++;
                            continue;
                        }
                    }

                    continue;
                }

                if (!readOnly)
                {
                    if ((command == RespCommand.GEORADIUS || command == RespCommand.GEORADIUSBYMEMBER)
                        && tokenBytes.EqualsUpperCaseSpanIgnoringCase(CmdStrings.STORE))
                    {
                        if (input.parseState.Count == currTokenIdx)
                        {
                            argNumError = true;
                            break;
                        }

                        destIdx = ++currTokenIdx;
                        continue;
                    }

                    if (tokenBytes.EqualsUpperCaseSpanIgnoringCase(CmdStrings.STOREDIST))
                    {
                        if ((command == RespCommand.GEORADIUS || command == RespCommand.GEORADIUSBYMEMBER))
                        {
                            if (input.parseState.Count == currTokenIdx)
                            {
                                argNumError = true;
                                break;
                            }

                            destIdx = ++currTokenIdx;
                        }

                        storeDist = true;
                        continue;
                    }
                }

                if (tokenBytes.EqualsUpperCaseSpanIgnoringCase(CmdStrings.WITHCOORD))
                {
                    searchOpts.withCoord = true;
                    continue;
                }

                if (tokenBytes.EqualsUpperCaseSpanIgnoringCase(CmdStrings.WITHDIST))
                {
                    searchOpts.withDist = true;
                    continue;
                }

                if (tokenBytes.EqualsUpperCaseSpanIgnoringCase(CmdStrings.WITHHASH))
                {
                    searchOpts.withHash = true;
                    continue;
                }

                return CmdStrings.RESP_SYNTAX_ERROR;
            }

            // Check that we have the mandatory options
            if ((errorMessage == default) && ((searchOpts.origin == 0) || (searchOpts.searchType == 0)))
                argNumError = true;

            // Check if we have a wrong number of arguments
            if (argNumError)
            {
                errorMessage = string.Format(CmdStrings.GenericErrWrongNumArgs, command.ToString());
            }

            if (destIdx != -1)
            {
                if ((searchOpts.withDist || searchOpts.withCoord || searchOpts.withHash))
                {
                    errorMessage = string.Format(CmdStrings.GenericErrStoreCommand, command.ToString());
                }
                searchOpts.withDist = storeDist;
            }
            else
            {
                readOnly = true;
            }

            // Check if we encountered an error while checking the parse state
            if (errorMessage != default)
            {
                return Encoding.ASCII.GetBytes(errorMessage);
            }

            // On storing to ZSET, we need to use either dist or hash as score.
            if (!readOnly && !searchOpts.withDist && !searchOpts.withHash)
            {
                searchOpts.withHash = true;
            }

            return default;
        }
    }
}