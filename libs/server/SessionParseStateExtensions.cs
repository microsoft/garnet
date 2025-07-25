﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Text;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// Extension methods for <see cref="SessionParseState"/>.
    /// </summary>
    public static class SessionParseStateExtensions
    {
        /// <summary>
        /// Parse info metrics type from parse state at specified index
        /// </summary>
        /// <param name="parseState">The parse state</param>
        /// <param name="idx">The argument index</param>
        /// <param name="value">Parsed value</param>
        /// <returns>True if value parsed successfully</returns>
        public static bool TryGetInfoMetricsType(this SessionParseState parseState, int idx, out InfoMetricsType value)
        {
            value = default;
            var sbArg = parseState.GetArgSliceByRef(idx).ReadOnlySpan;

            if (sbArg.EqualsUpperCaseSpanIgnoringCase("SERVER"u8))
                value = InfoMetricsType.SERVER;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("MEMORY"u8))
                value = InfoMetricsType.MEMORY;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("CLUSTER"u8))
                value = InfoMetricsType.CLUSTER;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("REPLICATION"u8))
                value = InfoMetricsType.REPLICATION;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("STATS"u8))
                value = InfoMetricsType.STATS;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("STORE"u8))
                value = InfoMetricsType.STORE;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("OBJECTSTORE"u8))
                value = InfoMetricsType.OBJECTSTORE;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("STOREHASHTABLE"u8))
                value = InfoMetricsType.STOREHASHTABLE;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("OBJECTSTOREHASHTABLE"u8))
                value = InfoMetricsType.OBJECTSTOREHASHTABLE;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("STOREREVIV"u8))
                value = InfoMetricsType.STOREREVIV;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("OBJECTSTOREREVIV"u8))
                value = InfoMetricsType.OBJECTSTOREREVIV;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("PERSISTENCE"u8))
                value = InfoMetricsType.PERSISTENCE;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("CLIENTS"u8))
                value = InfoMetricsType.CLIENTS;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("KEYSPACE"u8))
                value = InfoMetricsType.KEYSPACE;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("MODULES"u8))
                value = InfoMetricsType.MODULES;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("BPSTATS"u8))
                value = InfoMetricsType.BPSTATS;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("CINFO"u8))
                value = InfoMetricsType.CINFO;
            else return false;

            return true;
        }

        /// <summary>
        /// Parse latency metrics type from parse state at specified index
        /// </summary>
        /// <param name="parseState">The parse state</param>
        /// <param name="idx">The argument index</param>
        /// <param name="value">Parsed value</param>
        /// <returns>True if value parsed successfully</returns>
        public static bool TryGetLatencyMetricsType(this SessionParseState parseState, int idx, out LatencyMetricsType value)
        {
            value = default;
            var sbArg = parseState.GetArgSliceByRef(idx).ReadOnlySpan;

            if (sbArg.EqualsUpperCaseSpanIgnoringCase("NET_RS_LAT"u8))
                value = LatencyMetricsType.NET_RS_LAT;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("PENDING_LAT"u8))
                value = LatencyMetricsType.PENDING_LAT;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("TX_PROC_LAT"u8))
                value = LatencyMetricsType.TX_PROC_LAT;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("NET_RS_BYTES"u8))
                value = LatencyMetricsType.NET_RS_BYTES;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("NET_RS_OPS"u8))
                value = LatencyMetricsType.NET_RS_OPS;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("NET_RS_LAT_ADMIN"u8))
                value = LatencyMetricsType.NET_RS_LAT_ADMIN;
            else return false;

            return true;
        }

        /// <summary>
        /// Parse client type from parse state at specified index
        /// </summary>
        /// <param name="parseState">The parse state</param>
        /// <param name="idx">The argument index</param>
        /// <param name="value">Parsed value</param>
        /// <returns>True if value parsed successfully</returns>
        public static bool TryGetClientType(this SessionParseState parseState, int idx, out ClientType value)
        {
            value = ClientType.Invalid;
            var sbArg = parseState.GetArgSliceByRef(idx).ReadOnlySpan;

            if (sbArg.EqualsUpperCaseSpanIgnoringCase("NORMAL"u8))
                value = ClientType.NORMAL;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("MASTER"u8))
                value = ClientType.MASTER;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("REPLICA"u8))
                value = ClientType.REPLICA;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("PUBSUB"u8))
                value = ClientType.PUBSUB;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("SLAVE"u8))
                value = ClientType.SLAVE;

            return value != ClientType.Invalid;
        }

        /// <summary>
        /// Parse bit field overflow from parse state at specified index
        /// </summary>
        /// <param name="parseState">The parse state</param>
        /// <param name="idx">The argument index</param>
        /// <param name="value">Parsed value</param>
        /// <returns>True if value parsed successfully</returns>
        internal static bool TryGetBitFieldOverflow(this SessionParseState parseState, int idx, out BitFieldOverflow value)
        {
            value = default;
            var sbArg = parseState.GetArgSliceByRef(idx).ReadOnlySpan;

            if (sbArg.EqualsUpperCaseSpanIgnoringCase("WRAP"u8))
                value = BitFieldOverflow.WRAP;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("SAT"u8))
                value = BitFieldOverflow.SAT;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("FAIL"u8))
                value = BitFieldOverflow.FAIL;
            else return false;

            return true;
        }

        /// <summary>
        /// Parse bit field ENCODING slice from parse state at specified index
        /// </summary>
        /// <param name="parseState">The parse state</param>
        /// <param name="idx">The argument index</param>
        /// <param name="bitCount">parsed bitcount</param>
        /// <param name="isSigned">bitfield signtype</param>
        /// <returns></returns>
        internal static unsafe bool TryGetBitfieldEncoding(this SessionParseState parseState, int idx, out long bitCount, out bool isSigned)
        {
            bitCount = default;
            isSigned = default;
            var encodingSlice = parseState.GetArgSliceByRef(idx);

            if (encodingSlice.length <= 1)
            {
                return false;
            }

            var ptr = encodingSlice.ptr + 1;

            isSigned = *encodingSlice.ptr == 'i';

            if (!isSigned && *encodingSlice.ptr != 'u')
            {
                return false;
            }

            return
                RespReadUtils.TryReadInt64Safe(ref ptr, encodingSlice.ptr + encodingSlice.length,
                                           out bitCount, out var bytesRead,
                                           out _, out _, allowLeadingZeros: false) &&
                ((int)bytesRead == encodingSlice.length - 1) && (bytesRead > 0L) &&
                (bitCount > 0) &&
                ((isSigned && bitCount <= 64) ||
                 (!isSigned && bitCount < 64));
        }

        /// <summary>
        /// Parse bit field OFFSET slice from parse state at specified index
        /// </summary>
        /// <param name="parseState">The parse state</param>
        /// <param name="idx">The argument index</param>
        /// <param name="bitFieldOffset">parsed value</param>
        /// <param name="multiplyOffset">should value by multiplied by bitcount</param>
        /// <returns></returns>
        internal static unsafe bool TryGetBitfieldOffset(this SessionParseState parseState, int idx, out long bitFieldOffset, out bool multiplyOffset)
        {
            bitFieldOffset = default;
            multiplyOffset = default;
            var offsetSlice = parseState.GetArgSliceByRef(idx);

            if (offsetSlice.Length <= 0)
            {
                return false;
            }

            var ptr = offsetSlice.ptr;
            var len = offsetSlice.length;

            if (*ptr == '#')
            {
                if (offsetSlice.length == 1)
                    return false;

                multiplyOffset = true;
                ptr++;
                len--;
            }

            return
                RespReadUtils.TryReadInt64Safe(ref ptr, offsetSlice.ptr + offsetSlice.length,
                                           out bitFieldOffset, out var bytesRead,
                                           out _, out _, allowLeadingZeros: false) &&
                ((int)bytesRead == len) && (bytesRead > 0L) &&
                (bitFieldOffset >= 0);
        }

        /// <summary>
        /// Parse GEOSEARCH commands options from parse state based on command
        /// </summary>
        /// <param name="parseState"></param>
        /// <param name="command"></param>
        /// <param name="searchOpts"></param>
        /// <param name="destIdx"></param>
        /// <param name="error"></param>
        /// <returns></returns>
        public static bool TryGetGeoSearchOptions(this SessionParseState parseState,
                                                  RespCommand command,
                                                  out GeoSearchOptions searchOpts,
                                                  out int destIdx,
                                                  out ReadOnlySpan<byte> error)
        {
            error = default;
            searchOpts = default;
            destIdx = command == RespCommand.GEOSEARCHSTORE ? 0 : -1;

            bool readOnly = command == RespCommand.GEOSEARCH ||
                            command == RespCommand.GEORADIUS_RO ||
                            command == RespCommand.GEORADIUSBYMEMBER_RO;
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
                    searchOpts.fromMember = parseState.GetArgSliceByRef(currTokenIdx++).SpanByte.ToByteArray();
                    searchOpts.origin = GeoOriginType.FromMember;
                }
                else
                {
                    if (!parseState.TryGetGeoLonLat(currTokenIdx, out searchOpts.lon, out searchOpts.lat, out error))
                    {
                        return false;
                    }

                    currTokenIdx += 2;
                    searchOpts.origin = GeoOriginType.FromLonLat;
                }

                // Radius
                if (!parseState.TryGetDouble(currTokenIdx++, out searchOpts.radius))
                {
                    error = CmdStrings.RESP_ERR_NOT_VALID_RADIUS;
                    return false;
                }

                if (searchOpts.radius < 0)
                {
                    error = CmdStrings.RESP_ERR_RADIUS_IS_NEGATIVE;
                    return false;
                }

                searchOpts.searchType = GeoSearchType.ByRadius;
                if (!parseState.TryGetGeoDistanceUnit(currTokenIdx++, out searchOpts.unit))
                {
                    error = CmdStrings.RESP_ERR_NOT_VALID_GEO_DISTANCE_UNIT;
                    return false;
                }
            }

            // Read the options
            while (currTokenIdx < parseState.Count)
            {
                // Read token
                var tokenBytes = parseState.GetArgSliceByRef(currTokenIdx++).ReadOnlySpan;

                if (command == RespCommand.GEOSEARCH || command == RespCommand.GEOSEARCHSTORE)
                {
                    if (tokenBytes.EqualsUpperCaseSpanIgnoringCase(CmdStrings.FROMMEMBER))
                    {
                        if (searchOpts.origin != GeoOriginType.Undefined)
                        {
                            error = CmdStrings.RESP_SYNTAX_ERROR;
                            return false;
                        }

                        if (parseState.Count == currTokenIdx)
                        {
                            argNumError = true;
                            break;
                        }

                        searchOpts.fromMember = parseState.GetArgSliceByRef(currTokenIdx++).SpanByte.ToByteArray();
                        searchOpts.origin = GeoOriginType.FromMember;
                        continue;
                    }

                    if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("FROMLONLAT"u8))
                    {
                        if (searchOpts.origin != GeoOriginType.Undefined)
                        {
                            error = CmdStrings.RESP_SYNTAX_ERROR;
                            return false;
                        }

                        if (parseState.Count - currTokenIdx < 2)
                        {
                            argNumError = true;
                            break;
                        }

                        // Read coordinates
                        if (!parseState.TryGetGeoLonLat(currTokenIdx, out searchOpts.lon, out searchOpts.lat, out error))
                        {
                            return false;
                        }

                        currTokenIdx += 2;
                        searchOpts.origin = GeoOriginType.FromLonLat;
                        continue;
                    }

                    if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("BYRADIUS"u8))
                    {
                        if (searchOpts.searchType != GeoSearchType.Undefined)
                        {
                            error = CmdStrings.RESP_SYNTAX_ERROR;
                            return false;
                        }

                        if (parseState.Count - currTokenIdx < 2)
                        {
                            argNumError = true;
                            break;
                        }

                        // Read radius and units
                        if (!parseState.TryGetDouble(currTokenIdx++, out searchOpts.radius))
                        {
                            error = CmdStrings.RESP_ERR_NOT_VALID_RADIUS;
                            return false;
                        }

                        if (searchOpts.radius < 0)
                        {
                            error = CmdStrings.RESP_ERR_RADIUS_IS_NEGATIVE;
                            return false;
                        }

                        searchOpts.searchType = GeoSearchType.ByRadius;
                        if (!parseState.TryGetGeoDistanceUnit(currTokenIdx++, out searchOpts.unit))
                        {
                            error = CmdStrings.RESP_ERR_NOT_VALID_GEO_DISTANCE_UNIT;
                            return false;
                        }
                        continue;
                    }

                    if (tokenBytes.EqualsUpperCaseSpanIgnoringCase("BYBOX"u8))
                    {
                        if (searchOpts.searchType != GeoSearchType.Undefined)
                        {
                            error = CmdStrings.RESP_SYNTAX_ERROR;
                            return false;
                        }
                        searchOpts.searchType = GeoSearchType.ByBox;

                        if (parseState.Count - currTokenIdx < 3)
                        {
                            argNumError = true;
                            break;
                        }

                        // Read width, height
                        if (!parseState.TryGetDouble(currTokenIdx++, out searchOpts.boxWidth))
                        {
                            error = CmdStrings.RESP_ERR_NOT_VALID_WIDTH;
                            return false;
                        }

                        if (!parseState.TryGetDouble(currTokenIdx++, out var height))
                        {
                            error = CmdStrings.RESP_ERR_NOT_VALID_HEIGHT;
                            return false;
                        }

                        searchOpts.boxHeight = height;

                        if (searchOpts.boxWidth < 0 || searchOpts.boxHeight < 0)
                        {
                            error = CmdStrings.RESP_ERR_HEIGHT_OR_WIDTH_NEGATIVE;
                            return false;
                        }

                        // Read units
                        if (!parseState.TryGetGeoDistanceUnit(currTokenIdx++, out searchOpts.unit))
                        {
                            error = CmdStrings.RESP_ERR_NOT_VALID_GEO_DISTANCE_UNIT;
                            return false;
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
                    if (parseState.Count == currTokenIdx)
                    {
                        argNumError = true;
                        break;
                    }

                    if (!parseState.TryGetInt(currTokenIdx++, out var countValue))
                    {
                        error = CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER;
                        return false;
                    }

                    if (countValue <= 0)
                    {
                        error = CmdStrings.RESP_ERR_COUNT_IS_NOT_POSITIVE;
                        return false;
                    }

                    searchOpts.countValue = countValue;

                    if (parseState.Count > currTokenIdx)
                    {
                        var peekArg = parseState.GetArgSliceByRef(currTokenIdx).ReadOnlySpan;
                        if (peekArg.EqualsUpperCaseSpanIgnoringCase("ANY"u8))
                        {
                            searchOpts.withCountAny = true;
                            currTokenIdx++;
                            continue;
                        }
                    }

                    continue;
                }

                if (command != RespCommand.GEOSEARCH && command != RespCommand.GEORADIUS_RO &&
                    command != RespCommand.GEORADIUSBYMEMBER_RO)
                {
                    if ((command == RespCommand.GEORADIUS || command == RespCommand.GEORADIUSBYMEMBER)
                        && tokenBytes.EqualsUpperCaseSpanIgnoringCase(CmdStrings.STORE))
                    {
                        if (parseState.Count == currTokenIdx)
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
                            if (parseState.Count == currTokenIdx)
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

                error = CmdStrings.RESP_SYNTAX_ERROR;
                return false;
            }

            // Check that we have the mandatory options
            if ((searchOpts.origin == 0) || (searchOpts.searchType == 0))
                argNumError = true;

            // Check if we have a wrong number of arguments
            if (argNumError)
            {
                error = Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrWrongNumArgs, command.ToString()));
                return false;
            }

            if (destIdx != -1)
            {
                if (searchOpts.withDist || searchOpts.withCoord || searchOpts.withHash)
                {
                    error = Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrStoreCommand, command.ToString()));
                    return false;
                }
                searchOpts.withDist = storeDist;

                // On storing to ZSET, we need to use either dist or hash as score.
                if (!searchOpts.withDist && !searchOpts.withHash)
                {
                    searchOpts.withHash = true;
                }
            }

            return true;
        }


        /// <summary>
        /// Parse manager type from parse state at specified index
        /// </summary>
        /// <param name="parseState">The parse state</param>
        /// <param name="idx">The argument index</param>
        /// <param name="value">Parsed value</param>
        /// <returns>True if value parsed successfully</returns>
        internal static bool TryGetManagerType(this SessionParseState parseState, int idx, out ManagerType value)
        {
            value = default;
            var sbArg = parseState.GetArgSliceByRef(idx).ReadOnlySpan;

            if (sbArg.EqualsUpperCaseSpanIgnoringCase("MIGRATIONMANAGER"u8))
                value = ManagerType.MigrationManager;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("REPLICATIONMANAGER"u8))
                value = ManagerType.ReplicationManager;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("SERVERLISTENER"u8))
                value = ManagerType.ServerListener;
            else return false;

            return true;
        }

        /// <summary>
        /// Parse sorted set add option from parse state at specified index
        /// </summary>
        /// <param name="parseState">The parse state</param>
        /// <param name="idx">The argument index</param>
        /// <param name="value">Parsed value</param>
        /// <returns>True if value parsed successfully</returns>
        internal static bool TryGetSortedSetAddOption(this SessionParseState parseState, int idx, out SortedSetAddOption value)
        {
            value = SortedSetAddOption.None;
            var sbArg = parseState.GetArgSliceByRef(idx).ReadOnlySpan;

            if (sbArg.EqualsUpperCaseSpanIgnoringCase("XX"u8))
                value = SortedSetAddOption.XX;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("NX"u8))
                value = SortedSetAddOption.NX;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("LT"u8))
                value = SortedSetAddOption.LT;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("GT"u8))
                value = SortedSetAddOption.GT;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("CH"u8))
                value = SortedSetAddOption.CH;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("INCR"u8))
                value = SortedSetAddOption.INCR;
            else return false;

            return true;
        }

        /// <summary>
        /// Parse expire option from parse state at specified index
        /// </summary>
        /// <param name="parseState">The parse state</param>
        /// <param name="idx">The argument index</param>
        /// <param name="value">Parsed value</param>
        /// <returns>True if value parsed successfully</returns>
        internal static bool TryGetExpireOption(this SessionParseState parseState, int idx, out ExpireOption value)
        {
            value = ExpireOption.None;
            var sbArg = parseState.GetArgSliceByRef(idx).ReadOnlySpan;

            if (sbArg.EqualsUpperCaseSpanIgnoringCase("NX"u8))
                value = ExpireOption.NX;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("XX"u8))
                value = ExpireOption.XX;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("GT"u8))
                value = ExpireOption.GT;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("LT"u8))
                value = ExpireOption.LT;
            else return false;

            return true;
        }

        /// <summary>
        /// Parse sorted set aggregate type from parse state at specified index
        /// </summary>
        /// <param name="parseState">The parse state</param>
        /// <param name="idx">The argument index</param>
        /// <param name="value">Parsed value</param>
        /// <returns>True if value parsed successfully</returns>
        internal static bool TryGetSortedSetAggregateType(this SessionParseState parseState, int idx, out SortedSetAggregateType value)
        {
            value = default;
            var sbArg = parseState.GetArgSliceByRef(idx).ReadOnlySpan;

            if (sbArg.EqualsUpperCaseSpanIgnoringCase(CmdStrings.SUM))
                value = SortedSetAggregateType.Sum;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase(CmdStrings.MIN))
                value = SortedSetAggregateType.Min;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase(CmdStrings.MAX))
                value = SortedSetAggregateType.Max;
            else return false;

            return true;
        }

        /// <summary>
        /// Given the parseState and an index, potentially get the expiration option at that index.
        /// </summary>
        /// <param name="parseState">The parse state</param>
        /// <param name="idx">The argument index</param>
        /// <param name="value">Parsed expiration option value</param>
        /// <returns>If the argument at that index is a valid expiration option return true, else return false</returns>
        internal static bool TryGetExpirationOption(this SessionParseState parseState, int idx, out ExpirationOption value)
        {
            var sbArg = parseState.GetArgSliceByRef(idx).Span;
            return parseState.TryGetExpirationOptionWithToken(ref sbArg, out value);
        }

        /// <summary>
        /// Given the parse state and a token, potentially get the expiration option represented by the token.
        /// </summary>
        /// <param name="parseState">The parse state (used only to provide the dot notation for this method)</param>
        /// <param name="token">The token to parse</param>
        /// <param name="value">Parsed expiration option value</param>
        /// <returns>If the token is a valid expiration option return true, else false</returns>
        internal static bool TryGetExpirationOptionWithToken(this SessionParseState parseState, ref Span<byte> token, out ExpirationOption value)
        {
            value = default;
            if (token.EqualsUpperCaseSpanIgnoringCase(CmdStrings.EX))
                value = ExpirationOption.EX;
            else if (token.EqualsUpperCaseSpanIgnoringCase(CmdStrings.PX))
                value = ExpirationOption.PX;
            else if (token.EqualsUpperCaseSpanIgnoringCase(CmdStrings.EXAT))
                value = ExpirationOption.EXAT;
            else if (token.EqualsUpperCaseSpanIgnoringCase(CmdStrings.PXAT))
                value = ExpirationOption.PXAT;
            else if (token.EqualsUpperCaseSpanIgnoringCase(CmdStrings.KEEPTTL))
                value = ExpirationOption.KEEPTTL;
            else
                return false;

            return true;
        }

        /// <summary>
        /// Parse geo distance unit from parse state at specified index
        /// </summary>
        /// <param name="parseState">The parse state</param>
        /// <param name="idx">The argument index</param>
        /// <param name="value">Parsed value</param>
        /// <returns>True if value parsed successfully</returns>
        internal static bool TryGetGeoDistanceUnit(this SessionParseState parseState, int idx, out GeoDistanceUnitType value)
        {
            value = default;
            var sbArg = parseState.GetArgSliceByRef(idx).ReadOnlySpan;

            if (sbArg.EqualsUpperCaseSpanIgnoringCase("M"u8))
                value = GeoDistanceUnitType.M;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("KM"u8))
                value = GeoDistanceUnitType.KM;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("MI"u8))
                value = GeoDistanceUnitType.MI;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("FT"u8))
                value = GeoDistanceUnitType.FT;
            else
                return false;

            return true;
        }

        /// <summary>
        /// Parse geo longitude and latitude from parse state at specified index.
        /// </summary>
        /// <param name="parseState">The parse state</param>
        /// <param name="idx">The first argument index</param>
        /// <param name="lon">Longitude</param>
        /// <param name="lat">Latitude</param>
        /// <param name="error">Error if failed</param>
        /// <returns>True if value parsed successfully</returns>
        internal static bool TryGetGeoLonLat(this SessionParseState parseState, int idx, out double lon, out double lat,
                                             out ReadOnlySpan<byte> error)
        {
            error = default;
            lat = default;

            if (!parseState.TryGetDouble(idx++, out lon) ||
                !parseState.TryGetDouble(idx, out lat))
            {
                error = CmdStrings.RESP_ERR_NOT_VALID_FLOAT;
                return false;
            }

            if ((lon < GeoHash.LongitudeMin) ||
                (lat < GeoHash.LatitudeMin) ||
                (lon > GeoHash.LongitudeMax) ||
                (lat > GeoHash.LatitudeMax))
            {
                error = Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrLonLat, lon, lat));
                return false;
            }

            return true;
        }

        /// <summary>
        /// Parse timeout (in seconds) from parse state at specified index.
        /// </summary>
        /// <param name="parseState">The parse state</param>
        /// <param name="idx">The argument index</param>
        /// <param name="timeout">Timeout</param>
        /// <param name="error">Error if failed</param>
        /// <returns>True if value parsed successfully</returns>
        internal static bool TryGetTimeout(this SessionParseState parseState, int idx, out double timeout, out ReadOnlySpan<byte> error)
        {
            // .NET APIs do not support an higher value than int.MaxValue milliseconds.
            const double MAXTIMEOUT = int.MaxValue / 1000D;

            error = default;

            if (!parseState.TryGetDouble(idx, out timeout))
            {
                error = CmdStrings.RESP_ERR_TIMEOUT_NOT_VALID_FLOAT;
                return false;
            }

            if (timeout < 0)
            {
                error = CmdStrings.RESP_ERR_TIMEOUT_IS_NEGATIVE;
                return false;
            }

            if (timeout > MAXTIMEOUT)
            {
                error = CmdStrings.RESP_ERR_TIMEOUT_IS_OUT_OF_RANGE;
                return false;
            }

            return true;
        }


        /// <summary>
        /// Tries to extract keys from the key specifications in the given RespCommandsInfo.
        /// </summary>
        /// <param name="state">The SessionParseState instance.</param>
        /// <param name="keySpecs">The RespCommandKeySpecification array contains the key specification</param>
        /// <param name="keys">The list to store extracted keys.</param>
        /// <returns>True if keys were successfully extracted, otherwise false.</returns>
        internal static bool TryExtractKeysFromSpecs(this ref SessionParseState state, RespCommandKeySpecification[] keySpecs, out List<ArgSlice> keys)
        {
            keys = new();

            foreach (var spec in keySpecs)
            {
                if (!ExtractKeysFromSpec(ref state, keys, spec))
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Tries to extract keys and their associated flags from the key specifications in the given RespCommandsInfo.
        /// </summary>
        /// <param name="state">The SessionParseState instance.</param>
        /// <param name="keySpecs">The RespCommandKeySpecification array containing the key specifications.</param>
        /// <param name="keys">The list to store extracted keys.</param>
        /// <param name="flags">The list to store associated flags for each key.</param>
        /// <returns>True if keys and flags were successfully extracted, otherwise false.</returns>
        internal static bool TryExtractKeysAndFlagsFromSpecs(this ref SessionParseState state, RespCommandKeySpecification[] keySpecs, out List<ArgSlice> keys, out List<string[]> flags)
        {
            keys = new();
            flags = new();

            foreach (var spec in keySpecs)
            {
                var prevKeyCount = keys.Count;
                if (!ExtractKeysFromSpec(ref state, keys, spec))
                {
                    return false;
                }

                var keyFlags = spec.RespFormatFlags;
                for (int i = prevKeyCount; i < keys.Count; i++)
                {
                    flags.Add(keyFlags);
                }
            }

            return true;
        }

        /// <summary>
        /// Extracts keys from the given key specification in the provided SessionParseState.
        /// </summary>
        /// <param name="state">The SessionParseState instance.</param>
        /// <param name="keys">The list to store extracted keys.</param>
        /// <param name="spec">The key specification to use for extraction.</param>
        /// <returns>True if keys were successfully extracted, otherwise false.</returns>
        private static bool ExtractKeysFromSpec(ref SessionParseState state, List<ArgSlice> keys, RespCommandKeySpecification spec)
        {
            int startIndex = 0;

            if (spec.BeginSearch is BeginSearchKeySpecMethodBase bsKeyword)
            {
                if (!bsKeyword.TryGetStartIndex(ref state, out startIndex))
                {
                    return false;
                }
            }

            if (startIndex < 0 || startIndex >= state.Count)
                return false;

            if (spec.FindKeys is FindKeysKeySpecMethodBase findKey)
            {
                findKey.ExtractKeys(ref state, startIndex, keys);
            }

            return true;
        }
    }
}