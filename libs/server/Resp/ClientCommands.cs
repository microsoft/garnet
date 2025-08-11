// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Server session for RESP protocol - client commands are in this file
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// CLIENT LIST
        /// </summary>
        private bool NetworkCLIENTLIST()
        {
            if (Server is GarnetServerBase garnetServer)
            {
                IEnumerable<RespServerSession> toInclude;
                RespServerSession[] rentedBuffer = null;

                try
                {
                    if (parseState.Count == 0)
                    {
                        toInclude = garnetServer.ActiveConsumers().OfType<RespServerSession>();
                    }
                    else if (parseState.Count < 2)
                    {
                        return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                    }
                    else
                    {
                        ref var filter = ref parseState.GetArgSliceByRef(0);
                        AsciiUtils.ToUpperInPlace(filter.Span);

                        if (filter.Span.SequenceEqual(CmdStrings.TYPE))
                        {
                            if (parseState.Count != 2)
                            {
                                return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                            }

                            if (!parseState.TryGetClientType(1, out var clientType) ||
                                clientType == ClientType.SLAVE) // SLAVE is not legal as CLIENT|LIST was introduced after the SLAVE -> REPLICA rename
                            {
                                var type = parseState.GetString(1);
                                return AbortWithErrorMessage(Encoding.UTF8.GetBytes(string.Format(CmdStrings.GenericUnknownClientType, type)));
                            }

                            toInclude =
                                garnetServer
                                    .ActiveConsumers()
                                    .OfType<RespServerSession>()
                                    .Where(
                                        r =>
                                        {
                                            ClientType effectiveType;
                                            if (storeWrapper.clusterProvider is not null && r.clusterSession.RemoteNodeId is not null)
                                            {
                                                if (storeWrapper.clusterProvider.IsReplica(r.clusterSession.RemoteNodeId))
                                                {
                                                    effectiveType = ClientType.REPLICA;
                                                }
                                                else
                                                {
                                                    effectiveType = ClientType.MASTER;
                                                }
                                            }
                                            else
                                            {
                                                effectiveType = r.isSubscriptionSession ? ClientType.PUBSUB : ClientType.NORMAL;
                                            }

                                            return effectiveType == clientType;
                                        }
                                    );
                        }
                        else if (filter.Span.SequenceEqual(CmdStrings.ID))
                        {
                            // Try and put all the ids onto the stack, if the count is small
                            var numIds = parseState.Count - 1;

                            Span<long> ids = stackalloc long[32];
                            long[] rentedIds;

                            if (numIds <= ids.Length)
                            {
                                ids = ids[..numIds];
                                rentedIds = null;
                            }
                            else
                            {
                                rentedIds = ArrayPool<long>.Shared.Rent(numIds);
                                ids = rentedIds[..numIds];
                            }

                            try
                            {
                                for (var idIx = 1; idIx < parseState.Count; idIx++)
                                {
                                    if (!parseState.TryGetLong(idIx, out var id))
                                    {
                                        return AbortWithErrorMessage(CmdStrings.RESP_ERR_INVALID_CLIENT_ID);
                                    }

                                    ids[idIx - 1] = id;
                                }

                                var respIx = 0;
                                rentedBuffer = ArrayPool<RespServerSession>.Shared.Rent(ids.Length);

                                foreach (var consumer in garnetServer.ActiveConsumers())
                                {
                                    if (consumer is RespServerSession session && ids.IndexOf(session.Id) != -1)
                                    {
                                        rentedBuffer[respIx] = session;
                                        respIx++;
                                    }
                                }

                                toInclude = respIx == rentedBuffer.Length ? rentedBuffer : rentedBuffer.Take(respIx);
                            }
                            finally
                            {
                                if (rentedIds is not null)
                                {
                                    ArrayPool<long>.Shared.Return(rentedIds);
                                }
                            }
                        }
                        else
                        {
                            return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                        }
                    }

                    var nowMilliseconds = Environment.TickCount64;
                    var clusterProvider = this.storeWrapper.clusterProvider;
                    var resultSb = new StringBuilder();
                    var first = true;
                    foreach (var resp in toInclude)
                    {
                        if (!first)
                        {
                            // Redis uses a single \n, not \r\n like you might expect
                            resultSb.Append("\n");
                        }

                        WriteClientInfo(clusterProvider, resultSb, resp, nowMilliseconds);
                        first = false;
                    }

                    resultSb.Append("\n");
                    var result = resultSb.ToString();
                    WriteVerbatimString(Encoding.ASCII.GetBytes(result));

                    return true;
                }
                finally
                {
                    if (rentedBuffer is not null)
                    {
                        ArrayPool<RespServerSession>.Shared.Return(rentedBuffer);
                    }
                }
            }
            else
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_CANNOT_LIST_CLIENTS);
            }
        }

        /// <summary>
        /// CLIENT INFO
        /// </summary>
        /// <returns></returns>
        private bool NetworkCLIENTINFO()
        {
            if (parseState.Count != 0)
            {
                return AbortWithWrongNumberOfArguments("client|info");
            }

            var resultSb = new StringBuilder();
            WriteClientInfo(storeWrapper.clusterProvider, resultSb, this, Environment.TickCount64);

            resultSb.Append("\n");
            var result = resultSb.ToString();
            WriteVerbatimString(Encoding.ASCII.GetBytes(result));

            return true;
        }

        /// <summary>
        /// CLIENT KILL
        /// </summary>
        private bool NetworkCLIENTKILL()
        {
            if (Server is GarnetServerBase garnetServer)
            {
                if (parseState.Count == 0)
                {
                    // Nothing takes 0 args

                    return AbortWithWrongNumberOfArguments("CLIENT|KILL");
                }
                else if (parseState.Count == 1)
                {
                    // Old ip:port format

                    var target = parseState.GetString(0);

                    foreach (var consumer in garnetServer.ActiveConsumers())
                    {
                        if (consumer is RespServerSession session)
                        {
                            if (session.networkSender.RemoteEndpointName == target)
                            {
                                _ = session.TryKill();

                                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                                    SendAndReset();

                                return true;
                            }
                        }
                    }

                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_NO_SUCH_CLIENT, ref dcurr, dend))
                        SendAndReset();

                    return true;
                }
                else
                {
                    // New filter + value format

                    long? id = null;
                    ClientType? type = null;
                    string user = null;
                    string addr = null;
                    string lAddr = null;
                    bool? skipMe = null;
                    long? maxAge = null;

                    // Parse out all the filters
                    var argIx = 0;
                    while (argIx < parseState.Count)
                    {
                        if (argIx + 1 >= parseState.Count)
                        {
                            return AbortWithWrongNumberOfArguments("CLIENT|KILL");
                        }

                        ref var filter = ref parseState.GetArgSliceByRef(argIx);
                        var filterSpan = filter.Span;

                        var valueIx = argIx + 1;
                        ref var value = ref parseState.GetArgSliceByRef(valueIx);

                        AsciiUtils.ToUpperInPlace(filterSpan);

                        if (filterSpan.SequenceEqual(CmdStrings.ID))
                        {
                            if (!ParseUtils.TryReadLong(ref value, out var idParsed))
                            {
                                return AbortWithErrorMessage(Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrShouldBeGreaterThanZero, "client-id")));
                            }

                            if (id is not null)
                            {
                                return AbortWithErrorMessage(Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrDuplicateFilter, "ID")));
                            }

                            id = idParsed;
                        }
                        else if (filterSpan.SequenceEqual(CmdStrings.TYPE))
                        {
                            if (type is not null)
                            {
                                return AbortWithErrorMessage(Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrDuplicateFilter, "TYPE")));
                            }

                            if (!parseState.TryGetClientType(valueIx, out var typeParsed))
                            {
                                var typeStr = ParseUtils.ReadString(ref value);
                                return AbortWithErrorMessage(Encoding.UTF8.GetBytes(string.Format(CmdStrings.GenericUnknownClientType, typeStr)));
                            }

                            // Map SLAVE -> REPLICA for easier checking later
                            typeParsed = typeParsed == ClientType.SLAVE ? ClientType.REPLICA : typeParsed;

                            type = typeParsed;
                        }
                        else if (filterSpan.SequenceEqual(CmdStrings.USER))
                        {
                            if (user is not null)
                            {
                                return AbortWithErrorMessage(Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrDuplicateFilter, "USER")));
                            }

                            user = ParseUtils.ReadString(ref value);
                        }
                        else if (filterSpan.SequenceEqual(CmdStrings.ADDR))
                        {
                            if (addr is not null)
                            {
                                return AbortWithErrorMessage(Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrDuplicateFilter, "ADDR")));
                            }

                            addr = ParseUtils.ReadString(ref value);
                        }
                        else if (filterSpan.SequenceEqual(CmdStrings.LADDR))
                        {
                            if (lAddr is not null)
                            {
                                return AbortWithErrorMessage(Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrDuplicateFilter, "LADDR")));
                            }

                            lAddr = ParseUtils.ReadString(ref value);
                        }
                        else if (filterSpan.SequenceEqual(CmdStrings.SKIPME))
                        {
                            if (skipMe is not null)
                            {
                                return AbortWithErrorMessage(Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrDuplicateFilter, "SKIPME")));
                            }

                            AsciiUtils.ToUpperInPlace(value.Span);

                            if (value.Span.SequenceEqual(CmdStrings.YES))
                            {
                                skipMe = true;
                            }
                            else if (value.Span.SequenceEqual(CmdStrings.NO))
                            {
                                skipMe = false;
                            }
                            else
                            {
                                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
                            }
                        }
                        else if (filterSpan.SequenceEqual(CmdStrings.MAXAGE))
                        {
                            if (!ParseUtils.TryReadLong(ref value, out var maxAgeParsed))
                            {
                                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
                            }

                            if (maxAge is not null)
                            {
                                return AbortWithErrorMessage(Encoding.ASCII.GetBytes(string.Format(CmdStrings.GenericErrDuplicateFilter, "MAXAGE")));
                            }

                            maxAge = maxAgeParsed;
                        }
                        else
                        {
                            return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR);
                        }

                        argIx += 2;
                    }

                    // SKIPME defaults to true
                    skipMe ??= true;

                    logger?.LogInformation("Killing all sessions with id={id}, type={type}, user={user}, addr={addr}, laddr={lAddr}, maxAge={maxAge}, skipMe={skipMe}", id, type, user, addr, lAddr, maxAge, skipMe);

                    var nowMilliseconds = Environment.TickCount64;

                    // Actually go an kill matching ressions
                    var killed = 0;
                    foreach (var consumer in garnetServer.ActiveConsumers())
                    {
                        if (consumer is RespServerSession session)
                        {
                            if (!IsMatch(storeWrapper.clusterProvider, this, nowMilliseconds, session, id, type, user, addr, lAddr, maxAge, skipMe.Value))
                            {
                                continue;
                            }

                            logger?.LogInformation("Attempting to kill session {Id}", session.Id);

                            if (session.TryKill())
                            {
                                logger?.LogInformation("Killed session {Id}", session.Id);

                                killed++;
                            }
                        }
                    }

                    // Hand back result, which is count of clients _actually_ killed
                    while (!RespWriteUtils.TryWriteInt32(killed, ref dcurr, dend))
                        SendAndReset();

                    return true;
                }
            }
            else
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_CANNOT_LIST_CLIENTS);
            }

            // Returns true if the TARGET session is a match for all the given filter values
            static bool IsMatch(
                IClusterProvider clusterProvider,
                RespServerSession currentSession,
                long nowMilliseconds,
                RespServerSession targetSession,
                long? id,
                ClientType? type,
                string user,
                string addr,
                string lAddr,
                long? maxAge,
                bool skipMe
            )
            {
                if (skipMe && ReferenceEquals(currentSession, targetSession))
                {
                    return false;
                }

                var matches = true;
                if (id.HasValue)
                {
                    matches &= id.Value == targetSession.Id;
                }

                if (type is not null)
                {
                    ClientType targetType;
                    if (clusterProvider is not null && targetSession.clusterSession?.RemoteNodeId is not null)
                    {
                        if (clusterProvider.IsReplica(targetSession.clusterSession.RemoteNodeId))
                        {
                            targetType = ClientType.REPLICA;
                        }
                        else
                        {
                            targetType = ClientType.MASTER;
                        }
                    }
                    else
                    {
                        targetType = targetSession.isSubscriptionSession ? ClientType.PUBSUB : ClientType.NORMAL;
                    }

                    matches &= type.Value == targetType;
                }

                if (user is not null)
                {
                    // Using an ORDINAL match to fail-safe, if unicode normalization would change either name I'd prefer to not-match
                    matches &= user.Equals(targetSession._userHandle?.User.Name, StringComparison.Ordinal);
                }

                if (addr is not null)
                {
                    // Same logic, using ORDINAL to fail-safe
                    matches &= targetSession.networkSender.RemoteEndpointName.Equals(addr, StringComparison.Ordinal);
                }

                if (lAddr is not null)
                {
                    // And again, ORDINAL
                    matches &= targetSession.networkSender.LocalEndpointName.Equals(lAddr, StringComparison.Ordinal);
                }

                if (maxAge is not null)
                {
                    var targeAge = (nowMilliseconds - targetSession.CreationTicks) / 1_000;

                    matches &= targeAge > maxAge.Value;
                }

                return matches;
            }
        }

        /// <summary>
        /// CLIENT GETNAME
        /// </summary>
        private bool NetworkCLIENTGETNAME()
        {
            if (parseState.Count != 0)
            {
                return AbortWithWrongNumberOfArguments("CLIENT|GETNAME");
            }

            if (string.IsNullOrEmpty(this.clientName))
            {
                WriteNull();
            }
            else
            {
                while (!RespWriteUtils.TryWriteAsciiBulkString(this.clientName, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// CLIENT SETNAME
        /// </summary>
        private bool NetworkCLIENTSETNAME()
        {
            if (parseState.Count != 1)
            {
                return AbortWithWrongNumberOfArguments("CLIENT|SETNAME");
            }

            if (!parseState.TryGetClientName(0, out var name))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_INVALID_CLIENT_NAME);
            }

            this.clientName = name;

            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// CLIENT SETINFO
        /// </summary>
        private bool NetworkCLIENTSETINFO()
        {
            if (parseState.Count != 2)
            {
                return AbortWithWrongNumberOfArguments("CLIENT|SETINFO");
            }

            var option = parseState.GetArgSliceByRef(0);
            var value = parseState.GetString(1);
            if (option.Span.SequenceEqual(CmdStrings.LIB_NAME) || option.Span.SequenceEqual(CmdStrings.lib_name)) // Can't use EqualsUpperCaseSpanIgnoringCase as `-` is not upper case
            {
                this.clientLibName = value;
            }
            else if (option.Span.SequenceEqual(CmdStrings.LIB_VER) || option.Span.SequenceEqual(CmdStrings.lib_ver))
            {
                this.clientLibVersion = value;
            }
            else
            {
                return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
            }

            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// CLIENT UNBLOCK
        /// </summary>
        private bool NetworkCLIENTUNBLOCK()
        {
            if (parseState.Count is not (1 or 2))
            {
                return AbortWithWrongNumberOfArguments("client|unblock");
            }

            if (!parseState.TryGetLong(0, out var clientId))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
            }

            var toThrowError = false;
            if (parseState.Count == 2)
            {
                var option = parseState.GetArgSliceByRef(1);
                if (option.Span.EqualsUpperCaseSpanIgnoringCase(CmdStrings.TIMEOUT))
                {
                    toThrowError = false;
                }
                else if (option.Span.EqualsUpperCaseSpanIgnoringCase(CmdStrings.ERROR))
                {
                    toThrowError = true;
                }
                else
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_INVALID_CLIENT_UNBLOCK_REASON);
                }
            }

            if (Server is GarnetServerBase garnetServer)
            {
                var session = garnetServer.ActiveConsumers().OfType<RespServerSession>().FirstOrDefault(x => x.Id == clientId);

                if (session is null)
                {
                    while (!RespWriteUtils.TryWriteInt32(0, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                if (session.storeWrapper?.itemBroker is not null)
                {
                    var isBlocked = session.storeWrapper.itemBroker.TryGetObserver(session.ObjectStoreSessionID, out var observer);

                    if (!isBlocked)
                    {
                        while (!RespWriteUtils.TryWriteInt32(0, ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }

                    var result = observer.TryForceUnblock(toThrowError);

                    while (!RespWriteUtils.TryWriteInt32(result ? 1 : 0, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.TryWriteInt32(0, ref dcurr, dend))
                        SendAndReset();
                }
            }
            else
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_UBLOCKING_CLINET, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }
    }
}