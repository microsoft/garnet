// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using Garnet.common;
using Garnet.networking;
using Microsoft.Extensions.Logging;
using SocketSets;

namespace Garnet.server
{
    /// <summary>
    /// Garnet Server using Socket Set for networking.
    /// </summary>
    public sealed class GarnetServerSocketSet : GarnetServerBase, IServerHook
    {
        private sealed class Transport(GarnetServerSocketSet owner, SocketSetOptions options) : SocketSet(options)
        {
            protected override void OnAccept(ref AcceptContext ctx)
            {
                var conn = ctx.Connection;
                var sender = new SocketSetNetworkSender(conn, owner._bufferSettings, owner._pool,
                                                        localByConstruction: owner._unixDomain);
                var handler = new SocketSetGarnetHandler(owner, sender, owner._bufferSettings, owner._pool);
                handler.StartReceive();
                conn.UserToken = handler;
                // The BASE owns the handler dictionary; mirroring GarnetServerTcp's accept path.
                if (!owner.activeHandlers.TryAdd(handler, default))
                {
                    handler.CloseFromTransport();
                    return;
                }
                owner.IncrementConnectionsReceived();
            }

            protected override void OnReceive(ref ReceiveContext ctx)
            {
                if (ctx.Connection.UserToken is SocketSetGarnetHandler h && !h.Feed(ctx.Payload))
                    ctx.Connection.Close();
            }

            protected override void OnClosed(Connection connection)
            {
                if (connection.UserToken is SocketSetGarnetHandler h) owner.DropHandler(h);
            }
        }

        private readonly Transport _transport;
        private readonly NetworkBufferSettings _bufferSettings;
        private readonly LimitedFixedBufferPool _pool;

        /// <summary>
        /// Whether every peer on this server is local BY CONSTRUCTION — i.e. we are listening on AF_UNIX,
        /// which is same-host by definition and has no network form. Decided once at construction from the
        /// listen endpoint, so answering <c>IsLocalConnection</c> for a UDS peer costs nothing per connection
        /// and needs no peer-address plumbing. See REVIEW.md F9 for why the TCP case still cannot be answered.
        /// </summary>
        private readonly bool _unixDomain;

        public GarnetServerSocketSet(EndPoint endpoint, SocketSetOptions options,
                                     int serverBufferSize = 1 << 17, ILogger logger = null)
            : base(endpoint, serverBufferSize, logger)
        {
            _unixDomain = endpoint is System.Net.Sockets.UnixDomainSocketEndPoint;
            _bufferSettings = new NetworkBufferSettings(serverBufferSize, serverBufferSize);
            _pool = _bufferSettings.CreateBufferPool(ownerType: PoolOwnerType.ServerNetwork, logger: logger);
            _transport = new Transport(this, options);
        }

        public override void Start() => _transport.Listen(EndPoint);

        public override void Close() => _transport.Dispose();

        public override IEnumerable<IMessageConsumer> ActiveConsumers()
        {
            foreach (var kvp in activeHandlers)
            {
                var consumer = kvp.Key.Session;
                if (consumer != null) yield return consumer;
            }
        }

        public override IEnumerable<IClusterSession> ActiveClusterSessions()
        {
            foreach(var kvp in activeHandlers)
            {
                var consumer = kvp.Key.Session;
                if (consumer != null)
                    yield return ((RespServerSession)consumer).clusterSession;
            }
        }

        // ---- IServerHook: mirrors GarnetServerTcp.TryCreateMessageConsumer, including the >=4-byte gate
        //      (a shorter first packet must return false so the handler calls back with more bytes) and the
        //      throw-on-missing-provider semantics. ----

        public bool TryCreateMessageConsumer(Span<byte> bytes, INetworkSender networkSender, out IMessageConsumer session)
        {
            session = null!;

            // We need at least 4 bytes to determine the session's wire format.
            if (bytes.Length < 4) return false;

            WireFormat protocol = WireFormat.ASCII;
            if (!GetSessionProviders().TryGetValue(protocol, out var provider))
            {
                var input = System.Text.Encoding.ASCII.GetString(bytes);
                logger?.LogError("Cannot identify wire protocol {bytes}", input);
                throw new Exception($"Unsupported incoming wire format {protocol} {input}");
            }

            if (!AddSession(protocol, ref provider, networkSender, out session))
                throw new Exception("Unable to add session");

            return true;
        }

        public void DisposeMessageConsumer(INetworkHandler session)
        {
            if (session is SocketSetGarnetHandler h) DropHandler(h);
        }

        private void DropHandler(SocketSetGarnetHandler handler)
        {
            if (activeHandlers.TryRemove(handler, out _))
            {
                IncrementConnectionsDisposed();
                try { handler.Session?.Dispose(); } catch { }
                handler.CloseFromTransport();
            }
        }

        public override void Dispose()
        {
            Close();
            foreach (var kvp in activeHandlers)
            {
                if (kvp.Key is SocketSetGarnetHandler h) DropHandler(h);
            }
            base.Dispose();
            _pool.Dispose();
        }

        /// <summary>
        /// Per-connection glue: Garnet's <see cref="NetworkHandler{TServerHook,TNetworkSender}"/> driven from
        /// SocketSet's receive callback instead of a SocketAsyncEventArgs loop.
        ///
        /// The base class already owns everything hard — the receive-buffer accumulation and shift for partial
        /// messages, the wire-format sniff via <c>IServerHook.TryCreateMessageConsumer</c> on first bytes, the
        /// consumer dispatch, and the whole <see cref="INetworkSender"/> surface (with TLS interposition we
        /// deliberately never enable: SocketSet terminates TLS in-transport, so this handler only ever sees
        /// plaintext — which is precisely what makes a Garnet-TLS A/B a pure their-TLS-vs-ours comparison).
        /// All this class adds is (a) buffer allocation at start, mirroring <c>TcpNetworkHandlerBase</c>, and
        /// (b) <see cref="Feed"/>, which lands transport bytes in the base's buffer and calls the same public
        /// entrypoint the SAEA completion path calls.
        /// </summary>
        internal sealed unsafe class SocketSetGarnetHandler : NetworkHandler<GarnetServerSocketSet, SocketSetNetworkSender>
        {
            // The base holds its sender PRIVATELY, so the identity/lifecycle abstracts it delegates to us
            // cannot be answered from it -- keep our own reference.
            private readonly SocketSetNetworkSender _sender;

            public SocketSetGarnetHandler(GarnetServerSocketSet serverHook, SocketSetNetworkSender sender,
                                          NetworkBufferSettings settings, LimitedFixedBufferPool pool)
                : base(serverHook, sender, settings, pool, useTLS: false)
            {
                _sender = sender;
            }

            /// <summary>Allocate the receive buffer the base's accumulation machinery works over — the part
            /// <c>TcpNetworkHandlerBase.Start</c> does before arming its first SAEA receive.</summary>
            public void StartReceive()
            {
                networkReceiveBufferEntry = networkPool.Get(networkBufferSettings.initialReceiveBufferSize);
                networkReceiveBuffer = networkReceiveBufferEntry.entry;
                networkReceiveBufferPtr = networkReceiveBufferEntry.entryPtr;
            }

            /// <summary>
            /// Bytes arrived on the shard loop thread. The transport owns <paramref name="payload"/> only for
            /// the duration of the callback, so it is copied into the handler's own buffer — one copy the SAEA
            /// path does not pay (it receives directly into this buffer), accepted as the v1 cost and noted in
            /// the results when measured.
            ///
            /// The chunk loop re-reads the (protected) buffer fields each pass on purpose: the base grows the
            /// buffer for messages larger than it, swapping those fields, and a cached local would quietly keep
            /// copying into the old, abandoned array.
            /// </summary>
            public bool Feed(ReadOnlySpan<byte> payload)
            {
                while (!payload.IsEmpty)
                {
                    int free = networkReceiveBuffer.Length - networkBytesRead;
                    if (free <= 0) return false; // consumer stalled with a full buffer the base chose not to grow
                    int take = Math.Min(free, payload.Length);
                    payload.Slice(0, take).CopyTo(networkReceiveBuffer.AsSpan(networkBytesRead));
                    OnNetworkReceiveWithoutTLS(take);
                    payload = payload.Slice(take);
                }
                return true;
            }

            // NetworkHandler leaves the sender-identity and lifecycle abstracts to its subclass (the SAEA one
            // answers them from its Socket); ours answers from the wrapped sender.
            public override string RemoteEndpointName => _sender.RemoteEndpointName;
            public override string LocalEndpointName => _sender.LocalEndpointName;
            public override bool IsLocalConnection() => _sender.IsLocalConnection();
            public override bool TryClose() => _sender.TryClose();

            public override void Dispose()
            {
                networkReceiveBufferEntry?.Dispose();
                _sender.Dispose();
            }

            public void CloseFromTransport()
            {
                try { Dispose(); }
                catch { /* teardown must not throw into the loop thread */ }
            }
        }

        /// <summary>
        /// Garnet's outbound seam (<see cref="INetworkSender"/>) over a SocketSet <see cref="Connection"/>.
        ///
        /// The contract Garnet sessions actually use: <c>GetResponseObject</c> leases a PINNED buffer whose
        /// head/tail pointers the session writes RESP into, then <c>SendResponse(offset, size)</c> ships it.
        /// <see cref="Connection.Send(ReadOnlySpan{byte})"/> COPIES into transport buffers and is
        /// callable from any thread, so the lease can be returned/reused immediately after the call — every
        /// completion contract here is satisfied synchronously, which removes the callback machinery the SAEA
        /// sender needs (its buffers stay in flight until the socket completion fires).
        ///
        /// One copy per response, same as the SAEA path's kernel handoff — and the deferred-flush lesson from
        /// the proxy does NOT need replicating here yet: Garnet sessions batch replies into the response buffer
        /// themselves and send once per processed batch, so callback-granularity batching already exists one
        /// level up. Verify with SS_URING_STATS before believing that sentence under pipelined load.
        /// </summary>
        internal sealed unsafe class SocketSetNetworkSender : NetworkSenderBase
        {
            private readonly Connection _conn;
            private readonly LimitedFixedBufferPool _pool;
            private string _remoteName, _localName;

            private PoolEntry _reusableResponse;

            public SocketSetNetworkSender(Connection conn, NetworkBufferSettings settings, LimitedFixedBufferPool pool,
                                          string remoteName = null, string localName = null,
                                          bool localByConstruction = false)
                : base(settings.sendBufferSize)
            {
                _conn = conn;
                _pool = pool;
                _remoteName = remoteName;
                _localName = localName;
                _localByConstruction = localByConstruction;
            }

            private readonly bool _localByConstruction;

            /// <summary>
            /// Garnet surfaces these directly in <c>CLIENT LIST</c> as <c>addr=</c> / <c>laddr=</c>, and they
            /// were both the literal <c>"socketset"</c> until 2026-08-08 — which broke <c>CLIENT KILL ADDR</c>,
            /// <c>CLIENT LIST</c> filtering, and any tool that identifies a client by address, because every
            /// client was indistinguishable.
            ///
            /// FORMATTED LAZILY AND CACHED, because the interface demands a <c>string</c> and the transport must
            /// not pay for one per accept. <see cref="Connection.RemoteAddress"/> is an inline value type;
            /// nothing allocates until Garnet actually asks, and then once per connection.
            ///
            /// Falls back to <c>"socketset"</c> only when the address is genuinely unavailable — tracking off, an
            /// io_uring backend, or an unnamed AF_UNIX peer. That string is a poor answer, but it is the HONEST
            /// one, and it is now the exception rather than the rule.
            /// </summary>
            public override string RemoteEndpointName => _remoteName ??= Describe(_conn.RemoteAddress);

            /// <inheritdoc cref="RemoteEndpointName"/>
            public override string LocalEndpointName => _localName ??= Describe(_conn.LocalAddress);

            private static string Describe(PeerAddress address)
            {
                if (!address.IsSet) return "socketset";
                Span<char> buf = stackalloc char[64];
                return address.TryFormat(buf, out int n) ? buf.Slice(0, n).ToString() : "socketset";
            }

            /// <summary>
            /// FAILS CLOSED, deliberately, and this is a SECURITY answer rather than a fidelity one.
            ///
            /// Garnet calls this in exactly two places — <c>RespServerSession.CanRunDebug()</c> and
            /// <c>CanRunModule()</c> — to implement <c>ConnectionProtectionOption.Local</c>, i.e. "allow DEBUG /
            /// MODULE only from loopback". Stock <c>GarnetTcpNetworkSender</c> answers it with
            /// <c>IPAddress.IsLoopback(remote)</c>.
            ///
            /// This used to return <c>true</c> unconditionally, with a comment that all traffic was same-host.
            /// That precondition is void — SocketSet binds and serves real LAN addresses (see
            /// <c>bench/Verify-BindReachability.ps1</c>) — and the consequence was that an operator who chose
            /// <c>Local</c> BELIEVING it restricted access silently got <c>Yes</c>: DEBUG and MODULE from any
            /// remote peer, where MODULE LOAD is arbitrary code loading. A silently downgraded security control
            /// is worse than an absent one, because it reads as configured.
            ///
            /// <c>Connection</c> exposes no peer endpoint yet (see TODO item 4), so for TCP the honest answer is
            /// "cannot prove loopback" — and the safe direction for a permission check is DENY. That makes
            /// <c>Local</c> behave as <c>No</c> rather than as <c>Yes</c>: it costs a legitimate loopback operator
            /// their DEBUG/MODULE access, and it cannot hand a remote peer anything.
            ///
            /// **AF_UNIX is the exception, and it is answerable TODAY.** A Unix domain socket is same-host by
            /// definition — it has no network form at all — so every peer on a UDS listener is provably local
            /// without any peer-address plumbing. Stock <c>GarnetTcpNetworkSender</c> agrees: it returns
            /// <c>true</c> unconditionally for a <c>UnixDomainSocketEndPoint</c> peer. The flag is decided once
            /// from the LISTEN endpoint in <c>SocketSetGarnetServer</c>, so this costs nothing per connection.
            ///
            /// **RESOLVED 2026-08-08:** the TCP half is now a real test against the peer address rather than a
            /// comment. <see cref="PeerAddress.IsLoopback"/> is false for an address we could not obtain, so the
            /// fail-closed behaviour survives every case where tracking is off or the backend cannot supply one —
            /// which is the property that mattered, and the one the original hard-coded <c>true</c> lacked.
            /// </summary>
            public override bool IsLocalConnection() => _localByConstruction || _conn.RemoteAddress.IsLoopback;

            // Enter/Exit guard the response object against concurrent producers. Garnet's own senders use an
            // epoch/spin scheme; a lock is the v1 that is obviously correct, and the session model mostly
            // serialises callers anyway — measure before optimising it away.
            private readonly object _lock = new();
            public override void Enter() => Monitor.Enter(_lock);
            public override void Exit() => Monitor.Exit(_lock);

            public override void EnterAndGetResponseObject(out byte* head, out byte* tail)
            {
                Monitor.Enter(_lock);
                GetResponseObject();
                head = _reusableResponse!.entryPtr;
                tail = _reusableResponse.entryPtr + _reusableResponse.entry.Length;
            }

            public override void ExitAndReturnResponseObject()
            {
                ReturnResponseObject();
                Monitor.Exit(_lock);
            }

            public override void GetResponseObject() => _reusableResponse ??= _pool.Get(serverBufferSize);

            public override void ReturnResponseObject()
            {
                _reusableResponse?.Dispose();
                _reusableResponse = null;
            }

            public override byte* GetResponseObjectHead() => _reusableResponse is { } e ? e.entryPtr : null;
            public override byte* GetResponseObjectTail() => _reusableResponse is { } e ? e.entryPtr + e.entry.Length : null;

            public override bool SendResponse(int offset, int size)
            {
                var entry = _reusableResponse;
                if (entry is null) return false;
                // Send copies synchronously, so the lease survives for the next response rather than being
                // detached per send — which is what keeps this allocation-flat under load.
                return _conn.Send(new ReadOnlySpan<byte>(entry.entryPtr + offset, size));
            }

            public override void SendResponse(byte[] buffer, int offset, int count, object context)
            {
                _conn.Send(buffer.AsSpan(offset, count));
                SendCallback(context);
            }

            public override void SendCallback(object context) { }

            public override void DisposeNetworkSender(bool waitForSendCompletion)
            {
                // Sends copied synchronously: there is nothing in flight to wait for.
                ReturnResponseObject();
            }

            public override void Throttle() { }

            public override bool TryClose()
            {
                try { _conn.Close(); return true; }
                catch { return false; }
            }

            public override void Dispose() => DisposeNetworkSender(waitForSendCompletion: false);
        }
    }
}
