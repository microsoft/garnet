// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.networking;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Streams new AOF records from a standalone primary over an established PSYNC connection.
    /// </summary>
    internal sealed class StandaloneSyncDriver : IDisposable
    {
        readonly StoreWrapper storeWrapper;
        readonly INetworkSender networkSender;
        readonly ILogger logger;
        readonly CancellationTokenSource cts = new();
        readonly Task syncTask;
        TsavoriteLogScanSingleIterator iterator;

        public StandaloneSyncDriver(StoreWrapper storeWrapper, INetworkSender networkSender, long startAddress, ILogger logger)
        {
            this.storeWrapper = storeWrapper;
            this.networkSender = networkSender;
            this.logger = logger;

            syncTask = Task.Run(() => StreamAsync(startAddress, cts.Token));
        }

        async Task StreamAsync(long startAddress, CancellationToken token)
        {
            try
            {
                iterator = storeWrapper.appendOnlyFile.Log.ScanSingle(
                    0,
                    startAddress,
                    long.MaxValue,
                    scanUncommitted: true,
                    recover: false,
                    logger: logger);

                await foreach (var (entry, entryLength, currentAddress, _) in iterator.GetAsyncEnumerable(token).ConfigureAwait(false))
                {
                    SendFrame(entry.AsSpan(0, entryLength), currentAddress);
                    networkSender.Throttle();
                }
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
            }
            catch (ObjectDisposedException) when (token.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Standalone AOF stream failed");
                networkSender.TryClose();
            }
            finally
            {
                iterator?.Dispose();
                iterator = null;
            }
        }

        unsafe void SendFrame(ReadOnlySpan<byte> payload, long currentAddress)
        {
            Span<byte> header = stackalloc byte[StandaloneReplicationWireFormat.HeaderLength];
            StandaloneReplicationWireFormat.WriteHeader(header, payload.Length, currentAddress);

            networkSender.EnterAndGetResponseObject(out var head, out var tail);
            try
            {
                Write(header, ref head, tail);
                Write(payload, ref head, tail);
                Flush(ref head);
            }
            finally
            {
                networkSender.ExitAndReturnResponseObject();
            }

            void Write(ReadOnlySpan<byte> source, ref byte* destination, byte* end)
            {
                while (!source.IsEmpty)
                {
                    var available = (int)(end - destination);
                    if (available == 0)
                    {
                        Flush(ref destination);
                        networkSender.GetResponseObject();
                        destination = networkSender.GetResponseObjectHead();
                        end = networkSender.GetResponseObjectTail();
                        available = (int)(end - destination);
                    }

                    var count = Math.Min(source.Length, available);
                    source[..count].CopyTo(new Span<byte>(destination, count));
                    destination += count;
                    source = source[count..];
                }
            }

            void Flush(ref byte* destination)
            {
                var responseHead = networkSender.GetResponseObjectHead();
                var length = (int)(destination - responseHead);
                if (length > 0)
                {
                    networkSender.SendResponse(0, length);
                    destination = null;
                }
            }
        }

        public void Dispose()
        {
            cts.Cancel();
            iterator?.Dispose();
            _ = syncTask.ContinueWith(
                static (_, state) => ((CancellationTokenSource)state).Dispose(),
                cts,
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }
    }
}