// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Garnet checkpoint manager, inherits from Tsavorite's DeviceLogCommitCheckpointManager
    /// </summary>
    public class GarnetCheckpointManager : DeviceLogCommitCheckpointManager
    {
        public string CurrentHistoryId { get; set; }
        public string RecoveredHistoryId { get; set; }
        public AofAddress CurrentSafeAofAddress { get; private set; }
        public AofAddress RecoveredSafeAofAddress { get; private set; }

        /// <summary>
        /// Create new instance of Garnet checkpoint manager
        /// </summary>
        /// <param name="AofPhysicalSublogCount">Number of sublog for Aof</param>
        /// <param name="deviceFactoryCreator">Factory for getting devices</param>
        /// <param name="checkpointNamingScheme">Checkpoint naming helper</param>
        /// <param name="removeOutdated">Remove older Tsavorite log commits</param>
        /// <param name="fastCommitThrottleFreq">FastCommit throttle frequency - use only in FastCommit mode</param>
        /// <param name="logger">Logger</param>
        public GarnetCheckpointManager(int AofPhysicalSublogCount, INamedDeviceFactoryCreator deviceFactoryCreator, ICheckpointNamingScheme checkpointNamingScheme, bool removeOutdated = true, int fastCommitThrottleFreq = 0, ILogger logger = null)
            : base(deviceFactoryCreator, checkpointNamingScheme, removeOutdated, fastCommitThrottleFreq, logger)
        {
            CurrentHistoryId = null;
            RecoveredHistoryId = null;
            CurrentSafeAofAddress = AofAddress.Create(AofPhysicalSublogCount, 0);
            RecoveredSafeAofAddress = AofAddress.Create(AofPhysicalSublogCount, 0);
        }

        /// <summary>
        /// Set current AOF address
        /// </summary>
        /// <param name="safeAofTailAddress"></param>
        public void SetCurrentSafeAofAddress(ref AofAddress safeAofTailAddress) => CurrentSafeAofAddress = safeAofTailAddress;

        /// <summary>
        /// Set recovered AOF address
        /// </summary>
        /// <param name="recoveredSafeAofAddress"></param>
        public void SetRecoveredSafeAofAddress(ref AofAddress recoveredSafeAofAddress) => RecoveredSafeAofAddress = recoveredSafeAofAddress;

        /// <summary>
        /// Trailer appended to the cookie, identifying the on-disk layout a checkpoint was written with.
        /// Both cookie layouts read by the cluster replication layer are parsed from the front and ignore
        /// trailing bytes, so appending is backward compatible.
        /// </summary>
        private static ReadOnlySpan<byte> LayoutTrailerMagic => "GNTLAYOUT"u8;

        /// <summary>
        /// Layout in which every database owns its hybrid log devices (<c>Store/hlog[_dbId]</c>).
        /// Checkpoints written before per-database log devices carry no trailer at all.
        /// </summary>
        public const int PerDatabaseHybridLogLayout = 1;

        private static int LayoutTrailerLength => LayoutTrailerMagic.Length + sizeof(int);

        /// <summary>
        /// Whether to append the layout trailer to the cookie. Cluster mode permits only the default
        /// database, so its checkpoints never need the marker and its cookie stays byte-identical.
        /// </summary>
        protected virtual bool EmitLayoutTrailer => true;

        /// <summary>
        /// Read the on-disk layout a checkpoint was written with from its cookie.
        /// </summary>
        /// <param name="cookie">Cookie recovered from the checkpoint metadata</param>
        /// <param name="layout">Layout identifier, when present</param>
        /// <returns>True if the cookie carries a layout trailer</returns>
        public static bool TryGetCheckpointLayout(byte[] cookie, out int layout)
        {
            layout = 0;
            if (cookie == null || cookie.Length < LayoutTrailerLength)
                return false;

            var trailer = cookie.AsSpan(cookie.Length - LayoutTrailerLength);
            if (!trailer[..LayoutTrailerMagic.Length].SequenceEqual(LayoutTrailerMagic))
                return false;

            layout = BinaryPrimitives.ReadInt32LittleEndian(trailer[LayoutTrailerMagic.Length..]);
            return true;
        }

        private static byte[] AppendLayoutTrailer(byte[] cookie)
        {
            var body = cookie ?? [];
            var result = new byte[body.Length + LayoutTrailerLength];
            body.CopyTo(result, 0);

            var trailer = result.AsSpan(body.Length);
            LayoutTrailerMagic.CopyTo(trailer);
            BinaryPrimitives.WriteInt32LittleEndian(trailer[LayoutTrailerMagic.Length..], PerDatabaseHybridLogLayout);
            return result;
        }

        /// <inheritdoc />
        public override byte[] GetCookie()
        {
            var body = GetCookieBody();
            return EmitLayoutTrailer ? AppendLayoutTrailer(body) : body;
        }

        private unsafe byte[] GetCookieBody()
        {
            if (CurrentHistoryId == null) return null;

            if (CurrentSafeAofAddress.Length == 1)
            {
                // Legacy single log serialization
                var cookie = new byte[sizeof(int) + sizeof(long) + CurrentHistoryId.Length];
                var primaryReplIdBytes = Encoding.ASCII.GetBytes(CurrentHistoryId);
                fixed (byte* ptr = cookie)
                fixed (byte* pridPtr = primaryReplIdBytes)
                {
                    *(int*)ptr = sizeof(long) + CurrentHistoryId.Length;
                    *(long*)(ptr + 4) = CurrentSafeAofAddress[0];
                    Buffer.MemoryCopy(pridPtr, ptr + 12, primaryReplIdBytes.Length, primaryReplIdBytes.Length);
                }
                return cookie;
            }
            else
            {
                // Multi-log serialization
                using var ms = new MemoryStream();
                using var writer = new BinaryWriter(ms, Encoding.ASCII);

                //1. Write history-Id
                writer.Write(CurrentHistoryId == null ? 0 : 1);
                if (CurrentHistoryId != null) writer.Write(CurrentHistoryId);
                //2. Write checkpoint covered aof address
                CurrentSafeAofAddress.Serialize(writer);

                var byteArray = ms.ToArray();
                return byteArray;
            }
        }
    }
}