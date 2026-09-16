// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Globalization;
using System.IO;
using System.Linq;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Swaps the up-converted object log produced by an <c>--upgrade</c> run in as the store's live object log.
    /// <para>The swap is 2N file moves (N segments retired, N promoted), so it is journaled: a marker file naming the three base
    /// names is written before the first move and deleted only after the last. A marker present at startup means a previous swap was
    /// interrupted and the object log is in a mixed state; an <c>--upgrade</c> run finishes it, and any other run refuses to open.
    /// Every move is "move only if the source exists and the destination does not", so completing an interrupted swap is just
    /// re-running it.</para>
    /// </summary>
    public static class ObjectLogUpgradeSwap
    {
        /// <summary>Name of the journal file written while the object-log segments are being renamed.</summary>
        public const string MarkerFileName = "hlog_objs.upgrade-marker";

        /// <summary>The store subdirectory of the log directory; must match the <c>FileDescriptor</c> directory used to build the devices.</summary>
        public const string StoreDirectoryName = "Store";

        public static string GetStoreDirectory(string logDir) => Path.Combine(logDir, StoreDirectoryName);

        public static string GetMarkerPath(string logDir) => Path.Combine(GetStoreDirectory(logDir), MarkerFileName);

        /// <summary>Whether a previous swap was interrupted and left the object log partly renamed.</summary>
        public static bool HasPendingSwap(string logDir) => File.Exists(GetMarkerPath(logDir));

        /// <summary>
        /// Resolve any interrupted swap before the object-log devices are opened. An upgrade run completes it; any other run must refuse,
        /// because the object log under the live name is at best incomplete.
        /// </summary>
        /// <param name="logDir">The configured log directory.</param>
        /// <param name="isUpgrade">Whether this run was started with <c>--upgrade</c>.</param>
        /// <param name="logger">Logger.</param>
        public static void ResolvePendingSwap(string logDir, bool isUpgrade, ILogger logger)
        {
            if (!HasPendingSwap(logDir))
                return;

            if (!isUpgrade)
            {
                throw new GarnetException($"An object-log upgrade was interrupted while renaming segments ({GetMarkerPath(logDir)} is present)."
                    + " The object log is incomplete; re-run with --upgrade to finish the rename before starting the server.");
            }

            logger?.LogWarning("Found an interrupted object-log upgrade rename ({marker}); completing it before recovery.", GetMarkerPath(logDir));
            CompleteSwap(logDir, logger);
        }

        /// <summary>
        /// Reject an upgrade run that would append to object-log data left by a previous attempt. The upgrade device is written from its
        /// start, so reusing a non-empty one would interleave two conversions into one file.
        /// </summary>
        /// <param name="logDir">The configured log directory.</param>
        /// <param name="upgradeObjectLogFileName">Base name of the upgrade object log.</param>
        public static void VerifyNoPriorUpgradeAttempt(string logDir, string upgradeObjectLogFileName)
        {
            var dir = GetStoreDirectory(logDir);
            if (!Directory.Exists(dir))
                return;

            var leftovers = GetSegmentFiles(dir, upgradeObjectLogFileName);
            if (leftovers.Length > 0)
            {
                throw new GarnetException($"Object-log upgrade target '{Path.Combine(dir, upgradeObjectLogFileName)}' already has {leftovers.Length} segment file(s)"
                    + " from a previous upgrade attempt. Move or delete them before re-running --upgrade.");
            }
        }

        /// <summary>
        /// Retire the downlevel object log and promote the up-converted one in its place. Must be called with the store closed, so no
        /// device holds either file.
        /// </summary>
        /// <param name="logDir">The configured log directory.</param>
        /// <param name="objectLogFileName">Base name of the live object log.</param>
        /// <param name="upgradeObjectLogFileName">Base name of the up-converted object log.</param>
        /// <param name="logger">Logger.</param>
        public static void Swap(string logDir, string objectLogFileName, string upgradeObjectLogFileName, ILogger logger)
        {
            var dir = GetStoreDirectory(logDir);
            var retiredFileName = $"{objectLogFileName}_pre_upgrade_{DateTime.UtcNow.ToString("yyyyMMddHHmmss", CultureInfo.InvariantCulture)}";
            if (GetSegmentFiles(dir, retiredFileName).Length > 0)
                throw new GarnetException($"Object-log upgrade cannot retire the downlevel log to '{retiredFileName}': segment files with that base name already exist.");

            // Journal the three base names before the first move, so an interrupted rename can be identified and finished.
            File.WriteAllLines(GetMarkerPath(logDir), [objectLogFileName, upgradeObjectLogFileName, retiredFileName]);
            CompleteSwap(logDir, logger);
        }

        static void CompleteSwap(string logDir, ILogger logger)
        {
            var markerPath = GetMarkerPath(logDir);
            var names = File.ReadAllLines(markerPath);
            if (names.Length < 3 || names.Take(3).Any(string.IsNullOrWhiteSpace))
                throw new GarnetException($"Object-log upgrade marker '{markerPath}' is malformed; it must name the live, upgrade, and retired object logs.");

            var dir = GetStoreDirectory(logDir);
            var movedOut = MoveSegments(dir, names[0], names[2]);
            var movedIn = MoveSegments(dir, names[1], names[0]);
            File.Delete(markerPath);

            logger?.LogInformation("Object-log upgrade: retired {movedOut} downlevel segment(s) to '{retired}' and promoted {movedIn} up-converted segment(s) to '{live}'.",
                movedOut, names[2], movedIn, names[0]);
        }

        /// <summary>Move every segment file of <paramref name="fromBase"/> to <paramref name="toBase"/>, keeping its segment suffix.</summary>
        static int MoveSegments(string dir, string fromBase, string toBase)
        {
            var moved = 0;
            foreach (var sourcePath in GetSegmentFiles(dir, fromBase))
            {
                var suffix = Path.GetFileName(sourcePath)[fromBase.Length..];
                var destinationPath = Path.Combine(dir, toBase + suffix);
                if (File.Exists(destinationPath))
                    throw new GarnetException($"Object-log upgrade cannot move '{sourcePath}' to '{destinationPath}': the destination already exists.");
                File.Move(sourcePath, destinationPath);
                ++moved;
            }
            return moved;
        }

        /// <summary>
        /// The segment files of one log base name. A segment file is the base name followed by '.' and the segment id, so the match is
        /// verified rather than left to the glob, which would also accept a longer base name that happens to share the prefix.
        /// </summary>
        static string[] GetSegmentFiles(string dir, string baseFileName)
        {
            if (!Directory.Exists(dir))
                return [];
            return [.. Directory.GetFiles(dir, baseFileName + ".*")
                .Where(path => IsSegmentFileName(Path.GetFileName(path), baseFileName))
                .OrderBy(path => path, StringComparer.Ordinal)];
        }

        static bool IsSegmentFileName(string fileName, string baseFileName)
        {
            if (fileName.Length <= baseFileName.Length + 1
                    || !fileName.StartsWith(baseFileName, StringComparison.Ordinal)
                    || fileName[baseFileName.Length] != '.')
                return false;
            return fileName.AsSpan(baseFileName.Length + 1).ContainsAnyExcept("0123456789") == false;
        }
    }
}