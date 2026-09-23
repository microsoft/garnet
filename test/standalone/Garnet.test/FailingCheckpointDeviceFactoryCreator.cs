// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using Tsavorite.core;

namespace Garnet.test
{
    /// <summary>
    /// Which store checkpoint devices a <see cref="FailingCheckpointDeviceFactoryCreator"/> should fail to create.
    /// </summary>
    internal enum CheckpointDeviceFailure
    {
        /// <summary>Every checkpoint device is created normally.</summary>
        None,

        /// <summary>
        /// Every checkpoint device creation throws. A full checkpoint therefore fails in the index checkpoint task,
        /// before the hybrid-log checkpoint has been initialized.
        /// </summary>
        All,

        /// <summary>
        /// Only the hybrid-log snapshot devices throw. Those are requested at <c>Phase.WAIT_FLUSH</c>, after the index
        /// checkpoint and the hybrid-log checkpoint have both been initialized, so the abort has state from both to
        /// release. This is the shape of the real failure that motivated this work, where the device layer rejected an
        /// over-long <c>snapshot.obj.dat</c> path.
        /// </summary>
        SnapshotLogDevicesOnly,
    }

    /// <summary>
    /// Device factory creator that makes store checkpoint device creation fail on demand while leaving every other
    /// device - hybrid log, storage tier and AOF - working normally.
    /// </summary>
    /// <remarks>
    /// Injected by assigning it to <c>GarnetServerOptions.DeviceFactoryCreator</c>, which <c>GarnetServer.CreateStore</c>
    /// hands to the store's checkpoint manager. <c>DeviceLogCommitCheckpointManager</c> does not guard its
    /// <c>deviceFactory.Get</c> calls, so a throw from the returned factory propagates out of the checkpoint state
    /// machine the same way a real device-creation failure does, with no dependence on path length, file permissions
    /// or platform. Only devices below the store checkpoint base directory are affected, so the server keeps serving
    /// while its checkpoints fail.
    /// </remarks>
    internal sealed class FailingCheckpointDeviceFactoryCreator : INamedDeviceFactoryCreator
    {
        readonly INamedDeviceFactoryCreator inner = new LocalStorageNamedDeviceFactoryCreator();
        readonly string storeCheckpointBaseDirectory;

        /// <summary>
        /// Which checkpoint devices currently fail to be created. Read on every device request, so a failure can be
        /// armed and disarmed against a running server.
        /// </summary>
        internal volatile CheckpointDeviceFailure FailureMode = CheckpointDeviceFailure.None;

        // Taken from the naming scheme rather than hard-coded, so renaming a checkpoint file cannot silently turn
        // SnapshotLogDevicesOnly into a mode that fails nothing.
        static readonly string LogSnapshotFileName = new DefaultCheckpointNamingScheme(string.Empty).LogSnapshot(Guid.Empty).fileName;
        static readonly string ObjectLogSnapshotFileName = new DefaultCheckpointNamingScheme(string.Empty).ObjectLogSnapshot(Guid.Empty).fileName;

        /// <summary>
        /// Creates a creator that fails checkpoint devices under the given directory.
        /// </summary>
        /// <param name="storeCheckpointBaseDirectory">Store checkpoint base directory, as given by
        /// <c>GarnetServerOptions.StoreCheckpointBaseDirectory</c>. Every database's checkpoint directory lives
        /// beneath it, so a prefix match covers multi-database servers too.</param>
        internal FailingCheckpointDeviceFactoryCreator(string storeCheckpointBaseDirectory)
        {
            this.storeCheckpointBaseDirectory = storeCheckpointBaseDirectory;
        }

        /// <inheritdoc/>
        public INamedDeviceFactory Create(string baseName)
        {
            var factory = inner.Create(baseName);
            return baseName is not null && baseName.StartsWith(storeCheckpointBaseDirectory, StringComparison.Ordinal)
                ? new FailingNamedDeviceFactory(factory, this)
                : factory;
        }

        /// <summary>
        /// True if a device request should throw under the current failure mode.
        /// </summary>
        private bool ShouldFail(FileDescriptor fileInfo) => FailureMode switch
        {
            CheckpointDeviceFailure.All => true,
            CheckpointDeviceFailure.SnapshotLogDevicesOnly =>
                string.Equals(fileInfo.fileName, LogSnapshotFileName, StringComparison.Ordinal) ||
                string.Equals(fileInfo.fileName, ObjectLogSnapshotFileName, StringComparison.Ordinal),
            _ => false,
        };

        /// <summary>
        /// Delegating device factory that throws instead of creating a device while its owner is armed.
        /// </summary>
        private sealed class FailingNamedDeviceFactory : INamedDeviceFactory
        {
            readonly INamedDeviceFactory underlying;
            readonly FailingCheckpointDeviceFactoryCreator owner;

            internal FailingNamedDeviceFactory(INamedDeviceFactory underlying, FailingCheckpointDeviceFactoryCreator owner)
            {
                this.underlying = underlying;
                this.owner = owner;
            }

            /// <inheritdoc/>
            public IDevice Get(FileDescriptor fileInfo)
            {
                if (owner.ShouldFail(fileInfo))
                {
                    // Thrown before the underlying device is created, so a failed checkpoint leaves no open handle
                    // behind for the test's directory cleanup to trip over.
                    throw new IOException(
                        $"Simulated checkpoint device failure for {Path.Combine(fileInfo.directoryName ?? string.Empty, fileInfo.fileName ?? string.Empty)}");
                }

                return underlying.Get(fileInfo);
            }

            /// <inheritdoc/>
            public void Delete(FileDescriptor fileInfo) => underlying.Delete(fileInfo);

            /// <inheritdoc/>
            public IEnumerable<FileDescriptor> ListContents(string path) => underlying.ListContents(path);
        }
    }
}