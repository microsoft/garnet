// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using Tsavorite.core;

namespace Garnet.test
{
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
        /// While true, creating any store checkpoint device throws. Read on every device request, so a failure can be
        /// armed and disarmed against a running server.
        /// </summary>
        internal volatile bool ArmCheckpointFailure;

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
                if (owner.ArmCheckpointFailure)
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