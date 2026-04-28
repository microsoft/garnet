// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal interface ICheckpointDataSource
    {
        bool HasNextChunk { get; }
    }

    internal class CheckpointDataSource : ICheckpointDataSource
    {
        public CheckpointFileType type;
        public Guid token;
        public IDevice device;
        public long startOffset;
        public long currOffset;
        public long endOffset;

        public bool HasNextChunk
            => currOffset < endOffset;

        public long GetNextChunkBytes(int maxChunkSize)
            => Math.Min(endOffset - currOffset, maxChunkSize);
    }

    interface ICheckpointReader
    {
        IEnumerable<CheckpointDataSource> GetNextDataSource();
    }

    internal sealed class CheckpointTranmissionDriver : IDisposable
    {
        readonly CheckpointReadContext checkpointReadContext;
        readonly ICheckpointReader[] checkpointReaders;
        readonly ILogger logger;

        public CheckpointTranmissionDriver(ICheckpointReader[] checkpointReaders, CheckpointReadContext checkpointReadContext, ILogger logger)
        {
            this.checkpointReaders = checkpointReaders;
            this.checkpointReadContext = checkpointReadContext;
            this.logger = logger;
        }

        public void Dispose()
        {
            checkpointReadContext.Dispose();
        }

        public async Task SendCheckpoint()
        {
            foreach(var checkpointReader in checkpointReaders)
            {
                foreach (var cds in checkpointReader.GetNextDataSource())
                {
                    try
                    {                        
                        await checkpointReadContext.SendSegments(cds);
                    }
                    finally
                    {
                        cds.device.Dispose();
                    }
                }
            }
        }
    }
}
