// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteBase
    {
        public TsavoriteKernel Kernel;

        // The id of the partition this TsavoriteKV instance implements.
        internal ushort partitionId;

        protected ILoggerFactory loggerFactory;
        protected ILogger logger;

        /// <summary>
        /// Constructor
        /// </summary>
        public TsavoriteBase(TsavoriteKernel kernel, ushort partitionId, ILogger logger = null, ILoggerFactory loggerFactory = null)
        {
            this.Kernel = kernel;
            this.partitionId = partitionId;
            this.loggerFactory = loggerFactory;
            this.logger = logger ?? loggerFactory?.CreateLogger("TsavoriteKV");
        }

        public ushort PartitionId => partitionId;

        internal void Free()
        {
            Free(0);
            Free(1);
            Kernel.Dispose();
        }

        private static void Free(int version)
        {
        }

        /// <summary>
        /// Initialize: TODO: Left temporarily; remove
        /// </summary>
        public void Initialize(long numBuckets, int sectorSize)
        {
            Kernel = new(numBuckets, sectorSize, logger ?? loggerFactory?.CreateLogger("TsavoriteKernel"));
        }
    }
}