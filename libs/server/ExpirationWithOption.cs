// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// A struct that holds an expiration timestamp in .NET ticks and an ExpireOption enum.
    /// Holding a timestamp in this struct will cause a conversion to coarse ticks (4-bit shift), which is a 1600ns resolution.
    /// </summary>
    internal readonly struct ExpirationWithOption
    {
        // Encoded expiration timestamp in .NET ticks and ExpireOption enum in a single long value.
        private readonly long word;

        /// <summary>
        /// Initializes a new instance of the <see cref="ExpirationWithOption"/> struct.
        /// </summary>
        /// <param name="expirationTimeInTicks">Expiration timestamp in .NET ticks</param>
        /// <param name="expireOption">Expire option</param>
        public ExpirationWithOption(long expirationTimeInTicks, ExpireOption expireOption)
        {
            var coarseTicks = expirationTimeInTicks >> 4;
            word = (coarseTicks << 4) | ((long)expireOption & 0xF);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ExpirationWithOption"/> struct.
        /// </summary>
        /// <param name="word">Long value encoding an expiration timestamp in .NET ticks and an ExpireOption enum</param>
        public ExpirationWithOption(long word)
        {
            this.word = word;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ExpirationWithOption"/> struct.
        /// </summary>
        /// <param name="wordHead">Head of long value encoding an expiration timestamp in .NET ticks and an ExpireOption enum</param>
        /// <param name="wordTail">Tail of long value encoding an expiration timestamp in .NET ticks and an ExpireOption enum</param>
        public ExpirationWithOption(int wordHead, int wordTail)
        {
            word = (long)(((ulong)(uint)wordHead << 32) | (uint)wordTail);
        }

        /// <summary>
        /// Expiration timestamp in .NET ticks
        /// </summary>
        public long ExpirationTimeInTicks => (word >> 4) << 4;

        /// <summary>
        /// Expiration option
        /// </summary>
        public ExpireOption ExpireOption => (ExpireOption)(word & 0xF);

        /// <summary>
        /// Encoded expiration timestamp in .NET ticks and ExpireOption enum
        /// </summary>
        public long Word => word;

        /// <summary>
        /// Head of encoded expiration timestamp in .NET ticks and ExpireOption enum
        /// </summary>
        public int WordHead => (int)((word >> 32) & 0xFFFFFFFF);

        /// <summary>
        /// Tail of encoded expiration timestamp in .NET ticks and ExpireOption enum
        /// </summary>
        public int WordTail => (int)(word & 0xFFFFFFFF);
    }

}