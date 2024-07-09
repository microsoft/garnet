// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// Base class for custom command
    /// </summary>
    public abstract class CustomCommandProc : CustomFunctions
    {
        /// <summary>
        /// Custom command implementation
        /// </summary>
        /// <param name="garnetApi"></param>
        public abstract bool Execute(IGarnetApi garnetApi, ArgSlice input, ref MemoryResult<byte> output);
    }

    class CustomCommand
    {
        private readonly string nameStr;
        public readonly byte[] Name;
        public readonly byte Id;
        public readonly CustomCommandProc CustomCommandProc;

        internal CustomCommand(string name, byte id, CustomCommandProc customCommandProc)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentNullException(nameof(name));

            if (customCommandProc == null)
                throw new ArgumentNullException(nameof(customCommandProc));

            nameStr = name.ToUpperInvariant();
            Name = System.Text.Encoding.ASCII.GetBytes(nameStr);
            Id = id;
            CustomCommandProc = customCommandProc;
        }
    }
}