// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// Base class for custom command
    /// </summary>
    public abstract class CustomScriptProc : CustomFunctions
    {
        /// <summary>
        /// Custom command implementation
        /// </summary>
        /// <param name="garnetApi"></param>
        public abstract bool Execute(IGarnetApi garnetApi, ArgSlice input, ref MemoryResult<byte> output);
    }

    class CustomScript
    {
        private readonly string nameStr;
        public readonly byte[] Name;
        public readonly byte Id;
        public readonly CustomScriptProc CustomScriptProc;

        internal CustomScript(string name, byte id, CustomScriptProc customScriptProc)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentNullException(nameof(name));

            if (customScriptProc == null)
                throw new ArgumentNullException(nameof(customScriptProc));

            nameStr = name.ToUpperInvariant();
            Name = System.Text.Encoding.ASCII.GetBytes(nameStr);
            Id = id;
            CustomScriptProc = customScriptProc;
        }
    }
}