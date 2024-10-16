// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// Base class for custom command
    /// </summary>
    public abstract class CustomProcedure : CustomFunctions
    {
        /// <summary>
        /// Custom command implementation
        /// </summary>
        /// <param name="garnetApi"></param>
        public abstract bool Execute<TGarnetApi>(TGarnetApi garnetApi, ArgSlice input, ref MemoryResult<byte> output)
            where TGarnetApi : IGarnetApi;
    }

    class CustomProcedureWrapper
    {
        public readonly string NameStr;
        public readonly byte[] Name;
        public readonly byte Id;
        public readonly CustomProcedure CustomProcedureImpl;

        internal CustomProcedureWrapper(string name, byte id, CustomProcedure customScriptProc)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentNullException(nameof(name));

            if (customScriptProc == null)
                throw new ArgumentNullException(nameof(customScriptProc));

            NameStr = name.ToUpperInvariant();
            Name = System.Text.Encoding.ASCII.GetBytes(NameStr);
            Id = id;
            CustomProcedureImpl = customScriptProc;
        }
    }
}