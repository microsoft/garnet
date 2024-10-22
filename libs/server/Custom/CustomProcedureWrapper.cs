// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
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
        /// <param name="procInput"></param>
        /// <param name="output"></param>
        public abstract bool Execute<TGarnetApi>(TGarnetApi garnetApi, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
            where TGarnetApi : IGarnetApi;
    }

    class CustomProcedureWrapper
    {
        public readonly string NameStr;
        public readonly byte[] Name;
        public readonly byte Id;
        public readonly Func<CustomProcedure> CustomProcedure;

        internal CustomProcedureWrapper(string name, byte id, Func<CustomProcedure> customProcedure, CustomCommandManager customCommandManager)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentNullException(nameof(name));

            if (customProcedure == null)
                throw new ArgumentNullException(nameof(customProcedure));

            Debug.Assert(customCommandManager != null);

            NameStr = name.ToUpperInvariant();
            Name = System.Text.Encoding.ASCII.GetBytes(NameStr);
            Id = id;
            CustomProcedure = customProcedure;
        }
    }
}