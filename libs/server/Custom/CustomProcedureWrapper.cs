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

    class CustomProcedureWrapper : ICustomCommand
    {
        public byte[] Name { get; }

        public readonly string NameStr;
        public readonly byte Id;
        public readonly Func<CustomProcedure> CustomProcedureFactory;

        internal CustomProcedureWrapper(string name, byte id, Func<CustomProcedure> customProcedureFactory, CustomCommandManager customCommandManager)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentNullException(nameof(name));

            if (customProcedureFactory == null)
                throw new ArgumentNullException(nameof(customProcedureFactory));

            Debug.Assert(customCommandManager != null);

            NameStr = name.ToUpperInvariant();
            Name = System.Text.Encoding.ASCII.GetBytes(NameStr);
            Id = id;
            CustomProcedureFactory = customProcedureFactory;
        }
    }
}