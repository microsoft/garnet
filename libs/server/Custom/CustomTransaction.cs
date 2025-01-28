// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;

namespace Garnet.server
{
    class CustomTransaction : ICustomCommand
    {
        public byte[] Name { get; }

        public readonly string NameStr;
        public readonly byte id;
        public readonly int arity;
        public readonly Func<CustomTransactionProcedure> proc;

        internal CustomTransaction(string name, byte id, int arity, Func<CustomTransactionProcedure> proc)
        {
            if (name == null)
                throw new GarnetException("CustomTransaction name is null");
            NameStr = name.ToUpperInvariant();
            this.Name = System.Text.Encoding.ASCII.GetBytes(NameStr);
            this.id = id;
            this.arity = arity;
            this.proc = proc ?? throw new GarnetException("CustomTransactionProcedure is null");
        }
    }
}