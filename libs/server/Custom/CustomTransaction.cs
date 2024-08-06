// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;

namespace Garnet.server
{
    class CustomTransaction
    {
        public readonly string NameStr;
        public readonly int NumParams;
        public readonly byte[] name;
        public readonly byte id;
        public readonly Func<CustomTransactionProcedure> proc;

        internal CustomTransaction(string name, byte id, int numParams, Func<CustomTransactionProcedure> proc)
        {
            if (name == null)
                throw new GarnetException("CustomTransaction name is null");
            NameStr = name.ToUpperInvariant();
            this.name = System.Text.Encoding.ASCII.GetBytes(NameStr);
            this.id = id;
            NumParams = numParams;
            this.proc = proc ?? throw new GarnetException("CustomTransactionProcedure is null");
        }
    }
}