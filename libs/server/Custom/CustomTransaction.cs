// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.server
{
    class CustomTransaction
    {
        public readonly string nameStr;
        public readonly int NumParams;
        public readonly byte[] name;
        public readonly byte id;
        public readonly Func<CustomTransactionProcedure> proc;

        internal CustomTransaction(string name, byte id, int numParams, Func<CustomTransactionProcedure> proc)
        {
            nameStr = name.ToUpper();
            this.name = System.Text.Encoding.ASCII.GetBytes(nameStr);
            this.id = id;
            NumParams = numParams;
            this.proc = proc;
        }
    }
}