﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    public class CustomObjectCommand : ICustomCommand
    {
        public byte[] Name { get; }

        public readonly string NameStr;
        public readonly byte id;
        public readonly byte subid;
        public readonly CommandType type;
        public readonly int arity;
        public readonly CustomObjectFactory factory;
        public readonly CustomObjectFunctions functions;

        internal CustomObjectCommand(string name, byte id, byte subid, CommandType type, int arity, CustomObjectFactory factory, CustomObjectFunctions functions = null)
        {
            NameStr = name.ToUpperInvariant();
            this.Name = System.Text.Encoding.ASCII.GetBytes(NameStr);
            this.id = id;
            this.subid = subid;
            this.type = type;
            this.arity = arity;
            this.factory = factory;
            this.functions = functions;
        }
    }
}