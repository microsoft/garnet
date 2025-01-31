﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    public class CustomRawStringCommand : ICustomCommand
    {
        public byte[] Name { get; }

        public readonly string NameStr;
        public readonly ushort id;
        public readonly CommandType type;
        public readonly int arity;
        public readonly CustomRawStringFunctions functions;
        public long expirationTicks;

        internal CustomRawStringCommand(string name, ushort id, CommandType type, int arity, CustomRawStringFunctions functions, long expirationTicks)
        {
            NameStr = name.ToUpperInvariant();
            this.Name = System.Text.Encoding.ASCII.GetBytes(NameStr);
            this.id = id;
            this.type = type;
            this.arity = arity;
            this.functions = functions;
            this.expirationTicks = expirationTicks;
        }
    }
}