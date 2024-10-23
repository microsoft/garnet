// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    class CustomRawStringCommand
    {
        public readonly string NameStr;
        public readonly byte[] name;
        public readonly ushort id;
        public readonly CommandType type;
        public readonly CustomRawStringFunctions functions;
        public long expirationTicks;

        internal CustomRawStringCommand(string name, ushort id, CommandType type, CustomRawStringFunctions functions, long expirationTicks)
        {
            NameStr = name.ToUpperInvariant();
            this.name = System.Text.Encoding.ASCII.GetBytes(NameStr);
            this.id = id;
            this.type = type;
            this.functions = functions;
            this.expirationTicks = expirationTicks;
        }

        internal RespCommand GetRespCommand() => (RespCommand)(id + CustomCommandManager.StartOffset);
    }
}