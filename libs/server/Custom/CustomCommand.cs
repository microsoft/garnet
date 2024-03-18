// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    class CustomCommand
    {
        public readonly string nameStr;
        public readonly int NumKeys;
        public readonly int NumParams;
        public readonly byte[] name;
        public readonly byte id;
        public readonly CommandType type;
        public readonly CustomRawStringFunctions functions;
        public long expirationTicks;

        internal CustomCommand(string name, byte id, int numKeys, int numParams, CommandType type, CustomRawStringFunctions functions, long expirationTicks)
        {
            nameStr = name.ToUpper();
            this.name = System.Text.Encoding.ASCII.GetBytes(nameStr);
            this.id = id;
            NumKeys = numKeys;
            NumParams = numParams;
            this.type = type;
            this.functions = functions;
            this.expirationTicks = expirationTicks;
        }

        internal RespCommand GetRespCommand() => (RespCommand)(id + CustomCommandManager.StartOffset);
    }
}