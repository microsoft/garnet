// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    class CustomObjectCommand
    {
        public readonly string nameStr;
        public readonly int NumKeys;
        public readonly int NumParams;
        public readonly byte[] name;
        public readonly byte id;
        public readonly byte subid;
        public readonly CommandType type;
        public readonly CustomObjectFactory factory;

        internal CustomObjectCommand(string name, byte id, byte subid, int numKeys, int numParams, CommandType type, CustomObjectFactory factory)
        {
            nameStr = name.ToUpper();
            this.name = System.Text.Encoding.ASCII.GetBytes(nameStr);
            this.id = id;
            this.subid = subid;
            NumKeys = numKeys;
            NumParams = numParams;
            this.type = type;
            this.factory = factory;
        }

        internal RespCommand GetRespCommand() => (RespCommand)(id + CustomCommandManager.StartOffset);
    }
}