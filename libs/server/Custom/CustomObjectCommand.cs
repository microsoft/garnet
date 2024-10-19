// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    class CustomObjectCommand
    {
        public readonly string NameStr;
        public readonly byte[] name;
        public readonly byte id;
        public readonly byte subid;
        public readonly CommandType type;
        public readonly CustomObjectFactory factory;
        public readonly CustomObjectFunctions functions;

        internal CustomObjectCommand(string name, byte id, byte subid, CommandType type, CustomObjectFactory factory, CustomObjectFunctions functions = null)
        {
            NameStr = name.ToUpperInvariant();
            this.name = System.Text.Encoding.ASCII.GetBytes(NameStr);
            this.id = id;
            this.subid = subid;
            this.type = type;
            this.factory = factory;
            this.functions = functions;
        }
        
        internal GarnetObjectType GetObjectType() => (GarnetObjectType)(id + CustomCommandManager.TypeIdStartOffset);
    }
}