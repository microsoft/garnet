// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Custom object command wrapper
    /// </summary>
    class CustomObjectCommandWrapper
    {
        static readonly int MinMapSize = 8;

        public readonly byte id;
        public readonly CustomObjectFactory factory;
        public readonly ExtensibleCustomCommandMap<CustomObjectCommand> commandMap;

        public CustomObjectCommandWrapper(byte id, CustomObjectFactory functions)
        {
            this.id = id;
            this.factory = functions;
            this.commandMap = new ExtensibleCustomCommandMap<CustomObjectCommand>(MinMapSize, byte.MaxValue, 0, byte.MaxValue, false);
        }
    }
}