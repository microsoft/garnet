// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Custom object command wrapper
    /// </summary>
    class CustomObjectCommandWrapper
    {
        public readonly byte id;
        public readonly CustomObjectFactory factory;
        public int CommandId = 0;
        public readonly CustomObjectCommand[] commandMap;

        public CustomObjectCommandWrapper(byte id, CustomObjectFactory functions)
        {
            this.id = id;
            this.factory = functions;
            this.commandMap = new CustomObjectCommand[byte.MaxValue];
        }
    }
}