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
        static readonly byte MaxSubId = 31; // RespInputHeader uses the 3 MSBs of SubId, so SubId must fit in the 5 LSBs

        public readonly byte id;
        public readonly CustomObjectFactory factory;
        public ExtensibleMap<CustomObjectCommand> commandMap;

        public CustomObjectCommandWrapper(byte id, CustomObjectFactory functions)
        {
            this.id = id;
            this.factory = functions;
            this.commandMap = new ExtensibleMap<CustomObjectCommand>(MinMapSize, 0, MaxSubId);
        }
    }
}