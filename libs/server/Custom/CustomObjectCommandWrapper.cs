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
        static readonly byte MaxSubId = 255; // SubId now occupies its own header byte (RespInputHeader.subId), so it may use the full byte range

        public readonly byte id;
        public readonly CustomObjectFactory factory;
        public ExpandableMap<CustomObjectCommand> commandMap;

        public CustomObjectCommandWrapper(byte id, CustomObjectFactory functions)
        {
            this.id = id;
            this.factory = functions;
            this.commandMap = new ExpandableMap<CustomObjectCommand>(MinMapSize, 0, MaxSubId);
        }
    }
}