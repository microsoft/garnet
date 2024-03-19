// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Serializer for IGarnetObject
    /// </summary>
    public sealed class GarnetObjectSerializer : BinaryObjectSerializer<IGarnetObject>
    {
        readonly CustomObjectCommandWrapper[] customCommands;

        /// <summary>
        /// Constructor
        /// </summary>
        public GarnetObjectSerializer(CustomCommandManager customCommandManager)
        {
            this.customCommands = customCommandManager.objectCommandMap;
        }

        /// <inheritdoc />
        public override void Deserialize(out IGarnetObject obj)
        {
            var expiration = reader.ReadInt64();
            var type = (GarnetObjectType)reader.ReadByte();
            obj = type switch
            {
                GarnetObjectType.SortedSet => new SortedSetObject(reader, expiration),
                GarnetObjectType.List => new ListObject(reader, expiration),
                GarnetObjectType.Hash => new HashObject(reader, expiration),
                GarnetObjectType.Set => new SetObject(reader, expiration),
                _ => CustomDeserialize((byte)type, expiration),
            };
        }

        private IGarnetObject CustomDeserialize(byte type, long expiration)
        {
            if (type < CustomCommandManager.StartOffset) return null;
            return customCommands[type - CustomCommandManager.StartOffset].factory.Deserialize(type, expiration, reader);
        }

        /// <inheritdoc />
        public override void Serialize(ref IGarnetObject obj) => obj.Serialize(writer);
    }
}