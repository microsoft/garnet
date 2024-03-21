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
            var type = (GarnetObjectType)reader.ReadByte();
            obj = type switch
            {
                GarnetObjectType.SortedSet => new SortedSetObject(reader),
                GarnetObjectType.List => new ListObject(reader),
                GarnetObjectType.Hash => new HashObject(reader),
                GarnetObjectType.Set => new SetObject(reader),
                _ => CustomDeserialize((byte)type),
            };
        }

        private IGarnetObject CustomDeserialize(byte type)
        {
            if (type < CustomCommandManager.StartOffset) return null;
            return customCommands[type - CustomCommandManager.StartOffset].factory.Deserialize(type, reader);
        }

        /// <inheritdoc />
        public override void Serialize(ref IGarnetObject obj)
        {
            writer.Write(obj.Type);
            obj.Serialize(writer);
        }
    }
}