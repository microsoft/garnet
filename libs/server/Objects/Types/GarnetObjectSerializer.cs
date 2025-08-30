// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.IO;
using System.Text;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Serializer for IGarnetObject
    /// </summary>
    /// <remarks>Implements <see cref="IObjectSerializer{IHeapObject}"/> for Tsavorite <see cref="StoreFunctions{TKeyComparer, TRecordDisposer}"/></remarks>
    public sealed class GarnetObjectSerializer : BinaryObjectSerializer<IGarnetObject>, IObjectSerializer<IHeapObject>
    {
        readonly CustomCommandManager customCommandManager;

        /// <summary>
        /// Constructor
        /// </summary>
        public GarnetObjectSerializer(CustomCommandManager customCommandManager)
        {
            this.customCommandManager = customCommandManager;
        }

        /// <inheritdoc />
        public override void Deserialize(out IGarnetObject obj)
        {
            obj = DeserializeInternal(base.reader);
        }

        /// <inheritdoc />
        public void Deserialize(out IHeapObject obj)
        {
            obj = DeserializeInternal(base.reader);
        }

        /// <summary>Thread-safe version of Deserialize</summary>
        /// <param name="data"></param>
        /// <returns></returns>
        public IGarnetObject Deserialize(byte[] data)
        {
            Debug.Assert(data != null);

            using var ms = new MemoryStream(data);
            using var binaryReader = new BinaryReader(ms, Encoding.UTF8);
            return DeserializeInternal(binaryReader);
        }

        private IGarnetObject DeserializeInternal(BinaryReader binaryReader)
        {
            var type = (GarnetObjectType)binaryReader.ReadByte();
            var obj = type switch
            {
                GarnetObjectType.Null => null,
                GarnetObjectType.SortedSet => new SortedSetObject(binaryReader),
                GarnetObjectType.List => new ListObject(binaryReader),
                GarnetObjectType.Hash => new HashObject(binaryReader),
                GarnetObjectType.Set => new SetObject(binaryReader),
                _ => CustomDeserialize((byte)type, binaryReader),
            };
            return obj;
        }

        private IGarnetObject CustomDeserialize(byte type, BinaryReader binaryReader)
        {
            if (type < CustomCommandManager.CustomTypeIdStartOffset ||
                !customCommandManager.TryGetCustomObjectCommand(type, out var cmd)) return null;
            return cmd.factory.Deserialize(type, binaryReader);
        }

        /// <inheritdoc />
        public override void Serialize(IGarnetObject obj) => SerializeInternal(base.writer, obj);

        /// <summary>Thread safe version of Serialize.</summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static void Serialize(IGarnetObject obj, out byte[] bytes)
        {
            Debug.Assert(obj != null);

            using var ms = new MemoryStream();
            using var binaryWriter = new BinaryWriter(ms, Encoding.UTF8);
            SerializeInternal(binaryWriter, obj);
            bytes = ms.ToArray();
        }

        /// <inheritdoc />
        public void Serialize(IHeapObject obj) => SerializeInternal(base.writer, (IGarnetObject)obj);

        private static void SerializeInternal(BinaryWriter binaryWriter, IGarnetObject obj)
        {
            if (obj == null)
                binaryWriter.Write((byte)GarnetObjectType.Null);
            else
                obj.Serialize(binaryWriter);
        }
    }
}