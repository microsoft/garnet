// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.IO;
using System.Text;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Serializer for IGarnetObject
    /// </summary>
    /// <remarks>Implements <see cref="IObjectSerializer{IHeapObject}"/> for Tsavorite <see cref="StoreFunctions{TKeyComparer, TRecordTriggers}"/></remarks>
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
            var firstByte = binaryReader.ReadByte();

            // 0xFC..0xFF are reserved for a future object-serialization format version/escape byte
            // (see GarnetObjectType). No current writer emits these, so encountering one means the
            // data was written by a newer version whose object format this build cannot read.
            if (firstByte >= GarnetObjectTypeExtensions.ReservedObjectFormatByteStart)
                throw new GarnetException($"Unsupported object serialization format marker 0x{firstByte:X2}; the data may have been written by a newer Garnet version.");

            var type = (GarnetObjectType)firstByte;
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
            // Built-in type ids (0..LastObjectType) are handled by the caller. A type id below the
            // fixed custom-object base therefore lies in the reserved built-in band and is not valid
            // for this build: it was written either by an older build whose custom objects were based
            // at LastObjectType+1 (rather than the fixed base), or by a newer build with additional
            // built-in types. Fail fast instead of silently returning null, which would drop the
            // record and lose data without any indication.
            if (type < CustomCommandManager.CustomTypeIdStartOffset)
                throw new GarnetException($"Unsupported object type id 0x{type:X2}; the data may have been written by an incompatible Garnet version (e.g. legacy custom objects that used a different type-id range).");

            if (!customCommandManager.TryGetCustomObjectCommand(type, out var cmd)) return null;
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