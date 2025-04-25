// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using Tsavorite.core;

namespace Garnet.server
{
    public struct ObjectSizes
    {
        /// <summary>In-memory size, including .NET object overheads</summary>
        public long Memory;

        /// <summary>Serialized size, for disk IO or other storage</summary>
        public long Disk;

        public ObjectSizes(long memory, long disk)
        {
            Memory = memory;
            Disk = disk;
        }

        [Conditional("DEBUG")]
        public void Verify() => Debug.Assert(Memory >= 0 && Disk >= 0, $"Invalid sizes [{Memory}, {Disk}]");
    }

    /// <summary>
    /// Base class for Garnet heap objects
    /// </summary>
    public abstract class GarnetObjectBase : IGarnetObject
    {
        int serializationState;
        public byte[] serialized;

        /// <inheritdoc />
        public abstract byte Type { get; }

        /// <inheritdoc />
        public long MemorySize { get => sizes.Memory; set => sizes.Memory = value; }

        /// <inheritdoc />
        public long DiskSize { get => sizes.Disk; set => sizes.Disk = value; }

        public ObjectSizes sizes;

        protected GarnetObjectBase(ObjectSizes sizes)
        {
            sizes.Verify();
            this.sizes = sizes;
        }

        protected GarnetObjectBase(BinaryReader reader, ObjectSizes sizes)
            : this(sizes)
        {
            // Add anything here that should match DoSerialize()
        }

        /// <inheritdoc />
        public void Serialize(BinaryWriter writer)
        {
            while (true)
            {
                if (serializationState == (int)SerializationPhase.REST && MakeTransition(SerializationPhase.REST, SerializationPhase.SERIALIZING))
                {
                    // Directly serialize to wire, do not cache serialized state
                    writer.Write(Type);
                    DoSerialize(writer);
                    serializationState = (int)SerializationPhase.REST;
                    return;
                }

                if (serializationState == (int)SerializationPhase.SERIALIZED)
                {
                    // If serialized state is cached, use that
                    var _serialized = serialized;
                    if (_serialized != null)
                    {
                        writer.Write(Type);
                        writer.Write(_serialized);
                    }
                    else
                    {
                        // Write null object to stream
                        writer.Write((byte)GarnetObjectType.Null);
                    }
                    return;
                }

                Thread.Yield();
            }
        }

        /// <inheritdoc />
        public IGarnetObject CopyUpdate(bool isInNewVersion, ref RMWInfo rmwInfo)
        {
            var newValue = Clone();

            // If we are not currently taking a checkpoint, we can delete the old version
            // since the new version of the object is already created.
            if (!isInNewVersion)
            {
                rmwInfo.ClearSourceValueObject = true;
                return newValue;
            }

            // Create a serialized version for checkpoint version (v)
            while (true)
            {
                if (serializationState == (int)SerializationPhase.REST && MakeTransition(SerializationPhase.REST, SerializationPhase.SERIALIZING))
                {
                    using var ms = new MemoryStream();
                    using var writer = new BinaryWriter(ms, Encoding.UTF8);
                    DoSerialize(writer);
                    serialized = ms.ToArray();

                    serializationState = (int)SerializationPhase.SERIALIZED;
                    break;
                }

                if (serializationState >= (int)SerializationPhase.SERIALIZED)
                    break;

                _ = Thread.Yield();
            }

            return newValue;
        }

        /// <summary>
        /// Clone object (shallow copy)
        /// </summary>
        /// <returns></returns>
        public abstract GarnetObjectBase Clone();

        /// <inheritdoc />
        public abstract bool Operate(ref ObjectInput input, ref GarnetObjectStoreOutput output, out long sizeChange);

        /// <inheritdoc />
        public abstract void Dispose();

        /// <summary>
        /// Serialize to given writer
        /// NOTE: Make sure to first call base.DoSerialize(writer) in all derived classes.
        /// </summary>
        public virtual void DoSerialize(BinaryWriter writer)
        {
            // Add anything here that needs to be in front of the derived object data
        }

        private bool MakeTransition(SerializationPhase expectedPhase, SerializationPhase nextPhase)
        {
            if (Interlocked.CompareExchange(ref serializationState, (int)nextPhase, (int)expectedPhase) != (int)expectedPhase) return false;
            return true;
        }

        /// <summary>
        /// Scan the items of the collection
        /// </summary>
        /// <param name="start">Shift the scan to this index</param>
        /// <param name="items">The matching items in the collection</param>
        /// <param name="cursor">The cursor in the current page</param>
        /// <param name="count">The number of items being taken in one iteration</param>
        /// <param name="pattern">A patter used to match the members of the collection</param>
        /// <param name="patternLength">The number of characters in the pattern</param>
        /// <returns></returns>
        public abstract unsafe void Scan(long start, out List<byte[]> items, out long cursor, int count = 10, byte* pattern = default, int patternLength = 0, bool isNoValue = false);
    }
}