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
    /// <summary>
    /// Base class for Garnet heap objects
    /// </summary>
    public abstract class GarnetObjectBase : IGarnetObject
    {
        int serializationState;
        byte[] serialized;

        /// <inheritdoc />
        public long Expiration { get; set; }

        /// <inheritdoc />
        public long Size { get; set; }

        /// <inheritdoc />
        public void Serialize(BinaryWriter writer)
        {
            while (true)
            {
                if (serializationState == (int)SerializationPhase.REST && MakeTransition(SerializationPhase.REST, SerializationPhase.SERIALIZING))
                {
                    // Directly serialize to wire, do not cache serialized state
                    DoSerialize(writer);
                    serializationState = (int)SerializationPhase.REST;
                    return;
                }

                if (serializationState == (int)SerializationPhase.SERIALIZED)
                {
                    // If serialized state is cached, use that
                    if (serialized != null)
                    {
                        writer.Write(serialized);
                        // We can safely delete the serialized image now, as there is
                        // guaranteed to be a new v+1 image of the object in memory.
                        // Because "serialized" was created during PostCopyUpdater
                        serialized = null;
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
        public void CopyUpdate(ref IGarnetObject newValue)
        {
            newValue = Clone();
            newValue.Expiration = Expiration;

            while (true)
            {
                if (serializationState == (int)SerializationPhase.REST && MakeTransition(SerializationPhase.REST, SerializationPhase.SERIALIZING))
                {
                    using (var ms = new MemoryStream())
                    {
                        using var writer = new BinaryWriter(ms, new UTF8Encoding(), true);
                        DoSerialize(writer);
                        serialized = ms.ToArray();
                    }
                    serializationState = (int)SerializationPhase.SERIALIZED;
                    return;
                }

                if (serializationState >= (int)SerializationPhase.SERIALIZED)
                    return;

                Thread.Yield();
            }
        }

        /// <summary>
        /// Clone object (shallow copy)
        /// </summary>
        /// <returns></returns>
        public abstract GarnetObjectBase Clone();

        /// <inheritdoc />
        public abstract bool Operate(ref SpanByte input, ref SpanByteAndMemory output, out long sizeChange);

        /// <inheritdoc />
        public abstract void Dispose();

        /// <summary>
        /// Serialize to given writer
        /// </summary>
        public abstract void DoSerialize(BinaryWriter writer);

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
        public abstract unsafe void Scan(long start, out List<byte[]> items, out long cursor, int count = 10, byte* pattern = default, int patternLength = 0);
    }
}