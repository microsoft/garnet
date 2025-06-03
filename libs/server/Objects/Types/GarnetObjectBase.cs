// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using Garnet.common;
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
            Disk = disk + sizeof(byte); // Additional byte for GarnetObjectBase.Type
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
                // This is probably called from Flush, including for checkpoints. If CopyUpdate() has already serialized the object, we will use that
                // serialized state. Otherwise, we will serialize the object directly to the writer, and not create the serialized byte[]; only
                // CopyUpdater does that, as it must ensure the object's (v1) data is not changed during the checkpoint.
                if (serializationState == (int)SerializationPhase.REST && MakeTransition(SerializationPhase.REST, SerializationPhase.SERIALIZING))
                {
                    // Directly serialize to wire, do not cache serialized state
                    writer.Write(Type);
                    DoSerialize(writer);
                    serializationState = (int)SerializationPhase.REST;
                    return;
                }

                // If we are here, serializationState is one of the .SERIALIZ* states. This means that either:
                // - Another thread is currently serializing this object (e.g. checkpoint and eviction)
                // - CopyUpdate() is serializing this object

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
            // Note that this does a shallow copy of the object's internal structures (e.g. List<>), which means subsequent modifications of newValue
            // in the (v+1) version of the record will modify data seen from the 'this' in the (v) record. Normally this is OK because the (v) version
            // of the record is not reachable once the (v+1) version is inserted, but if a checkpoint is ongoing, the (v) version is part of that.
            var newValue = Clone();

            // If we are not currently taking a checkpoint, we can delete the old version since the new version of the object is already created
            // in the new record, which has been inserted (because this CopyUpdate() is called from PostCopyUpdater() after a successful CAS).
            if (!isInNewVersion)
            {
                rmwInfo.ClearSourceValueObject = true;
                return newValue;
            }

            // Create a serialized version for checkpoint version (v). This is only done for CopyUpdate during a checkpoint, to preserve the (v) data
            // of the object during a checkpoint while the (v+1) version of the record may modify the shallow-copied internal structures.
            while (true)
            {
                if (serializationState == (int)SerializationPhase.REST && MakeTransition(SerializationPhase.REST, SerializationPhase.SERIALIZING))
                {
                    using var ms = new MemoryStream();
                    using var writer = new BinaryWriter(ms, Encoding.UTF8);
                    DoSerialize(writer);
                    serialized = ms.ToArray();

                    serializationState = (int)SerializationPhase.SERIALIZED;    // This is the only place .SERIALIZED is set
                    break;
                }

                // If we're here, serializationState is one of the .SERIALIZ* states. CopyUpdate has a lock on the tag chain, so no other thread will
                // be running CopyUpdate. Therefore there are two possibilities:
                // 1. CopyUpdate has been called before and the state is .SERIALIZED and '_serialized' is created. We're done.
                // 2. Serialize() is running (likely in a Flush()) and the state is .SERIALIZING. We will Yield and loop to wait for it to finish.

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
        public abstract bool Operate(ref ObjectInput input, ref GarnetObjectStoreOutput output, byte respProtocolVersion, out long sizeChange);

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

        /// <summary>
        /// Implement Scan command
        /// </summary>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="respProtocolVersion"></param>
        protected unsafe void Scan(ref ObjectInput input, ref GarnetObjectStoreOutput output, byte respProtocolVersion)
        {
            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

            if (ReadScanInput(ref input, ref output.SpanByteAndMemory, out var cursorInput, out var pattern,
                              out var patternLength, out var limitCount, out var isNoValue, out var error))
            {
                Scan(cursorInput, out var items, out var cursorOutput, limitCount, pattern,
                     patternLength, isNoValue);

                writer.WriteArrayLength(2);
                writer.WriteInt64AsBulkString(cursorOutput);

                if (items.Count == 0)
                {
                    // Empty array
                    writer.WriteEmptyArray();
                }
                else
                {
                    // Write size of the array
                    writer.WriteArrayLength(items.Count);

                    foreach (var item in items)
                    {
                        if (item != null)
                        {
                            writer.WriteBulkString(item);
                        }
                        else
                        {
                            writer.WriteNull();
                        }
                    }
                }

                output.Header.result1 = items.Count;
            }
            else
            {
                writer.WriteError(error);
            }
        }

        /// <summary>
        /// Reads and parses scan parameters from RESP format
        /// </summary>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="cursorInput"></param>
        /// <param name="pattern"></param>
        /// <param name="patternLength"></param>
        /// <param name="countInInput"></param>
        /// <param name="error"></param>
        /// <returns></returns>
        private static unsafe bool ReadScanInput(ref ObjectInput input, ref SpanByteAndMemory output,
            out long cursorInput, out byte* pattern, out int patternLength, out int countInInput, out bool isNoValue, out ReadOnlySpan<byte> error)
        {
            // Largest number of items to print
            var limitCountInOutput = input.arg2;

            patternLength = 0;
            pattern = default;

            // Default of items in output
            countInInput = 10;

            error = default;
            isNoValue = false;

            // Cursor
            if (!input.parseState.TryGetLong(0, out cursorInput) || cursorInput < 0)
            {
                error = CmdStrings.RESP_ERR_GENERIC_INVALIDCURSOR;
                return false;
            }

            var currTokenIdx = 1;

            while (currTokenIdx < input.parseState.Count)
            {
                var sbParam = input.parseState.GetArgSliceByRef(currTokenIdx++).ReadOnlySpan;

                if (sbParam.EqualsUpperCaseSpanIgnoringCase(CmdStrings.MATCH))
                {
                    // Read pattern for keys filter
                    var sbPattern = input.parseState.GetArgSliceByRef(currTokenIdx++);
                    pattern = sbPattern.ToPointer();
                    patternLength = sbPattern.Length;
                }
                else if (sbParam.EqualsUpperCaseSpanIgnoringCase(CmdStrings.COUNT))
                {
                    if (!input.parseState.TryGetInt(currTokenIdx++, out countInInput))
                    {
                        error = CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER;
                        return false;
                    }

                    // Limiting number of items to send to the output
                    if (countInInput > limitCountInOutput)
                        countInInput = limitCountInOutput;
                }
                else if (sbParam.EqualsUpperCaseSpanIgnoringCase(CmdStrings.NOVALUES))
                {
                    isNoValue = true;
                }
            }

            return true;
        }
    }
}