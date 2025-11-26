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
        public long Expiration { get; set; }

        /// <inheritdoc />
        public long Size { get; set; }

        protected GarnetObjectBase(long expiration, long size)
        {
            Debug.Assert(size >= 0);
            this.Expiration = expiration;
            this.Size = size;
        }

        protected GarnetObjectBase(BinaryReader reader, long size)
            : this(expiration: reader.ReadInt64(), size: size)
        {
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
        public void CopyUpdate(ref IGarnetObject oldValue, ref IGarnetObject newValue, bool isInNewVersion)
        {
            newValue = Clone();
            newValue.Expiration = Expiration;

            // If we are not currently taking a checkpoint, we can delete the old version
            // since the new version of the object is already created.
            if (!isInNewVersion)
            {
                // Wait for any concurrent ongoing serialization of oldValue to complete
                while (true)
                {
                    if (serializationState == (int)SerializationPhase.REST && MakeTransition(SerializationPhase.REST, SerializationPhase.SERIALIZED))
                        break;

                    if (serializationState >= (int)SerializationPhase.SERIALIZED)
                        break;

                    _ = Thread.Yield();
                }
                oldValue = null;
                return;
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
        public abstract bool Operate(ref ObjectInput input, ref GarnetObjectStoreOutput output, byte respProtocolVersion, out long sizeChange);

        /// <inheritdoc />
        public abstract void Dispose();

        /// <summary>
        /// Serialize to given writer
        /// NOTE: Make sure to first call base.DoSerialize(writer) in all derived classes.
        /// </summary>
        public virtual void DoSerialize(BinaryWriter writer)
        {
            writer.Write(Expiration);
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
                    var sbPattern = input.parseState.GetArgSliceByRef(currTokenIdx++).SpanByte;
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