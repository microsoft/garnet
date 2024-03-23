// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.IO;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Custom object abstract base class
    /// </summary>
    public abstract class CustomObjectBase : GarnetObjectBase
    {
        /// <summary>
        /// Shared memory pool used by functions
        /// </summary>
        protected static MemoryPool<byte> MemoryPool => MemoryPool<byte>.Shared;

        /// <summary>
        /// Type of object
        /// </summary>
        readonly byte type;

        /// <summary>
        /// Base constructor
        /// </summary>
        /// <param name="type">Object type</param>
        /// <param name="size"></param>
        protected CustomObjectBase(byte type, long expiration, long size = 0)
            : base(expiration, size)
        {
            this.type = type;
        }

        protected CustomObjectBase(byte type, BinaryReader reader, long size = 0)
            : base(reader, size)
        {
            this.type = type;
        }

        /// <summary>
        /// Base copy constructor
        /// </summary>
        /// <param name="obj">Other object</param>
        protected CustomObjectBase(CustomObjectBase obj) : this(obj.type, obj.Expiration, obj.Size) { }

        /// <inheritdoc />
        public override byte Type => type;

        /// <summary>
        /// Create output as simple string, from given string
        /// </summary>
        protected static unsafe void WriteSimpleString(ref (IMemoryOwner<byte>, int) output, string simpleString)
        {
            var bytes = System.Text.Encoding.ASCII.GetBytes(simpleString);
            // Get space for simple string
            int len = 1 + bytes.Length + 2;
            output.Item1 = MemoryPool.Rent(len);
            fixed (byte* ptr = output.Item1.Memory.Span)
            {
                var curr = ptr;
                RespWriteUtils.WriteSimpleString(bytes, ref curr, ptr + len);
            }
            output.Item2 = len;
        }

        /// <summary>
        /// Create output as bulk string, from given Span
        /// </summary>
        protected static unsafe void WriteBulkString(ref (IMemoryOwner<byte>, int) output, Span<byte> bulkString)
        {
            // Get space for bulk string
            int len = RespWriteUtils.GetBulkStringLength(bulkString.Length);
            output.Item1 = MemoryPool.Rent(len);
            output.Item2 = len;
            fixed (byte* ptr = output.Item1.Memory.Span)
            {
                var curr = ptr;
                RespWriteUtils.WriteBulkString(bulkString, ref curr, ptr + len);
            }
        }

        /// <summary>
        /// Create null output as bulk string
        /// </summary>
        protected static unsafe void WriteNullBulkString(ref (IMemoryOwner<byte>, int) output)
        {
            // Get space for null bulk string "$-1\r\n"
            int len = 5;
            output.Item1 = MemoryPool.Rent(len);
            output.Item2 = len;
            fixed (byte* ptr = output.Item1.Memory.Span)
            {
                var curr = ptr;
                RespWriteUtils.WriteNull(ref curr, ptr + len);
            }
        }

        /// <summary>
        /// Create output as error message, from given string
        /// </summary>
        protected static unsafe void WriteError(ref (IMemoryOwner<byte>, int) output, string errorMessage)
        {
            var bytes = System.Text.Encoding.ASCII.GetBytes(errorMessage);
            // Get space for error
            int len = 1 + bytes.Length + 2;
            output.Item1 = MemoryPool.Rent(len);
            fixed (byte* ptr = output.Item1.Memory.Span)
            {
                var curr = ptr;
                RespWriteUtils.WriteError(bytes, ref curr, ptr + len);
            }
            output.Item2 = len;
        }

        /// <summary>
        /// Get argument from input, at specified offset (starting from 0)
        /// </summary>
        /// <param name="input">Input as ReadOnlySpan of byte</param>
        /// <param name="offset">Current offset into input</param>
        /// <returns>Argument as a span</returns>
        protected static unsafe ReadOnlySpan<byte> GetNextArg(ReadOnlySpan<byte> input, scoped ref int offset)
        {
            byte* result = null;
            int len = 0;

            fixed (byte* inputPtr = input)
            {
                byte* ptr = inputPtr + offset;
                byte* end = inputPtr + input.Length;
                if (ptr < end && RespReadUtils.ReadPtrWithLengthHeader(ref result, ref len, ref ptr, end))
                {
                    offset = (int)(ptr - inputPtr);
                    return new ReadOnlySpan<byte>(result, len);
                }
            }
            return default;
        }

        /// <summary>
        /// Get first arg from input
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        protected static ReadOnlySpan<byte> GetFirstArg(ReadOnlySpan<byte> input)
        {
            int offset = 0;
            return GetNextArg(input, ref offset);
        }

        /// <summary>
        /// Serialize to giver writer
        /// </summary>
        public abstract void SerializeObject(BinaryWriter writer);

        /// <summary>
        /// Clone object (new instance of object shell)
        /// </summary>
        public abstract CustomObjectBase CloneObject();

        /// <summary>
        /// Clone object (shallow copy)
        /// </summary>
        /// <returns></returns>
        public sealed override GarnetObjectBase Clone() => CloneObject();

        /// <inheritdoc />
        public sealed override void DoSerialize(BinaryWriter writer)
        {
            base.DoSerialize(writer);
            SerializeObject(writer);
        }

        /// <inheritdoc />
        public abstract override void Dispose();

        /// <inheritdoc />
        public abstract void Operate(byte subCommand, ReadOnlySpan<byte> input, ref (IMemoryOwner<byte>, int) output);

        /// <inheritdoc />
        public sealed override unsafe bool Operate(ref SpanByte input, ref SpanByteAndMemory output, out long sizeChange)
        {
            var header = (RespInputHeader*)input.ToPointer();
            sizeChange = 0;
            switch (header->cmd)
            {
                // Scan Command
                case RespCommand.COSCAN:
                    fixed (byte* _input = input.AsSpan())
                        if (ObjectUtils.ReadScanInput(_input, input.Length, ref output, out var cursorInput, out var pattern, out var patternLength, out int limitCount, out int bytesDone))
                        {
                            Scan(cursorInput, out var items, out var cursorOutput, count: limitCount, pattern: pattern, patternLength: patternLength);
                            ObjectUtils.WriteScanOutput(items, cursorOutput, ref output, bytesDone);
                        }
                    break;
                default:
                    (IMemoryOwner<byte> Memory, int Length) outp = (output.Memory, 0);
                    Operate(header->SubId, input.AsReadOnlySpan().Slice(RespInputHeader.Size), ref outp);
                    output.Memory = outp.Memory;
                    output.Length = outp.Length;
                    break;
            }
            return true;
        }
    }
}