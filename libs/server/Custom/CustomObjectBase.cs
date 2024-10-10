// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
using System.IO;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Custom object abstract base class
    /// </summary>
    public abstract class CustomObjectBase : GarnetObjectBase
    {
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
        public sealed override unsafe bool Operate(ref ObjectInput input, ref SpanByteAndMemory output, out long sizeChange, out bool removeKey)
        {
            sizeChange = 0;
            removeKey = false;

            switch (input.header.cmd)
            {
                // Scan Command
                case RespCommand.COSCAN:
                    if (ObjectUtils.ReadScanInput(ref input, ref output, out var cursorInput, out var pattern,
                            out var patternLength, out var limitCount, out bool _, out var error))
                    {
                        Scan(cursorInput, out var items, out var cursorOutput, count: limitCount, pattern: pattern,
                            patternLength: patternLength);
                        ObjectUtils.WriteScanOutput(items, cursorOutput, ref output);
                    }
                    else
                    {
                        ObjectUtils.WriteScanError(error, ref output);
                    }
                    break;
                default:
                    if ((byte)input.header.type != this.type)
                    {
                        // Indicates an incorrect type of key
                        output.Length = 0;
                        return true;
                    }
                    break;
            }

            return true;
        }
    }
}