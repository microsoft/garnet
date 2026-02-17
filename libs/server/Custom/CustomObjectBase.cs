// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
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
        /// Type of object
        /// </summary>
        readonly byte type;

        /// <summary>
        /// Base constructor
        /// </summary>
        /// <param name="type">Object type</param>
        /// <param name="heapMemorySize"></param>
        protected CustomObjectBase(byte type, long heapMemorySize = 0)
            : base(heapMemorySize)
        {
            this.type = type;
        }

        /// <summary>
        /// Base constructor
        /// </summary>
        /// <param name="type">Object type</param>
        /// <param name="reader"></param>
        /// <param name="heapMemorySize"></param>
        protected CustomObjectBase(byte type, BinaryReader reader, long heapMemorySize = 0)
            : base(reader, heapMemorySize)
        {
            this.type = type;
        }

        /// <summary>
        /// Base copy constructor
        /// </summary>
        /// <param name="obj">Other object</param>
        protected CustomObjectBase(CustomObjectBase obj) : this(obj.type) { }

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
        public sealed override IHeapObject Clone() => CloneObject();

        /// <inheritdoc />
        public sealed override void DoSerialize(BinaryWriter writer)
        {
            base.DoSerialize(writer);
            SerializeObject(writer);
        }

        /// <inheritdoc />
        public abstract override void Dispose();

        /// <inheritdoc />
        public sealed override bool Operate(ref ObjectInput input, ref ObjectOutput output,
                                            ref RespMemoryWriter writer, out long sizeChange)
        {
            sizeChange = 0;

            switch (input.header.cmd)
            {
                // Scan Command
                case RespCommand.COSCAN:
                    Scan(ref input, ref output, ref writer);
                    break;
                default:
                    if ((byte)input.header.type != this.type)
                    {
                        // Indicates an incorrect type of key
                        output.OutputFlags |= ObjectOutputFlags.WrongType;
                        output.SpanByteAndMemory.Length = 0;
                        return true;
                    }
                    break;
            }

            return true;
        }
    }
}