// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;

namespace Garnet.server
{
    /// <summary>
    /// Base class for creating custom objects
    /// </summary>
    public abstract class CustomObjectFactory
    {
        /// <summary>
        /// Create new (empty) instance of custom object
        /// </summary>
        public abstract CustomObjectBase Create(byte type);

        /// <summary>
        /// Deserialize value object from given reader
        /// </summary>
        /// <param name="type"></param>
        /// <param name="reader"></param>
        public abstract CustomObjectBase Deserialize(byte type, BinaryReader reader);
    }
}