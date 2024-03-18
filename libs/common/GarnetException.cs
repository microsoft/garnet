// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.common
{
    /// <summary>
    /// Garnet exception base type
    /// </summary>
    public class GarnetException : Exception
    {
        /// <summary>
        /// Throw Garnet exception
        /// </summary>
        public GarnetException()
        {
        }

        /// <summary>
        /// Throw Garnet exception with message
        /// </summary>
        /// <param name="message"></param>
        public GarnetException(string message) : base(message)
        {
        }

        /// <summary>
        /// Throw Garnet exception with message and inner exception
        /// </summary>
        /// <param name="message"></param>
        /// <param name="innerException"></param>
        public GarnetException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}