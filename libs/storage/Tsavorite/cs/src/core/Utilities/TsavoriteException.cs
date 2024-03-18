// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// Tsavorite exception base type
    /// </summary>
    public class TsavoriteException : Exception
    {
        /// <summary>
        /// Throw Tsavorite exception
        /// </summary>
        public TsavoriteException()
        {
        }

        /// <summary>
        /// Throw Tsavorite exception with message
        /// </summary>
        /// <param name="message"></param>
        public TsavoriteException(string message) : base(message)
        {
        }

        /// <summary>
        /// Throw Tsavorite exception with message and inner exception
        /// </summary>
        /// <param name="message"></param>
        /// <param name="innerException"></param>
        public TsavoriteException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Tsavorite IO exception type with message and inner exception
    /// </summary>
    public class TsavoriteIOException : TsavoriteException
    {
        /// <summary>
        /// Throw Tsavorite exception
        /// </summary>
        /// <param name="message"></param>
        /// <param name="innerException"></param>
        public TsavoriteIOException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}