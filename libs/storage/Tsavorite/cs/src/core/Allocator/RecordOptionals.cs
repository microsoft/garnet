// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Convenient means of reading and passing optional values--fields of a record that may or may not be present.
    /// </summary>
    internal struct RecordOptionals
    {
        internal long eTag;
        internal long expiration;

        internal void Initialize()
        {
            eTag = LogRecord.NoETag;
            expiration = 0;
        }
    }
}
