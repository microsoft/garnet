// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;

namespace Garnet.server.Objects.Types
{
    internal static class RespMemoryWriterExtensions
    {
        internal static void WriteNullWithEtagIfNeeded(this ref RespMemoryWriter writer, RespMetaCommand metaCmd, long etag)
        {
            writer.WriteEtagIfNeeded(metaCmd, etag);
            writer.WriteNull();
        }

        internal static void WriteDoubleNumericWithEtagIfNeeded(this ref RespMemoryWriter writer, double value, RespMetaCommand metaCmd, long etag)
        {
            writer.WriteEtagIfNeeded(metaCmd, etag);
            writer.WriteDoubleNumeric(value);
        }

        internal static void WriteInt32WithEtagIfNeeded(this ref RespMemoryWriter writer, int value, RespMetaCommand metaCmd, long etag)
        {
            writer.WriteEtagIfNeeded(metaCmd, etag);
            writer.WriteInt32(value);
        }

        private static void WriteEtagIfNeeded(this ref RespMemoryWriter writer, RespMetaCommand metaCmd, long etag)
        {
            if (!metaCmd.IsEtagCommand())
                return;

            writer.WriteArrayLength(2);
            writer.WriteInt64(etag);
        }
    }
}