﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Callback functions for main store
    /// </summary>
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long>
    {
        /// <inheritdoc />
        public bool SingleDeleter(ref SpanByte key, ref SpanByte value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo)
        {
            if (recordInfo.VectorSet && value.AsReadOnlySpan().ContainsAnyExcept((byte)0))
            {
                // Implies this is a vector set, needs special handling
                //
                // Will call back in after a drop with an all 0 value
                deleteInfo.Action = DeleteAction.CancelOperation;
                return false;
            }

            recordInfo.ClearHasETag();
            functionsState.watchVersionMap.IncrementVersion(deleteInfo.KeyHash);
            return true;
        }

        /// <inheritdoc />
        public void PostSingleDeleter(ref SpanByte key, ref DeleteInfo deleteInfo)
        {
            if (functionsState.appendOnlyFile != null)
                WriteLogDelete(ref key, deleteInfo.Version, deleteInfo.SessionID);
        }

        /// <inheritdoc />
        public bool ConcurrentDeleter(ref SpanByte key, ref SpanByte value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo)
        {
            if (recordInfo.VectorSet && value.AsReadOnlySpan().ContainsAnyExcept((byte)0))
            {
                // Implies this is a vector set, needs special handling
                //
                // Will call back in after a drop with an all 0 value
                deleteInfo.Action = DeleteAction.CancelOperation;
                return false;
            }

            recordInfo.ClearHasETag();
            if (!deleteInfo.RecordInfo.Modified)
                functionsState.watchVersionMap.IncrementVersion(deleteInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                WriteLogDelete(ref key, deleteInfo.Version, deleteInfo.SessionID);
            return true;
        }
    }
}