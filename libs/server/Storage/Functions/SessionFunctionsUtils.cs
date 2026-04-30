// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    internal static class SessionFunctionsUtils
    {
        internal enum IPUResult : byte
        {
            Failed = 0,
            Succeeded,
            NotUpdated,
        }

        /// <summary>
        /// Attempts to set the expiration time on a log record based on the specified <see cref="ExpireOption"/>.
        /// </summary>
        /// <param name="logRecord">The log record to update.</param>
        /// <param name="optionType">The expiration option that determines how the expiration should be set.</param>
        /// <param name="newExpiry">The new expiration value to set.</param>
        /// <param name="logErrorOnFail">True if method should log an error when failed to set expiration.</param>
        /// <param name="logger">The logger for error reporting.</param>
        /// <param name="expirationChanged">Set to true if the expiration was changed; otherwise, false.</param>
        /// <returns>True if the expiration was set or the operation was valid for the given option; otherwise, false.</returns>
        internal static bool EvaluateExpire(ref LogRecord logRecord, ExpireOption optionType, long newExpiry, bool hasExpiration, bool logErrorOnFail, ILogger logger, out bool expirationChanged)
        {
            expirationChanged = false;

            if (hasExpiration)
            {
                // Expiration already exists so there is no need to check for space (i.e. failure of TrySetExpiration)
                switch (optionType)
                {
                    case ExpireOption.NX:
                        return true;
                    case ExpireOption.XX:
                    case ExpireOption.None:
                        _ = logRecord.TrySetExpiration(newExpiry);
                        expirationChanged = true;
                        return true;
                    case ExpireOption.GT:
                    case ExpireOption.XXGT:
                        if (newExpiry > logRecord.Expiration)
                        {
                            _ = logRecord.TrySetExpiration(newExpiry);
                            expirationChanged = true;
                        }
                        return true;
                    case ExpireOption.LT:
                    case ExpireOption.XXLT:
                        if (newExpiry < logRecord.Expiration)
                        {
                            _ = logRecord.TrySetExpiration(newExpiry);
                            expirationChanged = true;
                        }
                        return true;
                    default:
                        throw new GarnetException($"{nameof(EvaluateExpire)} exception when HasExpiration is true. optionType: {optionType}");
                }
            }

            // No expiration yet.
            switch (optionType)
            {
                case ExpireOption.NX:
                case ExpireOption.None:
                case ExpireOption.LT: // If expiry doesn't exist, LT should treat the current expiration as infinite, so the new value must be less
                    var isSuccessful = logRecord.TrySetExpiration(newExpiry);
                    if (!isSuccessful && logErrorOnFail)
                    {
                        logger?.LogError("Failed to add expiration in {methodName}.{caseName}", nameof(EvaluateExpire), optionType);
                        return false;
                    }
                    expirationChanged = isSuccessful;
                    return isSuccessful;
                case ExpireOption.XX:
                case ExpireOption.GT:
                case ExpireOption.XXGT:
                case ExpireOption.XXLT:
                    return true;
                default:
                    throw new GarnetException($"{nameof(EvaluateExpire)} exception when HasExpiration is false. optionType: {optionType}");
            }
        }

        internal static bool InPlaceWriterForSpanValue<TInput, TVariableLengthInput>(ref LogRecord logRecord, ref TInput input, ReadOnlySpan<byte> newValue,
                ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, TVariableLengthInput varlenInput, FunctionsState functionsState, long expiration)
            where TVariableLengthInput : IVariableLengthInput<TInput>
        {
            RecordSizeInfo sizeInfo;

            if (logRecord.Info.ValueIsInline && (expiration == 0 || logRecord.Info.HasExpiration))
            {
                var (valueAddress, valueLength) = logRecord.PinnedValueAddressAndLength;
                if (!logRecord.TrySetPinnedValueSpan(newValue, valueAddress, ref valueLength))
                    return false;
                sizeInfo = new();
            }
            else
            {
                // Create local sizeInfo
                sizeInfo = new RecordSizeInfo() { FieldInfo = varlenInput.GetUpsertFieldInfo(logRecord, newValue, ref input) };
                functionsState.storeWrapper.store.Log.PopulateRecordSizeInfo(ref sizeInfo);

                if (!logRecord.TrySetValueSpanAndPrepareOptionals(newValue, in sizeInfo))
                    return false;
            }

            UpdateExpiration(ref logRecord, expiration);
            sizeInfo.AssertOptionalsIfSet(logRecord.Info);

            if (!logRecord.Info.Modified)
                functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            return true;
        }

        internal static bool InPlaceWriterForHeapObjectValue<TInput, TVariableLengthInput>(ref LogRecord logRecord, ref TInput input, IHeapObject newValue,
                ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, TVariableLengthInput varlenInput, FunctionsState functionsState, long expiration)
            where TVariableLengthInput : IVariableLengthInput<TInput>
        {

            // Create local sizeInfo
            var sizeInfo = new RecordSizeInfo() { FieldInfo = varlenInput.GetUpsertFieldInfo(logRecord, newValue, ref input) };
            functionsState.storeWrapper.store.Log.PopulateRecordSizeInfo(ref sizeInfo);

            if (!logRecord.TrySetValueObjectAndPrepareOptionals(newValue, in sizeInfo))
                return false;

            UpdateExpiration(ref logRecord, expiration);
            sizeInfo.AssertOptionalsIfSet(logRecord.Info);

            if (!logRecord.Info.Modified)
                functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            return true;
        }

        /// <inheritdoc />
        internal static bool InPlaceWriterForLogRecordValue<TSourceLogRecord, TInput, TVariableLengthInput>(ref LogRecord logRecord, ref TInput input, in TSourceLogRecord inputLogRecord,
                ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, TVariableLengthInput varlenInput, FunctionsState functionsState, long expiration)
            where TSourceLogRecord : ISourceLogRecord
            where TVariableLengthInput : IVariableLengthInput<TInput>
        {

            // Create local sizeInfo
            var sizeInfo = new RecordSizeInfo() { FieldInfo = varlenInput.GetUpsertFieldInfo(logRecord, inputLogRecord, ref input) };
            functionsState.storeWrapper.store.Log.PopulateRecordSizeInfo(ref sizeInfo);
            _ = logRecord.TryCopyFrom(in inputLogRecord, in sizeInfo);

            UpdateExpiration(ref logRecord, expiration);
            sizeInfo.AssertOptionalsIfSet(logRecord.Info);

            if (!logRecord.Info.Modified)
                functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void UpdateExpiration(ref LogRecord logRecord, long expiration)
        {
            if (expiration != 0)
            {
                if (!logRecord.TrySetExpiration(expiration))
                    Debug.Fail("Should have succeeded in setting Expiration as we should have ensured there was space there already");
            }
            else if (logRecord.Info.HasExpiration)
                _ = logRecord.RemoveExpiration();
        }
    }
}