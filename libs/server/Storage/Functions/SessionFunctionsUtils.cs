// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
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

        /// <summary>
        /// Determine whether a modified log record should have an etag
        /// </summary>
        /// <param name="currEtag">Source record etag</param>
        /// <param name="metaCommandInfo">Meta command info</param>
        /// <returns>True if destination record should have an etag</returns>
        internal static bool CheckModifiedRecordHasEtag(long currEtag, ref MetaCommandInfo metaCommandInfo)
        {
            var metaCmd = metaCommandInfo.MetaCommand;

            // Source record has an etag or meta command is not a conditional execution etag command - destination record will have an etag
            if (currEtag != LogRecord.NoETag || (metaCmd.IsEtagCommand() && !metaCmd.IsEtagCondExecCommand()))
                return true;

            // Source record does not have an etag and the current meta command is not an etag command - the destination record will not have an etag
            if (!metaCmd.IsEtagCommand())
                return false;

            // Current meta command is a conditional execution etag command - check the condition to determine etag addition to the destination record
            Debug.Assert(metaCmd.IsEtagCondExecCommand());
            var inputEtag = metaCommandInfo.Arg1;

            return metaCmd.CheckConditionalExecution(currEtag, inputEtag);
        }

        /// <summary>
        /// Get the updated etag value for a log record
        /// </summary>
        /// <param name="currEtag">Current etag</param>
        /// <param name="init">True if method called from initial update context</param>
        /// <param name="metaCommandInfo">Meta command info</param>
        /// <param name="execCmd">Execute command</param>
        /// <param name="readOnly">True if method called from read-only context</param>
        /// <returns>Updated etag</returns>
        public static long GetUpdatedEtag(long currEtag, ref MetaCommandInfo metaCommandInfo, out bool execCmd, bool init = false, bool readOnly = false)
        {
            execCmd = true;
            var updatedEtag = currEtag;
            long inputEtag = LogRecord.NoETag;

            var metaCmd = metaCommandInfo.MetaCommand;
            if (metaCmd == RespMetaCommand.None && currEtag == LogRecord.NoETag)
                return updatedEtag;

            if (metaCmd.IsEtagCondExecCommand())
            {
                inputEtag = metaCommandInfo.Arg1;

                if (!init)
                    execCmd = metaCmd.CheckConditionalExecution(currEtag, inputEtag);
            }

            if (execCmd && !readOnly)
            {
                updatedEtag = metaCmd switch
                {
                    RespMetaCommand.None or RespMetaCommand.ExecWithEtag => currEtag + 1,
                    RespMetaCommand.ExecIfMatch => inputEtag + 1,
                    RespMetaCommand.ExecIfGreater => inputEtag,
                    _ => throw new ArgumentException($"Unexpected meta command: {metaCmd}", nameof(metaCmd)),
                };
            }

            return updatedEtag;
        }
    }
}