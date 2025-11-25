// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

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
    }
}