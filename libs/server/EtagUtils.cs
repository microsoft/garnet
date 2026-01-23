// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    public static class EtagUtils
    {
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
