// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Garnet.common;

namespace Garnet.server
{
    // Not declared unsafe: an async method body cannot sit in an unsafe context, and the unsafe modifier
    // on a partial type declaration applies only to the part that carries it.
    internal sealed partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// <c>SAVE [DBID]</c>.
        /// </summary>
        bool NetworkSAVE()
        {
            if (parseState.Count > 1)
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.SAVE));

            // Checkpoint every active database unless one is named
            var dbId = -1;
            if (parseState.Count == 1 && !TryParseDatabaseId(0, out dbId))
                return true;

            ValueTask body;
            using (BeginAsyncCommand())
                body = SaveBodyAsync(dbId);

            return CompleteAsyncCommand(body);
        }

        [AsyncMethodBuilder(typeof(RespAsyncMethodBuilder))]
        async ValueTask SaveBodyAsync(int dbId)
        {
            var status = await storeWrapper.TakeCheckpointAsync(false, dbId: dbId, logger: logger).ConfigureAwait(false);

            switch (status)
            {
                case CheckpointStatus.AlreadyInProgress:
                    WriteError(CmdStrings.RESP_ERR_CHECKPOINT_ALREADY_IN_PROGRESS);
                    break;
                case CheckpointStatus.Failed:
                    // Nothing durable was written, so replying OK here would report data as saved that was not.
                    WriteError(CmdStrings.RESP_ERR_CHECKPOINT_FAILED);
                    break;
                default:
                    WriteDirect(CmdStrings.RESP_OK);
                    break;
            }
        }

        /// <summary>
        /// <c>BGSAVE [SCHEDULE] [DBID]</c>.
        /// </summary>
        bool NetworkBGSAVE()
        {
            if (parseState.Count > 2)
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.BGSAVE));

            // Checkpoint every active database unless one is named
            var dbId = -1;

            var tokenIdx = 0;
            if (parseState.Count > 0)
            {
                if (parseState.GetArgSliceByRef(tokenIdx).ReadOnlySpan
                    .EqualsUpperCaseSpanIgnoringCase(CmdStrings.SCHEDULE))
                    tokenIdx++;

                if (parseState.Count - tokenIdx > 0 && !TryParseDatabaseId(tokenIdx, out dbId))
                    return true;
            }

            ValueTask body;
            using (BeginAsyncCommand())
                body = BackgroundSaveBodyAsync(dbId);

            return CompleteAsyncCommand(body);
        }

        [AsyncMethodBuilder(typeof(RespAsyncMethodBuilder))]
        async ValueTask BackgroundSaveBodyAsync(int dbId)
        {
            var status = await storeWrapper.TakeCheckpointAsync(true, dbId: dbId, logger: logger).ConfigureAwait(false);

            // A background checkpoint replies before it runs, so its failure cannot be reported here; it surfaces
            // through LASTSAVE not advancing and through rdb_last_bgsave_status in INFO PERSISTENCE.
            if (status == CheckpointStatus.AlreadyInProgress)
                WriteError(CmdStrings.RESP_ERR_CHECKPOINT_ALREADY_IN_PROGRESS);
            else
                WriteSimpleString("Background saving started"u8);
        }
    }
}