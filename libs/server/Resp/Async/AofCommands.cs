// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Garnet.server
{
    // Not declared unsafe: an async method body cannot sit in an unsafe context, and the unsafe modifier
    // on a partial type declaration applies only to the part that carries it.
    internal sealed partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// <c>COMMITAOF [DBID]</c>.
        /// </summary>
        bool NetworkCOMMITAOF()
        {
            if (parseState.Count > 1)
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.COMMITAOF));

            // Commit the AOF of every active database unless one is named
            var dbId = -1;
            if (parseState.Count == 1 && !TryParseDatabaseId(0, out dbId))
                return true;

            ValueTask body;
            using (BeginAsyncCommand())
                body = CommitAofBodyAsync(dbId);

            return CompleteAsyncCommand(body);
        }

        [AsyncMethodBuilder(typeof(RespAsyncMethodBuilder))]
        async ValueTask CommitAofBodyAsync(int dbId)
        {
            _ = await storeWrapper.CommitAOFAsync(dbId).ConfigureAwait(false);
            WriteSimpleString("AOF file committed"u8);
        }

        /// <summary>
        /// Holds the batch's replies back until <paramref name="commit"/> lands, so a reply is acked no
        /// earlier than the AOF records behind it are durable. The flush itself is left to the batch end the
        /// session resumes into, which has nothing more to run and no further records to append.
        /// </summary>
        [AsyncMethodBuilder(typeof(RespAsyncMethodBuilder))]
        async ValueTask FlushAfterCommitAsync(ValueTask<bool> commit)
        {
            _ = await commit.ConfigureAwait(false);
            waitForAofBlocking = false;
        }
    }
}