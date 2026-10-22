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
        /// Replies to <c>PUBLISH</c>/<c>SPUBLISH</c> once the message has also reached the other cluster
        /// nodes. <paramref name="forward"/> is the in-flight forward started on the command's spans, which
        /// the caller has to begin while the parse state is still addressable; it copies anything it needs
        /// to keep before it goes async.
        /// </summary>
        bool CompleteClusterPublish(ValueTask forward, int numClients)
        {
            if (forward.IsCompletedSuccessfully)
            {
                WritePublishReply(numClients);
                return true;
            }

            ValueTask body;
            using (BeginAsyncCommand())
                body = ClusterPublishBodyAsync(forward, numClients);

            return CompleteAsyncCommand(body);
        }

        [AsyncMethodBuilder(typeof(RespAsyncMethodBuilder))]
        async ValueTask ClusterPublishBodyAsync(ValueTask forward, int numClients)
        {
            await forward.ConfigureAwait(false);
            WritePublishReply(numClients);
        }
    }
}