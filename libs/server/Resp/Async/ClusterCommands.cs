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
        /// Waits out a cluster command that could not finish synchronously -- an epoch transition, a
        /// migration handshake, a replica sync -- and then lets the cluster session write its reply.
        /// </summary>
        /// <param name="pending">The wait the cluster command handed back instead of blocking.</param>
        [AsyncMethodBuilder(typeof(RespAsyncMethodBuilder))]
        async ValueTask ClusterCommandBodyAsync(ValueTask pending)
        {
            await pending.ConfigureAwait(false);
            CompletePendingClusterCommand();
        }
    }
}