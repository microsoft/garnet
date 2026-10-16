// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Garnet.server
{
    // Not declared unsafe: an async method body cannot sit in an unsafe context, and the unsafe modifier
    // on a partial type declaration applies only to the part that carries it.
    internal sealed partial class RespServerSession
    {
        /// <summary>
        /// Reusable wait for a session parked until a key being migrated becomes accessible, released when this
        /// session is torn down.
        /// </summary>
        /// <remarks>
        /// Created on first use, and deliberately not disposed, for the same reasons as
        /// <see cref="SessionDelay"/>.
        /// </remarks>
        internal MigrationWait MigrationWait
        {
            get
            {
                var wait = migrationWait;
                if (wait is null)
                {
                    wait = new MigrationWait();
                    var existing = Interlocked.CompareExchange(ref migrationWait, wait, null);
                    if (existing is not null)
                        wait = existing;
                    else if (sessionTornDown)
                        wait.Cancel();
                }
                return wait;
            }
        }

        MigrationWait migrationWait;

        /// <summary>
        /// Parks the session until the migration that is holding a key its next command touches moves on, so the
        /// wait does not hold a thread the way spinning on the key would.
        /// </summary>
        /// <returns>True if the session parked, in which case the caller must stop processing the batch.</returns>
        /// <remarks>
        /// <para>
        /// Called from between commands with the read cursor rewound to the start of the command, so the command is
        /// parsed and verified again from the receive buffer when the wait is over. Nothing the command has done
        /// is kept: a command that has to wait has written no reply and touched no data.
        /// </para>
        /// <para>
        /// The park does not retain the batch scope, so the cluster epoch is released for the wait. The migration
        /// cannot finish its epoch transition while a session holds the epoch, and the session is waiting on the
        /// migration.
        /// </para>
        /// </remarks>
        [MethodImpl(MethodImplOptions.NoInlining)]
        bool ParkForMigration()
        {
            ValueTask body;
            using (BeginAsyncCommand())
                body = WaitForMigrationAsync();

            _ = CompleteAsyncCommand(body);
            return asyncSuspended;
        }

        [AsyncMethodBuilder(typeof(RespAsyncMethodBuilder))]
        async ValueTask WaitForMigrationAsync()
        {
            var wait = MigrationWait;
            wait.Arm();

            // The registration is refused if the migration has moved on since the verification that sent the
            // session here, and the command is then verified again straight away. A wake that finds the key still
            // blocked, or that was not meant for this wait, costs the same re-verification.
            if (!clusterSession.TryRegisterMigrationWaiter(wait))
                return;

            await wait.WaitAsync().ConfigureAwait(false);
        }
    }
}
