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
        /// The replication-lag gate of the AOF this session appends to, or null when there is no AOF.
        /// </summary>
        readonly AofBackpressure aofBackpressure;

        /// <summary>
        /// Reusable wait for a session parked until the AOF admits appends again, released when this session is
        /// torn down.
        /// </summary>
        /// <remarks>
        /// Created on first use, and deliberately not disposed, for the same reasons as
        /// <see cref="SessionDelay"/>.
        /// </remarks>
        internal AofGateWait AofGateWait
        {
            get
            {
                var wait = aofGateWait;
                if (wait is null)
                {
                    wait = new AofGateWait();
                    var existing = Interlocked.CompareExchange(ref aofGateWait, wait, null);
                    if (existing is not null)
                        wait = existing;
                    else if (sessionTornDown)
                        wait.Cancel();
                }
                return wait;
            }
        }

        AofGateWait aofGateWait;

        /// <summary>
        /// Whether <paramref name="cmd"/>, which has just been parsed, may append to the AOF.
        /// </summary>
        /// <remarks>
        /// This only decides which commands are worth waiting for. A command it misses still meets the gate in the
        /// append itself, which is the guard that must hold; it just waits there on a thread. EXEC is not
        /// included: only a transaction that has been queued makes it append, which the caller knows.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool MayAppendToAof(RespCommand cmd)
        {
            if (cmd.IsWriteOnly())
                return true;

            // A script, procedure or transaction can write, and what it will do is not known before it runs
            return cmd switch
            {
                RespCommand.EVAL or RespCommand.EVALSHA or RespCommand.RUNTXP
                    or RespCommand.CustomTxn or RespCommand.CustomProcedure => true,
                RespCommand.CustomRawStringCmd => currentCustomRawStringCommand.type != CommandType.Read,
                RespCommand.CustomObjCmd => currentCustomObjectCommand.type != CommandType.Read,
                _ => false,
            };
        }

        /// <summary>
        /// Whether <paramref name="cmd"/>, which has just been parsed, has to wait for the AOF to admit appends:
        /// it may append, the AOF is too far ahead of its replicas for that, and this session can park to wait.
        /// </summary>
        /// <remarks>
        /// The switch is read first, so with the gate off, which is the default, a command pays one load. A consume
        /// that cannot park, such as a script's, does not wait here; its appends are held by the gate in the
        /// append itself.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool MustWaitForAofGate(RespCommand cmd)
            => aofBackpressure is { Enabled: true } && MayAppendToAof(cmd) && CanSuspend && aofBackpressure.AdmissionClosed();

        /// <summary>
        /// Whether an EXEC, which has just been parsed against a transaction that has queued its commands, has to
        /// wait for the AOF to admit appends. Any of the queued commands may append, and none has run.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool MustWaitForAofGateToExec()
            => aofBackpressure is { Enabled: true } && CanSuspend && aofBackpressure.AdmissionClosed();

        /// <summary>
        /// Parks the session until the AOF admits appends again, so a write it cannot take yet waits without
        /// holding a thread the way stalling in the append would.
        /// </summary>
        /// <returns>True if the session parked, in which case the caller must stop processing the batch.</returns>
        /// <remarks>
        /// <para>
        /// Called from between commands with the read cursor rewound to the start of the command, so the command is
        /// parsed, authorized and verified again from the receive buffer when the wait is over: the slot it
        /// targets can have moved while the session waited. A command that has to wait has written no reply and
        /// touched no data.
        /// </para>
        /// <para>
        /// The park does not retain the batch scope, so the cluster epoch is released for the wait. The wait lasts
        /// as long as a replica takes to catch up, and an epoch transition cannot finish while a session holds the
        /// epoch.
        /// </para>
        /// </remarks>
        [MethodImpl(MethodImplOptions.NoInlining)]
        bool ParkForAofGate()
        {
            ValueTask body;
            using (BeginAsyncCommand())
                body = WaitForAofGateAsync();

            _ = CompleteAsyncCommand(body);
            return asyncSuspended;
        }

        [AsyncMethodBuilder(typeof(RespAsyncMethodBuilder))]
        async ValueTask WaitForAofGateAsync()
        {
            var wait = AofGateWait;
            wait.Arm();

            // The registration is refused if admission has reopened since the check that sent the session here, and
            // the command is then run straight away. A wake that finds admission closed again costs one more park.
            if (!aofBackpressure.TryRegisterWaiter(wait))
                return;

            await wait.WaitAsync().ConfigureAwait(false);
        }
    }
}