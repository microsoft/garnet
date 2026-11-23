// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace Garnet.server
{
    /// <summary>
    /// A logical group of related runtime configuration parameters published together as one consistent,
    /// versioned unit. Groups are held in an array indexed by <see cref="ServerConfigGroupType"/>, so the
    /// config table can mark, publish, and version them uniformly without knowing their concrete options type.
    /// </summary>
    public interface IConfigGroup
    {
        /// <summary>Logical group this instance represents; its value is also this group's table index.</summary>
        ServerConfigGroupType GetGroupType();

        /// <summary>
        /// True when the group has been republished since <paramref name="stampedVersion"/> was captured, i.e.
        /// a consumer that stamped that version now holds stale options. A single lock-free long compare.
        /// </summary>
        bool IsStale(long stampedVersion);

        /// <summary>Flags the group as changed so the next <see cref="PublishIfDirty"/> republishes it.</summary>
        void MarkDirty();

        /// <summary>
        /// If the group was marked dirty, re-materializes its options from <paramref name="config"/>, bumps the
        /// version, clears the dirty flag, and returns <see langword="true"/>; otherwise a no-op returning
        /// <see langword="false"/>.
        /// </summary>
        bool PublishIfDirty(RuntimeServerConfig config);

        /// <summary>
        /// Checks that the current slots in <paramref name="config"/> materialize into valid options without
        /// publishing them. On failure, <paramref name="error"/> holds a single-line RESP error message.
        /// </summary>
        bool TryValidate(RuntimeServerConfig config, out string error);
    }

    /// <summary>
    /// Typed refinement of <see cref="IConfigGroup"/> exposing the group's materialized options. Reads are
    /// typed; publish and versioning stay on the non-generic base so the group table stays uniform.
    /// </summary>
    /// <typeparam name="TOptions">Immutable value type the group materializes from its parameter slots.</typeparam>
    public interface IConfigGroup<TOptions> : IConfigGroup where TOptions : struct
    {
        /// <summary>Consistently reads the current options together with the version they were published under.</summary>
        TOptions Read(out long version);
    }

    /// <summary>
    /// Base implementation holding the group's current options behind a lock, with a monotonic version and a
    /// dirty flag. The lock guards the multi-field options struct against torn reads; the version is a
    /// lock-free staleness token. A future hot-path group can swap the lock for a seqlock without changing
    /// this surface.
    /// </summary>
    /// <typeparam name="TOptions">Immutable value type the group materializes from its parameter slots.</typeparam>
    internal abstract class ConfigGroupBase<TOptions> : IConfigGroup<TOptions> where TOptions : struct
    {
        readonly object gate = new();
        TOptions current;
        long version;
        bool dirty = true;

        /// <inheritdoc/>
        public abstract ServerConfigGroupType GetGroupType();

        /// <inheritdoc/>
        public bool IsStale(long stampedVersion) => Volatile.Read(ref version) != stampedVersion;

        /// <inheritdoc/>
        public void MarkDirty()
        {
            lock (gate)
                dirty = true;
        }

        /// <inheritdoc/>
        public TOptions Read(out long version)
        {
            lock (gate)
            {
                version = this.version;
                return current;
            }
        }

        /// <inheritdoc/>
        public bool PublishIfDirty(RuntimeServerConfig config)
        {
            lock (gate)
            {
                if (!dirty)
                    return false;

                current = Materialize(config);
                Volatile.Write(ref version, version + 1);
                dirty = false;
                return true;
            }
        }

        /// <inheritdoc/>
        public bool TryValidate(RuntimeServerConfig config, out string error)
        {
            try
            {
                _ = Materialize(config);
                error = null;
                return true;
            }
            catch (ArgumentException e)
            {
                // Exception messages may append the parameter name and value on extra lines; RESP errors are single-line.
                var message = e.Message;
                var newLine = message.AsSpan().IndexOfAny('\r', '\n');
                if (newLine >= 0)
                    message = message[..newLine];
                error = $"ERR Invalid {GetGroupType()} configuration: {message}";
                return false;
            }
        }

        /// <summary>Reads this group's parameter slots from <paramref name="config"/> into its options type.</summary>
        protected abstract TOptions Materialize(RuntimeServerConfig config);
    }
}