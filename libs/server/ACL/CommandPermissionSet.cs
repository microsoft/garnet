// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Linq;

namespace Garnet.server.ACL
{
    /// <summary>
    /// Wraps up command permissions behind a reference so it can be atomically swapped.
    /// </summary>
    public sealed class CommandPermissionSet
    {
        /// <summary>
        /// Users may have all commands or no commands added, in which case we can quickly check that.
        /// </summary>
        private enum AllState : byte
        {
            Invalid = 0,

            /// <summary>
            /// All commands, even those we don't know about, are allowed.
            /// </summary>
            AllPermitted,

            /// <summary>
            /// All commands are disabled.
            /// </summary>
            AllForbidden,

            /// <summary>
            /// Individual commands are enabled.
            /// </summary>
            PerCommand,
        }

        // Do not move these, initialization order is important
        private static readonly ushort CommandListLength = GetCommandListLength();

        public static readonly CommandPermissionSet All = new(AllState.AllPermitted, "+@all");
        public static readonly CommandPermissionSet None = new(AllState.AllForbidden, "");


        // Each bit corresponds to RespCommand + subcommand
        private readonly ulong[] _commandList;

        private AllState _all;

        private CommandPermissionSet(AllState all, string description)
            : this(all, new ulong[CommandListLength], description)
        {
        }

        private CommandPermissionSet(AllState all, ulong[] commandList, string description)
        {
            this._all = all;
            this._commandList = commandList;
            this.Description = description;
        }

        /// <summary>
        /// String which, when parsed by <see cref="ACLParser"/>, will produce an equivalent <see cref="CommandPermissionSet"/> to this one.
        /// 
        /// This is not updated automatically, and should be rationalized once modification via <see cref="RemoveCommand(RespCommand)"/> or <see cref="AddCommand(RespCommand)"/>
        /// is complete.
        /// </summary>
        public string Description { get; set; }

        /// <summary>
        /// Returns true if the given command + subCommand pair can be run.
        /// </summary>
        public bool CanRunCommand(RespCommand command)
        {
            if (this._all == AllState.AllPermitted)
            {
                return true;
            }

            if (this._all == AllState.AllForbidden)
            {
                return false;
            }

            RespCommand effectiveCommand = command.NormalizeForACLs();

            int index = (int)effectiveCommand;
            int ulongIndex = index / 64;
            int bitIndex = index % 64;

            return (_commandList[ulongIndex] & (1UL << bitIndex)) != 0;
        }

        /// <summary>
        /// Copy this permission set.
        /// </summary>
        public CommandPermissionSet Copy()
        {
            ulong[] copy = new ulong[this._commandList.Length];

            if (this._all == AllState.AllPermitted)
            {
                Array.Fill(copy, ulong.MaxValue);
            }
            else if (this._all == AllState.AllForbidden)
            {
                Array.Clear(copy);
            }
            else
            {
                Array.Copy(this._commandList, copy, this._commandList.Length);
            }

            return new(this._all, copy, Description);
        }

        /// <summary>
        /// Enable this command / sub-command pair.
        /// 
        /// subCommand == 0 is the root command.
        /// 
        /// This is not thread safe.
        /// </summary>
        public void AddCommand(RespCommand command)
        {
            Debug.Assert(command.NormalizeForACLs() == command, "Cannot control access to this command, it's an implementation detail");

            this._all = AllState.PerCommand;

            int index = (int)command;
            int ulongIndex = index / 64;
            int bitIndex = index % 64;

            _commandList[ulongIndex] |= (1UL << bitIndex);
        }

        /// <summary>
        /// Remove this command / sub-command pair.
        /// 
        /// subCommand == 0 is the root command.
        /// 
        /// This is not thread safe.
        /// </summary>
        public void RemoveCommand(RespCommand command)
        {
            Debug.Assert(command.NormalizeForACLs() == command, "Cannot control access to this command, it's an implementation detail");

            this._all = AllState.PerCommand;

            int index = (int)command;
            int ulongIndex = index / 64;
            int bitIndex = index % 64;

            _commandList[ulongIndex] &= ~(1UL << bitIndex);
        }

        /// <summary>
        /// Check if this and another <see cref="CommandPermissionSet"/> are equivalent.
        /// 
        /// They may be built using different commands, but if they cover the same set of runnable commands they are equivalent.
        /// 
        /// Note that All and None are only equivalent to themselves, as they are special cases that permit or forbid commands
        /// going forward.
        /// </summary>
        public bool IsEquivalentTo(CommandPermissionSet other)
        {
            if (this == CommandPermissionSet.All)
            {
                return other == CommandPermissionSet.All;
            }
            else if (this == CommandPermissionSet.None)
            {
                return other == CommandPermissionSet.None;
            }
            else
            {
                return this._commandList.AsSpan().SequenceEqual(other._commandList);
            }
        }

        /// <summary>
        /// Determines the size of the <see cref="_commandList"/> in each permission set.
        /// </summary>
        private static ushort GetCommandListLength()
        {
            int commandCount = (int)Enum.GetValues<RespCommand>().Where(static cmd => cmd != RespCommand.NONE && cmd != RespCommand.INVALID).Max();

            int neededBits = commandCount;
            int neededULongs = neededBits / 64;

            if ((neededBits % 64) != 0)
            {
                neededULongs++;
            }

            if (neededULongs > ushort.MaxValue)
            {
                throw new ACLException($"Too many commands bits to track for this to be a reasonable implementation");
            }

            return (ushort)neededULongs;
        }
    }
}