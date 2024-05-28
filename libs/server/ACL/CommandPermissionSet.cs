// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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

        // do not move these, initialization order is important
        private static readonly byte MaxSubCommands = GetMaxSubCommands();
        private static readonly ushort CommandListLength = GetCommandListLength();

        public static readonly CommandPermissionSet All = new(AllState.AllPermitted);
        public static readonly CommandPermissionSet None = new(AllState.AllForbidden);


        // each bit corresponds to RespCommand + subcommand
        private readonly ulong[] _commandList;

        private AllState _all;

        private CommandPermissionSet(AllState all)
            : this(all, new ulong[CommandListLength])
        {
        }

        private CommandPermissionSet(AllState all, ulong[] commandList)
        {
            this._all = all;
            this._commandList = commandList;
        }

        /// <summary>
        /// Returns true if the given command + subCommand pair can be run.
        /// </summary>
        public bool CanRunCommand(RespCommand command, byte subCommand)
        {
            // quick check for +@all
            if (this._all == AllState.AllPermitted)
            {
                return true;
            }

            // quick check for -@all
            if (this._all == AllState.AllForbidden)
            {
                return false;
            }

            int index = ((int)command * MaxSubCommands) + subCommand;
            int ulongIndex = index / 64;
            int bitIndex = index % 64;

            // if we can run it, this bit will be set
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

            return new(this._all, copy);
        }

        /// <summary>
        /// Enable this command / sub-command pair.
        /// 
        /// subCommand == 0 is the root command.
        /// 
        /// This is not thread safe.
        /// </summary>
        public void AddCommand(RespCommand command, byte subCommand)
        {
            this._all = AllState.PerCommand;

            int index = ((int)command * MaxSubCommands) + subCommand;
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
        public void RemoveCommand(RespCommand command, byte subCommand)
        {
            this._all = AllState.PerCommand;

            int index = ((int)command * MaxSubCommands) + subCommand;
            int ulongIndex = index / 64;
            int bitIndex = index % 64;

            _commandList[ulongIndex] &= ~(1UL << bitIndex);
        }

        /// <summary>
        /// Build the equivalent "+@all -@foo +set -acl|setuser" string for this permission set.
        /// </summary>
        public string GetDescription()
        {
            if (this == CommandPermissionSet.All)
            {
                return "+@all";
            }
            else if (this == CommandPermissionSet.None)
            {
                return "";
            }

            StringBuilder sb = new();
            RespAclCategories permittedCategories = 0;

            // handle individual categories
            foreach (RespAclCategories cat in Enum.GetValues<RespAclCategories>())
            {
                if (cat == RespAclCategories.None)
                {
                    continue;
                }

                bool allAllowed = true;
                if (RespCommandsInfo.TryGetCommandsforAclCategory(cat, out IReadOnlyList<RespCommandsInfo> commands))
                {
                    foreach ((RespCommand cmd, byte subCmd) in User.DetermineCommandDetails(commands))
                    {
                        bool canRun = this.CanRunCommand(cmd, subCmd);
                        allAllowed &= canRun;
                    }
                }
                else
                {
                    continue;
                }

                if (!allAllowed)
                {
                    // empty category
                    continue;
                }

                string categoryName = ACLParser.GetNameByACLCategory(cat);

                if (allAllowed)
                {
                    sb.Append($" +@{categoryName}");
                    permittedCategories |= cat;
                }
            }

            // todo: individual commands

            // todo: subcommands

            return sb.ToString();
        }

        /// <summary>
        /// Figure out the maximum actual number of sub commands we have configured.
        /// </summary>
        private static byte GetMaxSubCommands()
        {
            RespCommand[] cmds = Enum.GetValues<RespCommand>().Where(static cmd => cmd != RespCommand.NONE && cmd != RespCommand.INVALID).ToArray();

            byte max = 0;

            foreach (RespCommand cmd in cmds)
            {
                if (RespCommandsInfo.TryGetRespCommandInfo(cmd, out RespCommandsInfo info) && info.SubCommands != null)
                {
                    int len = info.SubCommands.Length;
                    if (len > byte.MaxValue - 1)
                    {
                        throw new ACLException($"Too many sub-commands ({len}) for command {cmd} to create command permission set");
                    }

                    if (len > max)
                    {
                        max = (byte)len;
                    }
                }
            }

            return max;
        }

        /// <summary>
        /// Determines the size of the <see cref="_commandList"/> in each permission set.
        /// </summary>
        private static ushort GetCommandListLength()
        {
            int commandCount = (int)Enum.GetValues<RespCommand>().Where(static cmd => cmd != RespCommand.NONE && cmd != RespCommand.INVALID).Max();

            byte maxSubCommands = GetMaxSubCommands();

            int neededBits = commandCount * (1 + maxSubCommands); // space for the parent command itself
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
