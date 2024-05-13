// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection.Metadata.Ecma335;
using System.Security;
using System.Text;
using System.Threading;

namespace Garnet.server.ACL
{
    /// <summary>
    /// Represents a Garnet user and associated access rights.
    /// </summary>
    public class User
    {
        /// <summary>
        /// Wraps up command permissions behind a reference so it can be atomically swapped.
        /// </summary>
        private sealed class CommandPermissionSet
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

        /// <summary>
        /// The associated username
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Tracks whether the user account is currently active or disabled
        /// </summary>
        public bool IsEnabled { get; set; }

        /// <summary>
        /// Indicates that no password matching will be done, regardless of the passed in password.
        /// Note that passwordless mode needs to be enabled to successfully authenticate when no password hashes are set.
        /// </summary>
        public bool IsPasswordless { get; set; }

        /// <summary>
        /// Creates a new user with the given name
        /// </summary>
        /// <param name="name">Name of the new user</param>
        public User(string name)
        {
            Name = name;
            IsEnabled = false;
            IsPasswordless = false;
            _enabledCommands = CommandPermissionSet.None;
        }

        /// <summary>
        /// Check whether the user can access the given command.
        /// </summary>
        public bool CanAccessCommand(RespCommand command, byte subCommand)
        => this._enabledCommands.CanRunCommand(command, subCommand);

        /// <summary>
        /// Adds the given category to the user.
        /// </summary>
        /// <param name="category">Bit flag of the category to add.</param>
        public void AddCategory(CommandCategory.Flag category)
        {
            // todo: probably just remove CommandCategory.Flag?

            IReadOnlyList<RespCommandsInfo> commandInfos;
            if (category != CommandCategory.Flag.All)
            {
                RespAclCategories trueCat = CommandCategory.ToRespAclCategory(category);

                if (!RespCommandsInfo.TryGetCommandsforAclCategory(trueCat, out commandInfos))
                {
                    throw new ACLException("Unable to obtain ACL information, this shouldn't be possible");
                }
            }
            else
            {
                commandInfos = Array.Empty<RespCommandsInfo>();
            }

            CommandPermissionSet prev = this._enabledCommands;
            CommandPermissionSet oldPerms;
            CommandPermissionSet updated;
            do
            {
                oldPerms = prev;

                if (category == CommandCategory.Flag.All)
                {
                    updated = CommandPermissionSet.All;
                }
                else
                {
                    updated = oldPerms.Copy();
                    foreach ((RespCommand cmd, byte subCmd) in DetermineCommandDetails(commandInfos))
                    {
                        updated.AddCommand(cmd, subCmd);
                    }
                }
            }
            while ((prev = Interlocked.CompareExchange(ref this._enabledCommands, updated, oldPerms)) != oldPerms);
        }

        /// <summary>
        /// Removes the given category from the user.
        /// </summary>
        /// <param name="category">Bit flag of the category to remove.</param>
        public void RemoveCategory(CommandCategory.Flag category)
        {
            // todo: probably just remove CommandCategory.Flag?
            IReadOnlyList<RespCommandsInfo> commandInfos;
            if (category != CommandCategory.Flag.All)
            {
                RespAclCategories trueCat = CommandCategory.ToRespAclCategory(category);

                if (!RespCommandsInfo.TryGetCommandsforAclCategory(trueCat, out commandInfos))
                {
                    throw new ACLException("Unable to obtain ACL information, this shouldn't be possible");
                }
            }
            else
            {
                commandInfos = Array.Empty<RespCommandsInfo>();
            }

            CommandPermissionSet prev = this._enabledCommands;
            CommandPermissionSet oldPerms;
            CommandPermissionSet updated;
            do
            {
                oldPerms = prev;

                if (category == CommandCategory.Flag.All)
                {
                    updated = CommandPermissionSet.None;
                }
                else
                {
                    updated = oldPerms.Copy();
                    foreach ((RespCommand cmd, byte subCmd) in DetermineCommandDetails(commandInfos))
                    {
                        updated.RemoveCommand(cmd, subCmd);
                    }
                }
            }
            while ((prev = Interlocked.CompareExchange(ref this._enabledCommands, updated, oldPerms)) != oldPerms);
        }

        /// <summary>
        /// Adds a new password for the user.
        /// </summary>
        /// <param name="password">ACLPassword to add to the allowed passwords for the user.</param>
        public void AddPasswordHash(ACLPassword password)
        {
            lock (_passwordHashes)
            {
                _passwordHashes.Add(password);
            }
        }

        /// <summary>
        /// Removes the password from the list of allowed passwords for the user.
        /// </summary>
        /// <param name="password">ACLPassword to remove from the allowed passwords for the user.</param>
        public void RemovePasswordHash(ACLPassword password)
        {
            lock (_passwordHashes)
            {
                _passwordHashes.Remove(password);
            }
        }

        /// <summary>
        /// Removes all passwords currently registered for the user.
        /// </summary>
        public void ClearPasswords()
        {
            lock (_passwordHashes)
            {
                _passwordHashes.Clear();
            }
        }

        /// <summary>
        /// Removes all currently configured capabilities from the user and disables the user
        /// </summary>
        public void Reset()
        {
            // Reset passwords
            this.ClearPasswords();

            // Reset categories
            this._enabledCommands = CommandPermissionSet.None;

            // Disable user
            this.IsEnabled = false;
        }

        /// <summary>
        /// Returns true if the given password hash is valid for this user.
        /// </summary>
        /// <param name="password">An ACL password hash to check against this user.</param>
        /// <returns>true if the given password hash is valid for this user, otherwise false.</returns>
        public bool ValidatePassword(ACLPassword password)
        {
            // Passwordless users accept any password
            if (IsPasswordless)
            {
                return true;
            }

            // Any of the registered password hashes is allowed

            bool matched = false;

            lock (_passwordHashes)
            {
                foreach (ACLPassword hash in _passwordHashes)
                {
                    if (password.Equals(hash))
                    {
                        matched = true;
                    }
                }
            }

            return matched;
        }

        /// <summary>
        /// Exports an easily readable textual representation of the user settings in ACL rule format.
        /// </summary>
        /// <returns>String representation of the user.</returns>
        public string DescribeUser()
        {
            StringBuilder stringBuilder = new();

            stringBuilder.Append($"user {this.Name}");

            // Flags
            if (this.IsEnabled)
            {
                stringBuilder.Append(" on");
            }
            else
            {
                stringBuilder.Append(" off");
            }

            if (this.IsPasswordless)
            {
                stringBuilder.Append(" nopass");
            }

            // Passwords
            foreach (ACLPassword hash in _passwordHashes)
            {
                stringBuilder.Append($" #{hash}");
            }

            // ACLs
            CommandPermissionSet perms = _enabledCommands;

            // Categories
            RespAclCategories completelyCoveredCategories = 0;
            if (perms == CommandPermissionSet.All)
            {
                stringBuilder.Append(" +@all");
            }
            else if (perms == CommandPermissionSet.None)
            {
                stringBuilder.Append(" -@all");
            }
            else
            {
                foreach (RespAclCategories cat in Enum.GetValues<RespAclCategories>())
                {
                    if (cat == RespAclCategories.None)
                    {
                        continue;
                    }

                    var allAllowed = true;
                    var allDenied = true;
                    if (RespCommandsInfo.TryGetCommandsforAclCategory(cat, out IReadOnlyList<RespCommandsInfo> commands))
                    {
                        for (int i = 0; i < commands.Count; i++)
                        {
                            RespCommandsInfo info = commands[i];
                            var canRun = perms.CanRunCommand(info.Command, 0);

                            allAllowed &= canRun;
                            allDenied &= !canRun;
                        }
                    }
                    else
                    {
                        continue;
                    }

                    if (allAllowed && allDenied)
                    {
                        // empty category
                        continue;
                    }

                    // todo: probably just remove CommandCategory?
                    string categoryName = CommandCategory.GetNameByFlag(CommandCategory.FromRespAclCategories(cat));

                    if (allAllowed)
                    {
                        stringBuilder.Append($" +@{categoryName}");
                        completelyCoveredCategories |= cat;
                    }
                    else if (allDenied)
                    {
                        stringBuilder.Append($" -@{categoryName}");
                        completelyCoveredCategories |= cat;
                    }
                }
            }

            // todo: individual commands

            return stringBuilder.ToString();
        }

        /// <summary>
        /// Determine the command / sub command pairs that are associated with this command information entries
        /// </summary>
        private static IEnumerable<(RespCommand Command, byte SubCommand)> DetermineCommandDetails(IReadOnlyList<RespCommandsInfo> infos)
        {
            for (int i = 0; i < infos.Count; i++)
            {
                RespCommandsInfo info = infos[i];

                if (info.Parent != null)
                {
                    // banning a sub command means we need to figure out the subCommand index...
                    int subCommadIx = Array.IndexOf(info.Parent.SubCommands, info);
                    if (subCommadIx == -1)
                    {
                        throw new ACLException("Couldn't find sub command index, this shouldn't happen");
                    }

                    yield return (info.Parent.Command, (byte)(subCommadIx + 1));
                }
                else
                {
                    yield return (info.Command, SubCommand: 0);

                    if (info.SubCommands != null)
                    {
                        for (int j = 0; j < info.SubCommands.Length; j++)
                        {
                            yield return (info.Command, (byte)(j + 1));
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Commands enabled for the user
        /// </summary>
        CommandPermissionSet _enabledCommands;

        /// <summary>
        /// A set of all allowed _passwordHashes for the user.
        /// 
        /// NOTE: HashSet is not thread-safe, so accesses need to be synchronized
        /// </summary>
        readonly HashSet<ACLPassword> _passwordHashes = [];
    }
}