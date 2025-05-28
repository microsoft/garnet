// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.CompilerServices;
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
        /// Returns a copy of the <see cref="User"/>s passwords.
        /// </summary>
        public ISet<ACLPassword> Passwords
        {
            get
            {
                lock (_passwordHashes)
                {
                    // Eventually replace with ReadOnlySet<T> in .NET 9
                    return _passwordHashes.ToImmutableHashSet<ACLPassword>();
                }
            }
        }

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
        /// Copy constructor for a <see cref="User"/>.
        /// </summary>
        /// <param name="user">The <see cref="User"/> the new <see cref="User"/> is constructed from.</param>
        public User(User user)
        {
            Name = user.Name;
            IsEnabled = user.IsEnabled;
            IsPasswordless = user.IsPasswordless;
            _enabledCommands = user._enabledCommands.Copy();
            _passwordHashes = user.CopyPasswordHashes();
        }

        /// <summary>
        /// Checks whether the user can access the given command.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool CanAccessCommand(RespCommand command)
        => this._enabledCommands.CanRunCommand(command);

        /// <summary>
        /// Adds the given category to the user.
        /// </summary>
        /// <param name="category">Bit flag of the category to add.</param>
        public void AddCategory(RespAclCategories category)
        {
            CommandPermissionSet prev = this._enabledCommands;

            // no-op
            if (prev == CommandPermissionSet.All)
            {
                return;
            }

            string descUpdate;
            IReadOnlyList<RespCommandsInfo> commandInfos;
            if (category != RespAclCategories.All)
            {
                if (!RespCommandsInfo.TryGetCommandsforAclCategory(category, out commandInfos))
                {
                    throw new ACLException("Unable to obtain ACL information, this shouldn't be possible");
                }

                bool canRunAll = true;
                foreach (RespCommand cmd in User.DetermineCommandDetails(commandInfos))
                {
                    if (!prev.CanRunCommand(cmd))
                    {
                        canRunAll = false;
                        break;
                    }
                }

                // NO-OP
                if (canRunAll)
                {
                    return;
                }

                descUpdate = $"+@{ACLParser.GetNameByACLCategory(category)}";
            }
            else
            {
                commandInfos = [];
                descUpdate = null;
            }

            CommandPermissionSet oldPerms;
            CommandPermissionSet updated;
            do
            {
                oldPerms = prev;

                if (category == RespAclCategories.All)
                {
                    updated = CommandPermissionSet.All;
                }
                else
                {
                    bool useDeepRationalization = false;
                    updated = oldPerms.Copy();

                    foreach (RespCommand cmd in DetermineCommandDetails(commandInfos))
                    {
                        // Perform deep rationalization when one instance of command overlap may exist.
                        useDeepRationalization = useDeepRationalization || updated.CanRunCommand(cmd);
                        updated.AddCommand(cmd);
                    }

                    updated.Description = RationalizeACLDescription(updated, $"{updated.Description} {descUpdate}", useDeepRationalization);
                }
            }
            while ((prev = Interlocked.CompareExchange(ref this._enabledCommands, updated, oldPerms)) != oldPerms);
        }

        /// <summary>
        /// Adds the given command to the user.
        /// 
        /// If the command has subcommands, and no specific subcommand is indicated, adds all subcommands too.
        /// </summary>
        /// <param name="command">Command to add.</param>
        public void AddCommand(RespCommand command)
        {
            CommandPermissionSet prev = this._enabledCommands;

            if (!RespCommandsInfo.TryGetRespCommandInfo(command, out RespCommandsInfo info))
            {
                throw new ACLException("Unable to obtain ACL information, this shouldn't be possible");
            }

            IEnumerable<RespCommand> toAdd = DetermineCommandDetails([info]);

            bool canAlreadyRun = true;
            foreach (RespCommand toCheck in toAdd)
            {
                if (!prev.CanRunCommand(toCheck))
                {
                    canAlreadyRun = false;
                    break;
                }
            }

            // Update is a no-op, skip the work
            if (canAlreadyRun)
            {
                return;
            }

            string descUpdate = $"+{info.Name.ToLowerInvariant()}";

            CommandPermissionSet oldPerms;
            CommandPermissionSet updated;
            do
            {
                bool useDeepRationalization = false;
                oldPerms = prev;
                updated = oldPerms.Copy();

                foreach (RespCommand cmd in toAdd)
                {
                    // Perform deep rationalization when one instance of command overlap may exist.
                    useDeepRationalization = useDeepRationalization || updated.CanRunCommand(cmd);
                    updated.AddCommand(cmd);
                }

                updated.Description = RationalizeACLDescription(updated, $"{updated.Description} {descUpdate}", useDeepRationalization);
            }
            while ((prev = Interlocked.CompareExchange(ref this._enabledCommands, updated, oldPerms)) != oldPerms);
        }

        /// <summary>
        /// Removes the given category from the user.
        /// </summary>
        /// <param name="category">Bit flag of the category to remove.</param>
        public void RemoveCategory(RespAclCategories category)
        {
            // Removing from -@all is a no-op
            CommandPermissionSet prev = this._enabledCommands;
            if (prev == CommandPermissionSet.None)
            {
                return;
            }

            string descUpdate;
            IReadOnlyList<RespCommandsInfo> commandInfos;
            if (category != RespAclCategories.All)
            {
                if (!RespCommandsInfo.TryGetCommandsforAclCategory(category, out commandInfos))
                {
                    throw new ACLException("Unable to obtain ACL information, this shouldn't be possible");
                }

                bool canRunAny = false;
                foreach (RespCommand cmd in User.DetermineCommandDetails(commandInfos))
                {
                    if (this.CanAccessCommand(cmd))
                    {
                        canRunAny = true;
                        break;
                    }
                }

                // NO-OP
                if (!canRunAny)
                {
                    return;
                }

                descUpdate = $"-@{ACLParser.GetNameByACLCategory(category)}";
            }
            else
            {
                commandInfos = [];
                descUpdate = null;
            }

            CommandPermissionSet oldPerms;
            CommandPermissionSet updated;
            do
            {
                oldPerms = prev;

                if (category == RespAclCategories.All)
                {
                    updated = CommandPermissionSet.None;
                }
                else
                {
                    bool useDeepRationalization = false;
                    updated = oldPerms.Copy();

                    foreach (RespCommand cmd in DetermineCommandDetails(commandInfos))
                    {
                        // Perform deep rationalization when one instance of command overlap may exist.
                        useDeepRationalization = useDeepRationalization || updated.CanRunCommand(cmd);
                        updated.RemoveCommand(cmd);
                    }

                    updated.Description = RationalizeACLDescription(updated, $"{updated.Description} {descUpdate}", useDeepRationalization);
                }
            }
            while ((prev = Interlocked.CompareExchange(ref this._enabledCommands, updated, oldPerms)) != oldPerms);
        }

        /// <summary>
        /// Removes the given command from the user.
        /// 
        /// If the command has subcommands, and no specific subcommand is indicated, removes all subcommands too.
        /// </summary>
        /// <param name="command">Command to remove.</param>
        public void RemoveCommand(RespCommand command)
        {
            CommandPermissionSet prev = this._enabledCommands;

            if (!RespCommandsInfo.TryGetRespCommandInfo(command, out RespCommandsInfo info))
            {
                throw new ACLException("Unable to obtain ACL information, this shouldn't be possible");
            }

            IEnumerable<RespCommand> toRemove = DetermineCommandDetails([info]);

            bool cantRun = true;
            foreach (RespCommand cmd in toRemove)
            {
                if (this.CanAccessCommand(cmd))
                {
                    cantRun = false;
                    break;
                }
            }

            // Update is a no-op, skip the work
            if (cantRun)
            {
                return;
            }

            string descUpdate = $"-{info.Name.ToLowerInvariant()}";

            CommandPermissionSet oldPerms;
            CommandPermissionSet updated;
            do
            {
                bool useDeepRationalization = false;
                oldPerms = prev;
                updated = oldPerms.Copy();

                foreach (RespCommand cmd in toRemove)
                {
                    // Perform deep rationalization when one instance of command overlap may exist.
                    useDeepRationalization = useDeepRationalization || updated.CanRunCommand(cmd);
                    updated.RemoveCommand(cmd);
                }

                updated.Description = RationalizeACLDescription(updated, $"{updated.Description} {descUpdate}", useDeepRationalization);
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
            string permsStr = perms.Description;
            if (!string.IsNullOrWhiteSpace(permsStr))
            {
                stringBuilder.Append($" {permsStr}");
            }

            return stringBuilder.ToString();
        }

        /// <summary>
        /// Returns a <see cref="string"/> containing the enabled commands.
        /// </summary>
        /// <returns>A <see cref="string"/> containing the enabled commands.</returns>
        public string GetEnabledCommandsDescription()
        {
            return _enabledCommands.Description;
        }

        /// <summary>
        /// Determine the command / sub command pairs that are associated with this command information entries
        /// </summary>
        internal static IEnumerable<RespCommand> DetermineCommandDetails(IReadOnlyList<RespCommandsInfo> infos)
        {
            foreach (var info in infos)
            {
                if (info.Parent != null)
                {
                    yield return info.Command;
                }
                else
                {
                    yield return info.Command;

                    if (info.SubCommands != null)
                    {
                        foreach (var subCommand in info.SubCommands)
                        {
                            yield return subCommand.Command;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Check to see if any tokens from a description can be removed without modifying the effective permissions.
        /// 
        /// This is an expensive method, but ACL modifications are rare enough it's hopefully not a problem. In situations
        /// where it is known ahead of time that the CommandPermissionSet does not require reductions, set useDeepRationalization
        /// to true to optimize the method execution by avoiding a deeper recursive analysis.
        /// </summary>
        private static string RationalizeACLDescription(CommandPermissionSet set, string description, bool useDeepRationalization)
        {
            List<string> parts = [.. description.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)];
            while (useDeepRationalization)
            {
                bool shrunk = false;

                for (int i = 0; i < parts.Count; i++)
                {
                    string withoutRule = $"user test on >xxx {string.Join(" ", parts.Take(i).Skip(1))}";
                    CommandPermissionSet withoutPerms = ACLParser.ParseACLRule(withoutRule).CopyCommandPermissionSet();
                    if (withoutPerms.IsEquivalentTo(set))
                    {
                        parts.RemoveAt(i);
                        i--;
                        shrunk = true;
                    }
                }

                if (!shrunk)
                {
                    break;
                }
            }

            return string.Join(" ", parts);
        }

        /// <summary>
        /// Returns a copy of the users current <see cref="CommandPermissionSet"/>.
        /// </summary>
        internal CommandPermissionSet CopyCommandPermissionSet()
        => _enabledCommands.Copy();

        /// <summary>
        /// Returns a copy of the password hashes for the <see cref="User"/>.
        /// </summary>
        /// <returns>A <see cref="HashSet{T}"/> of <see cref="ACLPassword"/> representing password hashes for the user.</returns>
        private HashSet<ACLPassword> CopyPasswordHashes()
        {
            HashSet<ACLPassword> passwordHashesCopy = null;

            lock (_passwordHashes)
            {
                passwordHashesCopy = new HashSet<ACLPassword>(_passwordHashes);
            }

            return passwordHashesCopy;
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