// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
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
        public bool CanAccessCommand(RespCommand command)
        => this._enabledCommands.CanRunCommand(command);

        /// <summary>
        /// Adds the given category to the user.
        /// </summary>
        /// <param name="category">Bit flag of the category to add.</param>
        public void AddCategory(RespAclCategories category)
        {
            IReadOnlyList<RespCommandsInfo> commandInfos;
            if (category != RespAclCategories.All)
            {
                if (!RespCommandsInfo.TryGetCommandsforAclCategory(category, out commandInfos))
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

                if (category == RespAclCategories.All)
                {
                    updated = CommandPermissionSet.All;
                }
                else
                {
                    updated = oldPerms.Copy();
                    foreach (RespCommand cmd in DetermineCommandDetails(commandInfos))
                    {
                        updated.AddCommand(cmd);
                    }
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
            if (!RespCommandsInfo.TryGetRespCommandInfo(command, out RespCommandsInfo info))
            {
                throw new ACLException("Unable to obtain ACL information, this shouldn't be possible");
            }

            IEnumerable<RespCommand> toAdd = DetermineCommandDetails([info]);

            CommandPermissionSet prev = this._enabledCommands;
            CommandPermissionSet oldPerms;
            CommandPermissionSet updated;
            do
            {
                oldPerms = prev;

                updated = oldPerms.Copy();
                foreach (RespCommand cmd in toAdd)
                {
                    updated.AddCommand(cmd);
                }
            }
            while ((prev = Interlocked.CompareExchange(ref this._enabledCommands, updated, oldPerms)) != oldPerms);
        }

        /// <summary>
        /// Removes the given category from the user.
        /// </summary>
        /// <param name="category">Bit flag of the category to remove.</param>
        public void RemoveCategory(RespAclCategories category)
        {
            IReadOnlyList<RespCommandsInfo> commandInfos;
            if (category != RespAclCategories.All)
            {
                if (!RespCommandsInfo.TryGetCommandsforAclCategory(category, out commandInfos))
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

                if (category == RespAclCategories.All)
                {
                    updated = CommandPermissionSet.None;
                }
                else
                {
                    updated = oldPerms.Copy();
                    foreach (RespCommand cmd in DetermineCommandDetails(commandInfos))
                    {
                        updated.RemoveCommand(cmd);
                    }
                }
            }
            while ((prev = Interlocked.CompareExchange(ref this._enabledCommands, updated, oldPerms)) != oldPerms);
        }

        /// <summary>
        /// REmoves the given command from the user.
        /// 
        /// If the command has subcommands, and no specific subcommand is indicated, removes all subcommands too.
        /// </summary>
        /// <param name="command">Command to remove.</param>
        public void RemoveCommand(RespCommand command)
        {
            if (!RespCommandsInfo.TryGetRespCommandInfo(command, out RespCommandsInfo info))
            {
                throw new ACLException("Unable to obtain ACL information, this shouldn't be possible");
            }

            IEnumerable<RespCommand> toAdd = DetermineCommandDetails([info]);

            CommandPermissionSet prev = this._enabledCommands;
            CommandPermissionSet oldPerms;
            CommandPermissionSet updated;
            do
            {
                oldPerms = prev;

                updated = oldPerms.Copy();
                foreach (RespCommand cmd in toAdd)
                {
                    updated.RemoveCommand(cmd);
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
            string permsStr = perms.GetDescription();
            if (!string.IsNullOrWhiteSpace(permsStr))
            {
                stringBuilder.Append($" {permsStr}");
            }


            return stringBuilder.ToString();
        }

        /// <summary>
        /// Determine the command / sub command pairs that are associated with this command information entries
        /// </summary>
        internal static IEnumerable<RespCommand> DetermineCommandDetails(IReadOnlyList<RespCommandsInfo> infos)
        {
            for (int i = 0; i < infos.Count; i++)
            {
                RespCommandsInfo info = infos[i];

                if (info.Parent != null)
                {
                    yield return info.SubCommand.Value;
                }
                else
                {
                    yield return info.Command;

                    if (info.SubCommands != null)
                    {
                        foreach (RespCommandsInfo subCommand in info.SubCommands)
                        {
                            yield return subCommand.SubCommand.Value;
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