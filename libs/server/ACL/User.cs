// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
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
        }

        /// <summary>
        /// Checks whether the user can access the given command category.
        /// </summary>
        /// <param name="category">Command category to check.</param>
        /// <returns>true if user is allowed to access the given category, otherwise false.</returns>
        public bool CanAccessCategory(CommandCategory.Flag category)
        {
            return (_categories & (uint)category) == (uint)category;
        }

        /// <summary>
        /// Adds the given category to the user.
        /// </summary>
        /// <param name="category">Bit flag of the category to add.</param>
        public void AddCategory(CommandCategory.Flag category)
        {
            uint oldCategories;
            do
            {
                oldCategories = _categories;
            }
            while (oldCategories != Interlocked.CompareExchange(ref this._categories, oldCategories | (uint)category, oldCategories));
        }

        /// <summary>
        /// Removes the given category from the user.
        /// </summary>
        /// <param name="category">Bit flag of the category to remove.</param>
        public void RemoveCategory(CommandCategory.Flag category)
        {
            uint oldCategories;
            do
            {
                oldCategories = _categories;
            }
            while (oldCategories != Interlocked.CompareExchange(ref this._categories, oldCategories & ~((uint)category), oldCategories));
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
            _categories = (uint)CommandCategory.Flag.None;

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
            lock (_passwordHashes)
            {
                foreach (ACLPassword hash in _passwordHashes)
                {
                    if (password.Equals(hash))
                    {
                        return true;
                    }
                }
            }

            return false;
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

            // Categories
            var highestFlag = Enum.GetValues(typeof(CommandCategory.Flag)).Cast<int>().Max();

            for (int i = 0; i <= highestFlag; i++)
            {
                CommandCategory.Flag flag = (CommandCategory.Flag)(1 << i);
                if (this.CanAccessCategory(flag))
                {
                    stringBuilder.Append($" +@{CommandCategory.GetNameByFlag(flag)}");
                }
            }

            return stringBuilder.ToString();
        }

        /// <summary>
        /// Categories enabled for the user
        /// </summary>
        uint _categories;

        /// <summary>
        /// A set of all allowed _passwordHashes for the user.
        /// 
        /// NOTE: HashSet is not thread-safe, so accesses need to be synchronized
        /// </summary>
        readonly HashSet<ACLPassword> _passwordHashes = [];
    }
}