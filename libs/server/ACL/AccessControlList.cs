// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Garnet.server.ACL
{
    /// <summary>
    /// Models the high-level Access Control List (ACL) that defines access and command limitations for Garnet users.
    /// </summary>
    public class AccessControlList
    {
        /// <summary>
        /// Username to use for the default user
        /// </summary>
        const string DefaultUserName = "default";

        /// <summary>
        /// Dictionary containing all users defined in the ACL
        /// </summary>
        ConcurrentDictionary<string, User> _users = new();

        /// <summary>
        /// The currently configured default user (for fast default lookups)
        /// </summary>
        User _defaultUser;

        /// <summary>
        /// Creates a new Access Control List from an optional ACL configuration file
        /// and sets the default user's password, if not provided by the configuration.
        /// </summary>
        /// <param name="defaultPassword">The password for the default user (if not provided by configuration file).</param>
        /// <param name="aclConfigurationFile">ACL configuration file.</param>
        /// <exception cref="ACLException">Thrown if configuration file cannot be parsed.</exception>
        public AccessControlList(string defaultPassword = "", string aclConfigurationFile = null)
        {
            if (!string.IsNullOrEmpty(aclConfigurationFile))
            {
                // Attempt to load ACL configuration file
                Load(defaultPassword, aclConfigurationFile);
            }
            else
            {
                // If no ACL file is defined, only create the default user
                _defaultUser = CreateDefaultUser(defaultPassword);
            }
        }

        /// <summary>
        /// Returns the user with the given name.
        /// </summary>
        /// <param name="username">Username of the user to retrieve.</param>
        /// <returns>Matching user object, or null if no user with the given name was found.</returns>
        public User GetUser(string username)
        {
            if (_users.TryGetValue(username, out var user))
            {
                return user;
            }
            return null;
        }

        /// <summary>
        /// Returns the currently configured default user.
        /// </summary>
        /// <returns>The default user of this access control list.</returns>
        public User GetDefaultUser()
        {
            return _defaultUser;
        }

        /// <summary>
        /// Adds the given user to the ACL.
        /// </summary>
        /// <param name="user">User to add to the list.</param>
        /// <exception cref="ACLUserAlreadyExistsException">Thrown if a user with the given username already exists.</exception>
        public void AddUser(User user)
        {
            // If a user with the given name already exists in the ACL, the new user cannot be added
            if (!_users.TryAdd(user.Name, user))
            {
                throw new ACLUserAlreadyExistsException(user.Name);
            }
        }

        /// <summary>
        /// Deletes the user associated with the given username.
        /// </summary>
        /// <param name="username">Username of the user to delete.</param>
        /// <returns>true if successful, false if no matching user was found.</returns>
        /// <exception cref="ACLException">Thrown if the given user exists but cannot be deleted.</exception>
        public bool DeleteUser(string username)
        {
            if (username == DefaultUserName)
            {
                throw new ACLException("The special 'default' user cannot be removed from the system");
            }
            return _users.TryRemove(username, out _);
        }

        /// <summary>
        /// Remove all users from the list.
        /// </summary>
        public void ClearUsers()
        {
            _users.Clear();
        }

        /// <summary>
        /// Return a list of all usernames and user objects.
        /// </summary>
        /// <returns>Dictionary of username/user pairs.</returns>
        public IReadOnlyDictionary<string, User> GetUsers()
        {
            return _users;
        }

        /// <summary>
        /// Creates the default user, if it does not exist yet.
        /// </summary>
        /// <param name="defaultPassword">Password to use if new user is created.</param>
        /// <returns>The newly created or already existing default user.</returns>
        User CreateDefaultUser(string defaultPassword = "")
        {
            User defaultUser;

            while (!_users.TryGetValue(DefaultUserName, out defaultUser))
            {
                // Default user is always admin
                defaultUser = new User(DefaultUserName);
                defaultUser.AddCategory(CommandCategory.Flag.Admin);

                // Automatically created default users are always enabled
                defaultUser.IsEnabled = true;

                // Set the password if requested
                if (!string.IsNullOrEmpty(defaultPassword))
                {
                    ACLPassword password = ACLPassword.ACLPasswordFromString(defaultPassword);
                    defaultUser.AddPasswordHash(password);
                }
                else
                {
                    defaultUser.IsPasswordless = true;
                }

                // Add the user to the user list
                try
                {
                    AddUser(defaultUser);
                    break;
                }
                catch (ACLUserAlreadyExistsException)
                {
                    // If AddUser failed, continue looping to retrieve the concurrently created user
                }
            }
            return defaultUser;
        }

        /// <summary>
        /// Loads the given ACL configuration file and replaces all currently defined rules in this ACL.
        /// If the given ACL file contains errors, the old rules remain unmodified.
        /// </summary>
        /// <param name="defaultPassword">The password for the default user (if not defined in ACL configuration file)</param>
        /// <param name="aclConfigurationFile">ACL configuration file.</param>
        /// <exception cref="ACLException">Thrown if configuration file cannot be parsed.</exception>
        public void Load(string defaultPassword, string aclConfigurationFile)
        {
            // Attempt to load ACL configuration file
            if (!File.Exists(aclConfigurationFile))
            {
                throw new ACLException($"Cannot find ACL configuration file '{aclConfigurationFile}'");
            }

            // Import file into a new temporary access control list to guarantee atomicity
            AccessControlList acl = new();
            StreamReader streamReader;

            try
            {
                streamReader = new StreamReader(File.OpenRead(aclConfigurationFile), Encoding.UTF8, true);
            }
            catch
            {
                throw new ACLException($"Unable to open ACL configuration file '{aclConfigurationFile}'");
            }

            // Remove default user and load statements
            try
            {
                acl._users.Clear();
                acl.Import(streamReader, aclConfigurationFile);
            }
            catch (ACLParsingException exception)
            {
                throw new ACLException($"Unable to parse ACL rule {exception.Filename}:{exception.Line}:  {exception.Message}");
            }
            finally
            {
                streamReader.Close();
            }

            // Add back default user and update the cached default user handle
            _defaultUser = acl.CreateDefaultUser(defaultPassword);

            // Atomically replace the user list
            _users = acl._users;
        }

        /// <summary>
        /// Imports Access Control List rules from the given reader.
        /// </summary>
        /// <param name="input">Input text reader to a list of ACL user definition rules.</param>
        /// <param name="configurationFile">Configuration file identifier for clean debug messages.</param>
        /// <exception cref="ACLParsingException">Thrown if ACL rules cannot be parsed.</exception>
        void Import(StreamReader input, string configurationFile = "<undefined>")
        {
            // Read and parse input line-by-line
            string line;
            int curLine = 0;
            while ((line = input.ReadLine()) != null)
            {
                curLine++;

                // Skip empty lines and comments
                line = line.Trim();
                if (line.Length < 1 || line.StartsWith('#'))
                {
                    continue;
                }

                // Parse the ACL rules stored in the line
                try
                {
                    ACLParser.ParseACLRule(line, this);
                }
                catch (ACLException exception)
                {
                    throw new ACLParsingException(exception.Message, configurationFile, curLine);
                }
            }
        }
    }
}