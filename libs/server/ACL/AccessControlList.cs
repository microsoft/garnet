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
        /// Dictionary containing all <see cref="UserHandle"/>s defined in the ACL.
        /// </summary>
        ConcurrentDictionary<string, UserHandle> _userHandles = new();

        /// <summary>
        /// The <see cref="UserHandle"/> for the currently configured default user (for fast default lookups).
        /// </summary>
        UserHandle _defaultUserHandle;

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
                _defaultUserHandle = CreateDefaultUserHandle(defaultPassword);
            }
        }

        /// <summary>
        /// Returns the <see cref="UserHandle"/> with the given name.
        /// </summary>
        /// <param name="username">Username of the user to retrieve.</param>
        /// <returns>Matching user object, or null if no user with the given name was found.</returns>
        public UserHandle GetUserHandle(string username)
        {
            if (_userHandles.TryGetValue(username, out var userHandle))
            {
                return userHandle;
            }
            return null;
        }

        /// <summary>
        /// Returns the currently configured default <see cref="UserHandle"/>.
        /// </summary>
        /// <returns>The default user of this access control list.</returns>
        public UserHandle GetDefaultUserHandle()
        {
            return _defaultUserHandle;
        }

        /// <summary>
        /// Adds the given <see cref="UserHandle"/> to the ACL.
        /// </summary>
        /// <param name="userHandle">User to add to the list.</param>
        /// <exception cref="ACLUserAlreadyExistsException">Thrown if a user with the given username already exists.</exception>
        public void AddUserHandle(UserHandle userHandle)
        {
            var username = userHandle?.User.Name;

            // If a user with the given name already exists in the ACL, the new user cannot be added
            if (!_userHandles.TryAdd(username, userHandle))
            {
                throw new ACLUserAlreadyExistsException(username);
            }
        }

        /// <summary>
        /// Deletes the <see cref="UserHandle"/> associated with the given username.
        /// </summary>
        /// <param name="username">Username of the user to delete.</param>
        /// <returns>true if successful, false if no matching user was found.</returns>
        /// <exception cref="ACLException">Thrown if the given user exists but cannot be deleted.</exception>
        public bool DeleteUserHandle(string username)
        {
            if (username == DefaultUserName)
            {
                throw new ACLException("The special 'default' user cannot be removed from the system");
            }
            return _userHandles.TryRemove(username, out _);
        }

        /// <summary>
        /// Remove all <see cref="UserHandle"/>s from the list.
        /// </summary>
        public void ClearUsers()
        {
            _userHandles.Clear();
        }

        /// <summary>
        /// Return a list of all usernames and user objects.
        /// </summary>
        /// <returns>Dictionary of username/user pairs.</returns>
        public IReadOnlyDictionary<string, UserHandle> GetUserHandles()
        {
            return _userHandles;
        }

        /// <summary>
        /// Creates the default user, if it does not exist yet.
        /// </summary>
        /// <param name="defaultPassword">Password to use if new user is created.</param>
        /// <returns>The newly created or already existing default user.</returns>
        UserHandle CreateDefaultUserHandle(string defaultPassword = "")
        {
            UserHandle defaultUserHandle;

            while (!_userHandles.TryGetValue(DefaultUserName, out defaultUserHandle))
            {
                // Default user always has full access
                User defaultUser = new User(DefaultUserName);
                defaultUser.AddCategory(RespAclCategories.All);

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

                defaultUserHandle = new UserHandle(defaultUser);
                // Add the user to the user list
                try
                {
                    AddUserHandle(defaultUserHandle);
                    break;
                }
                catch (ACLUserAlreadyExistsException)
                {
                    // If AddUser failed, continue looping to retrieve the concurrently created user
                }
            }
            return defaultUserHandle;
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
                acl._userHandles.Clear();
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
            _defaultUserHandle = acl.CreateDefaultUserHandle(defaultPassword);

            // Atomically replace the user list
            _userHandles = acl._userHandles;
        }

        /// <summary>
        /// Save current
        /// </summary>
        /// <param name="aclConfigurationFile"></param>
        public void Save(string aclConfigurationFile)
        {
            if (string.IsNullOrEmpty(aclConfigurationFile))
            {
                throw new ACLException($"ACL configuration file not set.");
            }

            // Lock to ensure one flush at a time
            lock (this)
            {
                StreamWriter streamWriter = null;
                try
                {
                    // Initialize so as to allow the streamwriter buffer to fill in memory and do a manual flush afterwards
                    streamWriter = new StreamWriter(path: aclConfigurationFile, append: false, encoding: Encoding.UTF8, bufferSize: 1 << 16)
                    {
                        AutoFlush = false
                    };

                    // Write lines into buffer
                    foreach (var userHandle in _userHandles)
                        streamWriter.WriteLine(userHandle.Value.User.DescribeUser());

                    // Flush data buffer
                    streamWriter.Flush();
                }
                finally
                {
                    // Finally ensure streamWriter is closed
                    streamWriter?.Close();
                }
            }
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