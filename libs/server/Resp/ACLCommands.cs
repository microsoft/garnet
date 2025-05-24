// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Garnet.server.ACL;
using Garnet.server.Auth;
using Garnet.server.Auth.Settings;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Server session for RESP protocol - ACL Commands
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        private bool ValidateACLAuthenticator()
        {
            if (_authenticator is null or not GarnetACLAuthenticator)
            {
                WriteError(CmdStrings.RESP_ERR_ACL_AUTH_DISABLED);
                return false;
            }
            return true;
        }

        private bool ValidateACLFileUse()
        {
            if (storeWrapper.serverOptions.AuthSettings is not AclAuthenticationSettings)
            {
                WriteError(CmdStrings.RESP_ERR_ACL_AUTH_DISABLED);
                return false;
            }

            var aclAuthenticationSettings = (AclAuthenticationSettings)storeWrapper.serverOptions.AuthSettings;
            if (aclAuthenticationSettings.AclConfigurationFile == null)
            {
                WriteError(CmdStrings.RESP_ERR_ACL_AUTH_FILE_DISABLED);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Processes ACL LIST subcommand.
        /// </summary>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool NetworkAclList()
        {
            // No additional args allowed
            if (parseState.Count != 0)
            {
                WriteError($"ERR Unknown subcommand or wrong number of arguments for ACL LIST.");
            }
            else
            {
                if (!ValidateACLAuthenticator())
                    return true;

                var aclAuthenticator = (GarnetACLAuthenticator)_authenticator;
                var userHandles = aclAuthenticator.GetAccessControlList().GetUserHandles();
                WriteArrayLength(userHandles.Count);

                foreach (var userHandle in userHandles)
                {
                    WriteAsciiBulkString(userHandle.Value.User.DescribeUser());
                }
            }

            return true;
        }

        /// <summary>
        /// Processes ACL USERS subcommand.
        /// </summary>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool NetworkAclUsers()
        {
            // No additional args allowed
            if (parseState.Count != 0)
            {
                WriteError($"ERR Unknown subcommand or wrong number of arguments for ACL USERS.");
            }
            else
            {
                if (!ValidateACLAuthenticator())
                    return true;

                var aclAuthenticator = (GarnetACLAuthenticator)_authenticator;
                var users = aclAuthenticator.GetAccessControlList().GetUserHandles();
                WriteArrayLength(users.Count);

                foreach (var user in users)
                {
                    WriteAsciiBulkString(user.Key);
                }
            }

            return true;
        }

        /// <summary>
        /// Processes ACL CAT subcommand.
        /// </summary>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool NetworkAclCat()
        {
            // No additional args allowed
            if (parseState.Count != 0)
            {
                WriteError($"ERR Unknown subcommand or wrong number of arguments for ACL CAT.");
            }
            else
            {
                if (!ValidateACLAuthenticator())
                    return true;

                var categories = ACLParser.ListCategories();
                WriteArrayLength(categories.Count);

                foreach (var category in categories)
                {
                    WriteAsciiBulkString(category);
                }
            }

            return true;
        }

        /// <summary>
        /// Processes ACL SETUSER subcommand.
        /// </summary>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool NetworkAclSetUser()
        {
            // Have to have at least the username
            if (parseState.Count == 0)
            {
                WriteError($"ERR Unknown subcommand or wrong number of arguments for ACL SETUSER.");
            }
            else
            {
                if (!ValidateACLAuthenticator())
                    return true;

                var aclAuthenticator = (GarnetACLAuthenticator)_authenticator;

                // REQUIRED: username
                var username = parseState.GetString(0);

                var ops = new string[parseState.Count];
                for (var i = 1; i < parseState.Count; i++)
                {
                    ops[i] = parseState.GetString(i);
                }

                try
                {
                    // Modify or create the user with the given username
                    var userHandle = aclAuthenticator.GetAccessControlList().GetUserHandle(username);

                    if (userHandle == null)
                    {
                        userHandle = new UserHandle(new User(username));

                        try
                        {
                            aclAuthenticator.GetAccessControlList().AddUserHandle(userHandle);
                        }
                        catch (ACLUserAlreadyExistsException)
                        {
                            // If AddUser failed, retrieve the concurrently created user
                            userHandle = aclAuthenticator.GetAccessControlList().GetUserHandle(username);
                        }
                    }

                    User newUser;
                    User currentUser;
                    do
                    {
                        // Modifications to user permissions must be performed against the effective user.
                        currentUser = userHandle.User;
                        newUser = new User(currentUser);

                        // Remaining parameters are ACL operations
                        for (var i = 1; i < ops.Length; i++)
                        {
                            ACLParser.ApplyACLOpToUser(ref newUser, ops[i]);
                        }

                    }
                    while (!userHandle.TrySetUser(newUser, currentUser));
                }
                catch (ACLException exception)
                {
                    // Abort command execution
                    WriteError($"ERR {exception.Message}");

                    return true;
                }

                WriteOK();
            }

            return true;
        }

        /// <summary>
        /// Processes ACL DELUSER subcommand.
        /// </summary>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool NetworkAclDelUser()
        {
            // Have to have at least the username
            if (parseState.Count == 0)
            {
                WriteError($"ERR Unknown subcommand or wrong number of arguments for ACL DELUSER.");
            }
            else
            {
                if (!ValidateACLAuthenticator())
                    return true;

                var aclAuthenticator = (GarnetACLAuthenticator)_authenticator;
                var successfulDeletes = 0;

                try
                {
                    // Attempt to delete the users with the given names
                    for (var i = 0; i < parseState.Count; i++)
                    {
                        var username = parseState.GetString(i);

                        if (aclAuthenticator.GetAccessControlList().DeleteUserHandle(username))
                        {
                            successfulDeletes += 1;
                        }
                    }
                }
                catch (ACLException exception)
                {
                    logger?.LogDebug("ACLException: {message}", exception.Message);

                    // Abort command execution
                    WriteError($"ERR {exception.Message}");

                    return true;
                }

                // Return the number of successful deletes
                WriteInt32(successfulDeletes);
            }

            return true;
        }

        /// <summary>
        /// Processes ACL WHOAMI subcommand.
        /// </summary>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool NetworkAclWhoAmI()
        {
            // No additional args allowed
            if (parseState.Count != 0)
            {
                WriteError($"ERR Unknown subcommand or wrong number of arguments for ACL WHOAMI.");
            }
            else
            {
                if (!ValidateACLAuthenticator())
                    return true;

                var aclAuthenticator = (GarnetACLAuthenticator)_authenticator;

                // Return the name of the currently authenticated user.
                Debug.Assert(aclAuthenticator.GetUserHandle()?.User != null);

                WriteSimpleString(aclAuthenticator.GetUserHandle().User.Name);
            }

            return true;
        }

        /// <summary>
        /// Processes ACL LOAD subcommand.
        /// </summary>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool NetworkAclLoad()
        {
            // No additional args allowed
            if (parseState.Count != 0)
            {
                WriteError($"ERR Unknown subcommand or wrong number of arguments for ACL LOAD.");
            }
            else
            {
                if (!ValidateACLAuthenticator())
                    return true;

                if (!ValidateACLFileUse())
                    return true;

                // NOTE: This is temporary as long as ACL operations are only supported when using the ACL authenticator
                Debug.Assert(storeWrapper.serverOptions.AuthSettings != null);
                Debug.Assert(storeWrapper.serverOptions.AuthSettings.GetType().BaseType == typeof(AclAuthenticationSettings));
                var aclAuthenticationSettings = (AclAuthenticationSettings)storeWrapper.serverOptions.AuthSettings;

                // Try to reload the configured ACL configuration file
                try
                {
                    logger?.LogInformation("Reading updated ACL configuration file '{filepath}'", aclAuthenticationSettings.AclConfigurationFile);
                    storeWrapper.accessControlList.Load(aclAuthenticationSettings.DefaultPassword, aclAuthenticationSettings.AclConfigurationFile);

                    WriteOK();
                }
                catch (ACLException exception)
                {
                    WriteError($"ERR {exception.Message}");
                }
            }

            return true;
        }

        /// <summary>
        /// Processes ACL SAVE subcommand.
        /// </summary>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool NetworkAclSave()
        {
            if (parseState.Count != 0)
            {
                WriteError($"ERR Unknown subcommand or wrong number of arguments for ACL SAVE.");
            }

            if (!ValidateACLAuthenticator())
                return true;

            if (!ValidateACLFileUse())
                return true;

            // NOTE: This is temporary as long as ACL operations are only supported when using the ACL authenticator
            Debug.Assert(storeWrapper.serverOptions.AuthSettings != null);
            Debug.Assert(storeWrapper.serverOptions.AuthSettings.GetType().BaseType == typeof(AclAuthenticationSettings));
            var aclAuthenticationSettings = (AclAuthenticationSettings)storeWrapper.serverOptions.AuthSettings;

            try
            {
                storeWrapper.accessControlList.Save(aclAuthenticationSettings.AclConfigurationFile);
                logger?.LogInformation("ACL configuration file '{filepath}' saved!", aclAuthenticationSettings.AclConfigurationFile);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "ACL SAVE faulted");
                WriteError($"ERR {ex.Message}");

                return true;
            }

            WriteOK();
            return true;
        }

        /// <summary>
        /// Processes ACL GETUSER subcommand.
        /// </summary>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool NetworkAclGetUser()
        {
            // The username must be provided.
            if (parseState.Count != 1)
            {
                WriteError($"ERR Unknown subcommand or wrong number of arguments for ACL GETUSER.");
            }
            else
            {
                if (!ValidateACLAuthenticator())
                    return true;

                var aclAuthenticator = (GarnetACLAuthenticator)_authenticator;
                User user = null;

                try
                {
                    var userHandle = aclAuthenticator
                        .GetAccessControlList()
                        .GetUserHandle(parseState.GetString(0));

                    if (userHandle != null)
                    {
                        user = new User(userHandle.User);
                    }
                }
                catch (ACLException exception)
                {
                    // Abort command execution
                    WriteError($"ERR {exception.Message}");

                    return true;
                }

                if (user is null)
                {
                    WriteNull();
                }
                else
                {
                    WriteMapLength(3);

                    WriteAsciiBulkString("flags");

                    WriteSetLength(1);

                    WriteAsciiBulkString(user.IsEnabled ? "on" : "off");

                    var passwords = user.Passwords;
                    WriteAsciiBulkString("passwords");

                    WriteArrayLength(passwords.Count);

                    foreach (var password in passwords)
                    {
                        WriteAsciiBulkString($"#{password.ToString()}");
                    }

                    WriteAsciiBulkString("commands");

                    WriteAsciiBulkString(user.GetEnabledCommandsDescription());
                }
            }

            return true;
        }
    }
}