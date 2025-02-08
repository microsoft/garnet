// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Garnet.common;
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
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_ACL_AUTH_DISABLED, ref dcurr, dend))
                    SendAndReset();
                return false;
            }
            return true;
        }

        private bool ValidateACLFileUse()
        {
            if (storeWrapper.serverOptions.AuthSettings is not AclAuthenticationSettings)
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_ACL_AUTH_DISABLED, ref dcurr, dend))
                    SendAndReset();
                return false;
            }

            var aclAuthenticationSettings = (AclAuthenticationSettings)storeWrapper.serverOptions.AuthSettings;
            if (aclAuthenticationSettings.AclConfigurationFile == null)
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_ACL_AUTH_FILE_DISABLED, ref dcurr, dend))
                    SendAndReset();
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
                while (!RespWriteUtils.TryWriteError($"ERR Unknown subcommand or wrong number of arguments for ACL LIST.", ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                if (!ValidateACLAuthenticator())
                    return true;

                var aclAuthenticator = (GarnetACLAuthenticator)_authenticator;
                var users = aclAuthenticator.GetAccessControlList().GetUsers();
                while (!RespWriteUtils.TryWriteArrayLength(users.Count, ref dcurr, dend))
                    SendAndReset();

                foreach (var user in users)
                {
                    while (!RespWriteUtils.TryWriteAsciiBulkString(user.Value.GetEffectiveUser().DescribeUser(), ref dcurr, dend))
                        SendAndReset();
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
                while (!RespWriteUtils.TryWriteError($"ERR Unknown subcommand or wrong number of arguments for ACL USERS.", ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                if (!ValidateACLAuthenticator())
                    return true;

                var aclAuthenticator = (GarnetACLAuthenticator)_authenticator;
                var users = aclAuthenticator.GetAccessControlList().GetUsers();
                while (!RespWriteUtils.TryWriteArrayLength(users.Count, ref dcurr, dend))
                    SendAndReset();

                foreach (var user in users)
                {
                    while (!RespWriteUtils.TryWriteAsciiBulkString(user.Key, ref dcurr, dend))
                        SendAndReset();
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
                while (!RespWriteUtils.TryWriteError($"ERR Unknown subcommand or wrong number of arguments for ACL CAT.", ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                if (!ValidateACLAuthenticator())
                    return true;

                var categories = ACLParser.ListCategories();
                RespWriteUtils.TryWriteArrayLength(categories.Count, ref dcurr, dend);

                foreach (var category in categories)
                {
                    while (!RespWriteUtils.TryWriteAsciiBulkString(category, ref dcurr, dend))
                        SendAndReset();
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
                while (!RespWriteUtils.TryWriteError($"ERR Unknown subcommand or wrong number of arguments for ACL SETUSER.", ref dcurr, dend))
                    SendAndReset();
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
                    var user = aclAuthenticator.GetAccessControlList().GetUser(username);

                    if (user == null)
                    {
                        user = new User(username);

                        try
                        {
                            aclAuthenticator.GetAccessControlList().AddUser(user);
                        }
                        catch (ACLUserAlreadyExistsException)
                        {
                            // If AddUser failed, retrieve the concurrently created user
                            user = aclAuthenticator.GetAccessControlList().GetUser(username);
                        }
                    }

                    User newUser;
                    User effectiveUser;
                    do
                    {
                        // Modifications to user permissions must be performed against the effective user.
                        effectiveUser = user.GetEffectiveUser();
                        newUser = new User(effectiveUser);

                        // Remaining parameters are ACL operations
                        for (var i = 1; i < ops.Length; i++)
                        {
                            ACLParser.ApplyACLOpToUser(ref newUser, ops[i]);
                        }

                    } while(!user.TrySetEffectiveUser(newUser, effectiveUser));
                }
                catch (ACLException exception)
                {
                    // Abort command execution
                    while (!RespWriteUtils.TryWriteError($"ERR {exception.Message}", ref dcurr, dend))
                        SendAndReset();

                    return true;
                }

                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
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
                while (!RespWriteUtils.TryWriteError($"ERR Unknown subcommand or wrong number of arguments for ACL DELUSER.", ref dcurr, dend))
                    SendAndReset();
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

                        if (aclAuthenticator.GetAccessControlList().DeleteUser(username))
                        {
                            successfulDeletes += 1;
                        }
                    }
                }
                catch (ACLException exception)
                {
                    logger?.LogDebug("ACLException: {message}", exception.Message);

                    // Abort command execution
                    while (!RespWriteUtils.TryWriteError($"ERR {exception.Message}", ref dcurr, dend))
                        SendAndReset();

                    return true;
                }

                // Return the number of successful deletes
                while (!RespWriteUtils.TryWriteInt32(successfulDeletes, ref dcurr, dend))
                    SendAndReset();
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
                while (!RespWriteUtils.TryWriteError($"ERR Unknown subcommand or wrong number of arguments for ACL WHOAMI.", ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                if (!ValidateACLAuthenticator())
                    return true;

                var aclAuthenticator = (GarnetACLAuthenticator)_authenticator;

                // Return the name of the currently authenticated user.
                Debug.Assert(aclAuthenticator.GetUser() != null);

                while (!RespWriteUtils.TryWriteSimpleString(aclAuthenticator.GetUser().Name, ref dcurr, dend))
                    SendAndReset();
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
                while (!RespWriteUtils.TryWriteError($"ERR Unknown subcommand or wrong number of arguments for ACL LOAD.", ref dcurr, dend))
                    SendAndReset();
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

                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
                catch (ACLException exception)
                {
                    while (!RespWriteUtils.TryWriteError($"ERR {exception.Message}", ref dcurr, dend))
                        SendAndReset();
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
                while (!RespWriteUtils.TryWriteError($"ERR Unknown subcommand or wrong number of arguments for ACL SAVE.", ref dcurr, dend))
                    SendAndReset();
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
                while (!RespWriteUtils.TryWriteError($"ERR {ex.Message}", ref dcurr, dend))
                    SendAndReset();

                return true;
            }

            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }
    }
}