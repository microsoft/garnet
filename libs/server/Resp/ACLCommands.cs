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
        private bool IsACLAuthenticatorEnabled()
        {
            if (_authenticator is null or not GarnetACLAuthenticator)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_ACL_AUTH_DISABLED, ref dcurr, dend))
                    SendAndReset();
                return false;
            }
            return true;
        }

        private bool CanUseAclFile()
        {
            if (storeWrapper.serverOptions.AuthSettings is not AclAuthenticationSettings)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_ACL_AUTH_DISABLED, ref dcurr, dend))
                    SendAndReset();
                return false;
            }

            var aclAuthenticationSettings = (AclAuthenticationSettings)storeWrapper.serverOptions.AuthSettings;
            if (aclAuthenticationSettings.AclConfigurationFile == null)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_ACL_AUTH_FILE_DISABLED, ref dcurr, dend))
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
                while (!RespWriteUtils.WriteError($"ERR Unknown subcommand or wrong number of arguments for ACL LIST.", ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                if (!IsACLAuthenticatorEnabled())
                    return true;

                var aclAuthenticator = (GarnetACLAuthenticator)_authenticator;
                var users = aclAuthenticator.GetAccessControlList().GetUsers();
                while (!RespWriteUtils.WriteArrayLength(users.Count, ref dcurr, dend))
                    SendAndReset();

                foreach (var user in users)
                {
                    while (!RespWriteUtils.WriteAsciiBulkString(user.Value.DescribeUser(), ref dcurr, dend))
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
                while (!RespWriteUtils.WriteError($"ERR Unknown subcommand or wrong number of arguments for ACL USERS.", ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                if (!IsACLAuthenticatorEnabled())
                    return true;

                var aclAuthenticator = (GarnetACLAuthenticator)_authenticator;
                var users = aclAuthenticator.GetAccessControlList().GetUsers();
                while (!RespWriteUtils.WriteArrayLength(users.Count, ref dcurr, dend))
                    SendAndReset();

                foreach (var user in users)
                {
                    while (!RespWriteUtils.WriteAsciiBulkString(user.Key, ref dcurr, dend))
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
                while (!RespWriteUtils.WriteError($"ERR Unknown subcommand or wrong number of arguments for ACL CAT.", ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                if (!IsACLAuthenticatorEnabled())
                    return true;

                var categories = ACLParser.ListCategories();
                RespWriteUtils.WriteArrayLength(categories.Count, ref dcurr, dend);

                foreach (var category in categories)
                {
                    while (!RespWriteUtils.WriteAsciiBulkString(category, ref dcurr, dend))
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
                while (!RespWriteUtils.WriteError($"ERR Unknown subcommand or wrong number of arguments for ACL SETUSER.", ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                if (!IsACLAuthenticatorEnabled())
                    return true;

                var aclAuthenticator = (GarnetACLAuthenticator)_authenticator;

                // REQUIRED: username
                var username = parseState.GetString(0);

                // Modify or create the user with the given username
                var user = aclAuthenticator.GetAccessControlList().GetUser(username);

                try
                {
                    if (user == null)
                    {
                        user = new User(username);
                        aclAuthenticator.GetAccessControlList().AddUser(user);
                    }

                    // Remaining parameters are ACL operations
                    for (var i = 1; i < parseState.Count; i++)
                    {
                        var op = parseState.GetString(i);
                        ACLParser.ApplyACLOpToUser(ref user, op);
                    }
                }
                catch (ACLException exception)
                {
                    logger?.LogDebug("ACLException: {message}", exception.Message);

                    // Abort command execution
                    while (!RespWriteUtils.WriteError($"ERR {exception.Message}", ref dcurr, dend))
                        SendAndReset();

                    return true;
                }

                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
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
                while (!RespWriteUtils.WriteError($"ERR Unknown subcommand or wrong number of arguments for ACL DELUSER.", ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                if (!IsACLAuthenticatorEnabled())
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
                    while (!RespWriteUtils.WriteError($"ERR {exception.Message}", ref dcurr, dend))
                        SendAndReset();

                    return true;
                }

                // Return the number of successful deletes
                while (!RespWriteUtils.WriteInteger(successfulDeletes, ref dcurr, dend))
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
                while (!RespWriteUtils.WriteError($"ERR Unknown subcommand or wrong number of arguments for ACL WHOAMI.", ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                if (!IsACLAuthenticatorEnabled())
                    return true;

                var aclAuthenticator = (GarnetACLAuthenticator)_authenticator;

                // Return the name of the currently authenticated user.
                Debug.Assert(aclAuthenticator.GetUser() != null);

                while (!RespWriteUtils.WriteSimpleString(aclAuthenticator.GetUser().Name, ref dcurr, dend))
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
                while (!RespWriteUtils.WriteError($"ERR Unknown subcommand or wrong number of arguments for ACL LOAD.", ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                if (!IsACLAuthenticatorEnabled())
                    return true;

                if (!CanUseAclFile())
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

                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
                catch (ACLException exception)
                {
                    while (!RespWriteUtils.WriteError($"ERR {exception.Message}", ref dcurr, dend))
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
                while (!RespWriteUtils.WriteError($"ERR Unknown subcommand or wrong number of arguments for ACL SAVE.", ref dcurr, dend))
                    SendAndReset();
            }

            if (!IsACLAuthenticatorEnabled())
                return true;

            if (!CanUseAclFile())
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
                while (!RespWriteUtils.WriteError($"ERR {ex.Message}", ref dcurr, dend))
                    SendAndReset();

                return true;
            }

            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }
    }
}