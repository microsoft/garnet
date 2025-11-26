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
                return AbortWithWrongNumberOfArguments("acl|list");
            }

            if (!ValidateACLAuthenticator())
                return true;

            var aclAuthenticator = (GarnetACLAuthenticator)_authenticator;
            var userHandles = aclAuthenticator.GetAccessControlList().GetUserHandles();
            while (!RespWriteUtils.TryWriteArrayLength(userHandles.Count, ref dcurr, dend))
                SendAndReset();

            foreach (var userHandle in userHandles)
            {
                while (!RespWriteUtils.TryWriteAsciiBulkString(userHandle.Value.User.DescribeUser(), ref dcurr, dend))
                    SendAndReset();
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
                return AbortWithWrongNumberOfArguments("acl|users");
            }

            if (!ValidateACLAuthenticator())
                return true;

            var aclAuthenticator = (GarnetACLAuthenticator)_authenticator;
            var users = aclAuthenticator.GetAccessControlList().GetUserHandles();
            while (!RespWriteUtils.TryWriteArrayLength(users.Count, ref dcurr, dend))
                SendAndReset();

            foreach (var user in users)
            {
                while (!RespWriteUtils.TryWriteAsciiBulkString(user.Key, ref dcurr, dend))
                    SendAndReset();
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
                return AbortWithErrorMessage($"ERR Unknown subcommand or wrong number of arguments for ACL CAT.");
            }

            if (!ValidateACLAuthenticator())
                return true;

            var categories = ACLParser.ListCategories();
            RespWriteUtils.TryWriteArrayLength(categories.Count, ref dcurr, dend);

            foreach (var category in categories)
            {
                while (!RespWriteUtils.TryWriteAsciiBulkString(category, ref dcurr, dend))
                    SendAndReset();
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
                return AbortWithWrongNumberOfArguments("acl|setuser");
            }

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
                while (!RespWriteUtils.TryWriteError($"ERR {exception.Message}", ref dcurr, dend))
                    SendAndReset();

                return true;
            }

            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

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
                return AbortWithWrongNumberOfArguments("acl|deluser");
            }

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
                while (!RespWriteUtils.TryWriteError($"ERR {exception.Message}", ref dcurr, dend))
                    SendAndReset();

                return true;
            }

            // Return the number of successful deletes
            while (!RespWriteUtils.TryWriteInt32(successfulDeletes, ref dcurr, dend))
                SendAndReset();

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
                return AbortWithWrongNumberOfArguments("acl|whoami");
            }

            if (!ValidateACLAuthenticator())
                return true;

            var aclAuthenticator = (GarnetACLAuthenticator)_authenticator;

            // Return the name of the currently authenticated user.
            Debug.Assert(aclAuthenticator.GetUserHandle()?.User != null);

            WriteAsciiBulkString(aclAuthenticator.GetUserHandle().User.Name);

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
                return AbortWithWrongNumberOfArguments("acl|load");
            }

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
                return AbortWithWrongNumberOfArguments("acl|save");
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

        /// <summary>
        /// Processes ACL GENPASS subcommand.
        /// </summary>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool NetworkAclGenPass()
        {
            if (parseState.Count > 1)
            {
                return AbortWithWrongNumberOfArguments("acl|genpass");
            }

            // Default length
            var length = 64;
            if (parseState.Count == 1)
            {
                if (!parseState.TryGetLong(0, out var bits))
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
                }

                if ((bits <= 0) || (bits > 4096))
                {
                    return AbortWithErrorMessage("ERR ACL GENPASS argument must be the number of bits for the output password, a positive number up to 4096"u8);
                }

                // Bits gets rounded to the next multiple of 4 and converted to bytes to get the password length
                length = (int)(bits / 4) + (bits % 4 == 0 ? 0 : 1);
            }

            WriteAsciiBulkString(System.Security.Cryptography.RandomNumberGenerator.GetHexString(length, true));
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
                return AbortWithWrongNumberOfArguments("acl|getuser");
            }

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
                while (!RespWriteUtils.TryWriteError($"ERR {exception.Message}", ref dcurr, dend))
                    SendAndReset();

                return true;
            }

            if (user is null)
            {
                WriteNull();
            }
            else
            {
                WriteMapLength(3);

                while (!RespWriteUtils.TryWriteAsciiBulkString("flags", ref dcurr, dend))
                    SendAndReset();

                WriteSetLength(1);

                while (!RespWriteUtils.TryWriteAsciiBulkString(user.IsEnabled ? "on" : "off", ref dcurr, dend))
                    SendAndReset();

                var passwords = user.Passwords;
                while (!RespWriteUtils.TryWriteAsciiBulkString("passwords", ref dcurr, dend))
                    SendAndReset();

                while (!RespWriteUtils.TryWriteArrayLength(passwords.Count, ref dcurr, dend))
                    SendAndReset();

                foreach (var password in passwords)
                {
                    while (!RespWriteUtils.TryWriteAsciiBulkString($"#{password.ToString()}", ref dcurr, dend))
                        SendAndReset();
                }

                while (!RespWriteUtils.TryWriteAsciiBulkString("commands", ref dcurr, dend))
                    SendAndReset();

                while (!RespWriteUtils.TryWriteAsciiBulkString(user.GetEnabledCommandsDescription(), ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }
    }
}