// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Text;
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
        /// <summary>
        /// Processes ACL LIST subcommand.
        /// </summary>
        /// <param name="count">The number of arguments remaining in buffer</param>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool NetworkAclList(int count)
        {
            // No additional args allowed
            if (count != 0)
            {
                while (!RespWriteUtils.WriteError($"ERR Unknown subcommand or wrong number of arguments for ACL LIST.", ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                GarnetACLAuthenticator aclAuthenticator = (GarnetACLAuthenticator)_authenticator;

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
        /// <param name="count">The number of arguments remaining in buffer</param>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool NetworkAclUsers(int count)
        {
            // No additional args allowed
            if (count != 0)
            {
                while (!RespWriteUtils.WriteError($"ERR Unknown subcommand or wrong number of arguments for ACL USERS.", ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                GarnetACLAuthenticator aclAuthenticator = (GarnetACLAuthenticator)_authenticator;

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
        /// <param name="count">The number of arguments remaining in buffer</param>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool NetworkAclCat(int count)
        {
            // No additional args allowed
            if (count != 0)
            {
                while (!RespWriteUtils.WriteError($"ERR Unknown subcommand or wrong number of arguments for ACL CAT.", ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
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
        /// <param name="count">The number of arguments remaining in buffer</param>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool NetworkAclSetUser(int count)
        {
            // Have to have at least the username
            if (count == 0)
            {
                while (!RespWriteUtils.WriteError($"ERR Unknown subcommand or wrong number of arguments for ACL SETUSER.", ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                GarnetACLAuthenticator aclAuthenticator = (GarnetACLAuthenticator)_authenticator;

                // REQUIRED: username
                var usernameSpan = GetCommand(out bool success);
                if (!success) return false;

                // Modify or create the user with the given username
                // FIXME: This step should be atomic in the future. This will prevent partial execution of faulty ACL strings.
                var username = Encoding.ASCII.GetString(usernameSpan);
                var user = aclAuthenticator.GetAccessControlList().GetUser(username);

                var opsParsed = 0;
                try
                {
                    if (user == null)
                    {
                        user = new User(username);
                        aclAuthenticator.GetAccessControlList().AddUser(user);
                    }

                    // Remaining parameters are ACL operations
                    for (; opsParsed < count - 1; opsParsed++)
                    {
                        var op = GetCommand(out bool successOp);
                        Debug.Assert(successOp);

                        ACLParser.ApplyACLOpToUser(ref user, Encoding.ASCII.GetString(op));
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
        /// <param name="count">The number of arguments remaining in buffer</param>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool NetworkAclDelUser(int count)
        {
            // Have to have at least the username
            if (count == 0)
            {
                while (!RespWriteUtils.WriteError($"ERR Unknown subcommand or wrong number of arguments for ACL DELUSER.", ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                GarnetACLAuthenticator aclAuthenticator = (GarnetACLAuthenticator)_authenticator;

                var attemptedDeletes = 0;
                var successfulDeletes = 0;

                try
                {
                    // Attempt to delete the users with the given names
                    for (; attemptedDeletes < count; attemptedDeletes++)
                    {
                        var username = GetCommand(out bool success);
                        if (!success) return false;

                        if (aclAuthenticator.GetAccessControlList().DeleteUser(Encoding.ASCII.GetString(username)))
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
        /// <param name="count">The number of arguments remaining in buffer</param>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool NetworkAclWhoAmI(int count)
        {
            // No additional args allowed
            if (count != 0)
            {
                while (!RespWriteUtils.WriteError($"ERR Unknown subcommand or wrong number of arguments for ACL WHOAMI.", ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                GarnetACLAuthenticator aclAuthenticator = (GarnetACLAuthenticator)_authenticator;

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
        /// <param name="count">The number of arguments remaining in buffer</param>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool NetworkAclLoad(int count)
        {
            // No additional args allowed
            if (count != 0)
            {
                while (!RespWriteUtils.WriteError($"ERR Unknown subcommand or wrong number of arguments for ACL LOAD.", ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
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
        /// <param name="count">The number of arguments remaining in buffer</param>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool NetworkAclSave(int count)
        {
            if (count != 0)
            {
                while (!RespWriteUtils.WriteError($"ERR Unknown subcommand or wrong number of arguments for ACL SAVE.", ref dcurr, dend))
                    SendAndReset();
            }

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