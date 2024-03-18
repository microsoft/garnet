// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Text;
using Garnet.common;
using Garnet.server.ACL;
using Garnet.server.Auth;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Server session for RESP protocol - ACL Commands
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// Processes subcommands for the "ACL" RESP-Command.
        /// NOTE: This function assumes the ACL token was already parsed.
        /// </summary>
        /// <param name="bufSpan">The remaining command bytes</param>
        /// <param name="count">The number of arguments remaining in bufSpan</param>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool ProcessACLCommands(ReadOnlySpan<byte> bufSpan, int count)
        {
            // Only proceed if current authenticator can be used with ACL commands.
            // Currently only GarnetACLAuthenticator is supported.
            if (!_authenticator.HasACLSupport || (_authenticator.GetType() != typeof(GarnetACLAuthenticator)))
            {
                if (!DrainCommands(bufSpan, count))
                    return false;
                while (!RespWriteUtils.WriteDirect(Encoding.ASCII.GetBytes($"-ERR ACL commands are not supported by the configured authenticator.\r\n"), ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            // Check that a subcommand is specified
            if (count < 1)
            {
                if (!DrainCommands(bufSpan, count))
                    return false;

                var errorMsg = string.Format(CmdStrings.ErrMissingParam, "ACL");
                var bresp_ERRMISSINGPARAM = Encoding.ASCII.GetBytes(errorMsg);
                bresp_ERRMISSINGPARAM.CopyTo(new Span<byte>(dcurr, bresp_ERRMISSINGPARAM.Length));
                dcurr += bresp_ERRMISSINGPARAM.Length;

                return true;
            }

            GarnetACLAuthenticator aclAuthenticator = (GarnetACLAuthenticator)_authenticator;

            // Mandatory: <subcommand>
            var subcommandSpan = GetCommand(bufSpan, out bool success1);
            if (!success1) return false;

            string subcommand = Encoding.ASCII.GetString(subcommandSpan).ToUpper();

            // Subcommand: LIST
            if ((subcommand == "LIST") && (count == 1))
            {
                if (!CheckACLAdminPermissions(bufSpan, count - 2, out bool success))
                {
                    return success;
                }

                var users = aclAuthenticator.GetAccessControlList().GetUsers();
                RespWriteUtils.WriteArrayLength(users.Count, ref dcurr, dend);

                foreach (var user in users)
                {
                    RespWriteUtils.WriteBulkString(Encoding.ASCII.GetBytes(user.Value.DescribeUser()), ref dcurr, dend);
                }
                SendAndReset();
            }
            // Subcommand: USERS
            else if ((subcommand == "USERS") && (count == 1))
            {
                if (!CheckACLAdminPermissions(bufSpan, count - 2, out bool success))
                {
                    return success;
                }

                var users = aclAuthenticator.GetAccessControlList().GetUsers();
                RespWriteUtils.WriteArrayLength(users.Count, ref dcurr, dend);

                foreach (var user in users)
                {
                    RespWriteUtils.WriteBulkString(Encoding.ASCII.GetBytes(user.Key), ref dcurr, dend);
                }
                SendAndReset();
            }
            // Subcommand: CAT
            else if ((subcommand == "CAT") && (count == 1))
            {
                var categories = CommandCategory.ListCategories();
                RespWriteUtils.WriteArrayLength(categories.Count, ref dcurr, dend);

                foreach (var category in categories)
                {
                    RespWriteUtils.WriteBulkString(Encoding.ASCII.GetBytes(category), ref dcurr, dend);
                }
                SendAndReset();
            }
            // Subcommand: SETUSER <username> [<ops>...]
            else if ((subcommand == "SETUSER") && (count >= 2))
            {
                if (!CheckACLAdminPermissions(bufSpan, count - 2, out bool success))
                {
                    return success;
                }

                // REQUIRED: username
                var usernameSpan = GetCommand(bufSpan, out success);
                if (!success) return false;

                // Modify or create the user with the given username
                // FIXME: This step should be atomic in the future. This will prevent partial execution of faulty ACL strings.
                var username = Encoding.ASCII.GetString(usernameSpan);
                ACL.User user = aclAuthenticator.GetAccessControlList().GetUser(username);

                int opsParsed = 0;
                try
                {
                    if (user == null)
                    {
                        user = new User(username);
                        aclAuthenticator.GetAccessControlList().AddUser(user);
                    }

                    // Remaining parameters are ACL operations
                    for (; opsParsed < count - 2; opsParsed++)
                    {
                        var op = GetCommand(bufSpan, out bool successOp);
                        Debug.Assert(successOp);

                        ACLParser.ApplyACLOpToUser(ref user, Encoding.ASCII.GetString(op));
                    }
                }
                catch (ACLException exception)
                {
                    logger?.LogDebug("ACLException: {message}", exception.Message);

                    // Abort command execution
                    if (!DrainCommands(bufSpan, count - opsParsed - 3))
                        return false;
                    while (!RespWriteUtils.WriteDirect(Encoding.ASCII.GetBytes($"-ERR {exception.Message}\r\n"), ref dcurr, dend))
                        SendAndReset();

                    return true;
                }

                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
            // Subcommand: DELUSER [<username> ...]
            else if (subcommand == "DELUSER" && (count >= 1))
            {
                if (!CheckACLAdminPermissions(bufSpan, count - 2, out bool success))
                {
                    return success;
                }

                int attemptedDeletes = 0;
                int successfulDeletes = 0;

                try
                {
                    // Attempt to delete the users with the given names
                    for (; attemptedDeletes < count - 1; attemptedDeletes++)
                    {
                        var username = GetCommand(bufSpan, out success);
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
                    if (!DrainCommands(bufSpan, count - attemptedDeletes - 2))
                        return false;
                    while (!RespWriteUtils.WriteDirect(Encoding.ASCII.GetBytes($"-ERR {exception.Message}\r\n"), ref dcurr, dend))
                        SendAndReset();

                    return true;
                }

                // Return the number of successful deletes
                while (!RespWriteUtils.WriteInteger(successfulDeletes, ref dcurr, dend))
                    SendAndReset();
            }
            // Subcommand: WHOAMI
            else if ((subcommand == "WHOAMI") && (count == 1))
            {
                // Return the name of the currently authenticated user.
                Debug.Assert(aclAuthenticator.GetUser() != null);
                while (!RespWriteUtils.WriteSimpleString(Encoding.ASCII.GetBytes(aclAuthenticator.GetUser().Name), ref dcurr, dend))
                    SendAndReset();
            }
            // Subcommand: LOAD
            else if ((subcommand == "LOAD") && (count == 1))
            {
                if (!CheckACLAdminPermissions(bufSpan, count - 2, out bool success))
                {
                    return success;
                }

                // NOTE: This is temporary as long as ACL operations are only supported when using the ACL authenticator
                Debug.Assert(this.storeWrapper.serverOptions.AuthSettings != null);
                Debug.Assert(this.storeWrapper.serverOptions.AuthSettings.GetType() == typeof(AclAuthenticationSettings));
                AclAuthenticationSettings aclAuthenticationSettings = (AclAuthenticationSettings)this.storeWrapper.serverOptions.AuthSettings;

                // Try to reload the configured ACL configuration file
                try
                {
                    logger?.LogInformation("Reading updated ACL configuration file '{filepath}'", aclAuthenticationSettings.AclConfigurationFile);
                    this.storeWrapper.accessControlList.Load(aclAuthenticationSettings.DefaultPassword, aclAuthenticationSettings.AclConfigurationFile);

                    while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
                catch (ACLException exception)
                {
                    while (!RespWriteUtils.WriteDirect(Encoding.ASCII.GetBytes($"-ERR {exception.Message}\r\n"), ref dcurr, dend))
                        SendAndReset();
                }
            }
            // Unknown or invalidly specified ACL subcommand
            else
            {
                if (!DrainCommands(bufSpan, count - 1))
                    return false;

                while (!RespWriteUtils.WriteDirect(Encoding.ASCII.GetBytes($"-ERR Unknown subcommand or wrong number of arguments for ACL command '{subcommand}'.\r\n"), ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }
    }
}