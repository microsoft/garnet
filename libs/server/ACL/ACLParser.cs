// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Garnet.server.ACL
{
    class ACLParser
    {
        /// <summary>
        /// Parses a single-line ACL rule and returns a new user according to that rule.
        /// 
        /// ACL rules follow a subset of the Redis ACL rule syntax, with each rule
        /// being formatted as follows:
        /// 
        ///     ACL_RULE := user &lt;username> (&lt;ACL_OPERATION>)+
        ///     ACL_OPERATION := on | off | +@&lt;category> | -@&lt;category>
        /// 
        /// To manage user account:
        ///     on/off: enable/disable the user account
        /// 
        /// To configure user passwords:
        ///     &gt;&lt;password>: Add the password to the list of valid passwords for the user
        ///     &lt;&lt;password>: Remove the password from the list of valid password for the user
        ///     #&lt;hash>: Add the password hash to the list of valid passwords for the user
        ///     !&lt;hash>: Remove the password hash from the list of valid passwords for the user
        ///     nopass: Specify this user can login without a password.
        ///     resetpass: Reset all passwords defined for the user so far and disable passwordless login.
        /// </summary>
        /// <param name="input">A single line Redis-style ACL rule.</param>
        /// <param name="acl">An optional access control list to modify.</param>
        /// <returns>A user object representing the modified user.</returns>
        /// <exception cref="ACLParsingException">Thrown if the ACL rule cannot be parsed.</exception>
        /// <exception cref="ACLCategoryDoesNotExistException">Thrown if the ACL command category used by the operation does not exist.</exception>
        /// <exception cref="ACLUnknownOperationException">Thrown if the given operation does not exist.</exception>
        public static User ParseACLRule(string input, AccessControlList acl = null)
        {

            // Tokenize input string 
            Regex regex = new Regex("\\s+");
            string[] tokens = regex.Split(input.Trim());

            // Sanity check for correctness
            if (tokens.Length < 3)
            {
                throw new ACLParsingException("Malformed ACL rule");
            }

            // Expect keyword USER
            if (tokens[0].ToLower() != "user")
            {
                throw new ACLParsingException("ACL rules need to start with the USER keyword");
            }

            // Expect username
            string username = tokens[1];

            // Retrieve/add the user with the username to the access control list, if provided
            User user;
            if (acl != null)
            {
                user = acl.GetUser(username);

                if (user == null)
                {
                    user = new User(username);
                    acl.AddUser(user);
                }
            }
            else
            {
                user = new User(username);
            }

            // Parse remaining tokens as ACL operations
            for (int i = 2; i < tokens.Length; i++)
            {
                ApplyACLOpToUser(ref user, tokens[i]);
            }

            return user;
        }

        /// <summary>
        /// Parses the given ACL operation string and applies it to the given user.
        /// </summary>
        /// <param name="user">User to apply the operation to.</param>
        /// <param name="op">ACL operation as string.</param>
        /// <exception cref="ACLCategoryDoesNotExistException">Thrown if the ACL command category used by the operation does not exist.</exception>
        /// <exception cref="ACLUnknownOperationException">Thrown if the given operation does not exist.</exception>
        public static void ApplyACLOpToUser(ref User user, string op)
        {
            // Bail early for empty op
            if (op.Length == 0)
            {
                return;
            }

            if (op == "on")
            {
                // Enable user
                user.IsEnabled = true;
            }
            else if (op == "off")
            {
                // Disable user
                user.IsEnabled = false;
            }
            else if (op == "nopass")
            {
                // Make account passwordless
                user.ClearPasswords();
                user.IsPasswordless = true;
            }
            else if (op == "reset")
            {
                // Remove all passwords and access rights from the user
                user.Reset();
            }
            else if (op == "resetpass")
            {
                // Remove all passwords from the user
                user.ClearPasswords();
                user.IsPasswordless = false;
            }
            else if (op[0] == '>')
            {
                // Add password from cleartext
                user.AddPasswordHash(ACLPassword.ACLPasswordFromString(op.Substring(1)));
            }
            else if (op[0] == '<')
            {
                // Remove password from cleartext
                user.RemovePasswordHash(ACLPassword.ACLPasswordFromString(op.Substring(1)));
            }
            else if ((op[0] == '#') || (op[0] == '!'))
            {
                try
                {
                    if (op[0] == '#')
                    {
                        // Add password from hash
                        user.AddPasswordHash(ACLPassword.ACLPasswordFromHash(op.Substring(1)));
                    }
                    else
                    {
                        // Remove password from hash
                        user.RemovePasswordHash(ACLPassword.ACLPasswordFromHash(op.Substring(1)));
                    }
                }
                catch (ACLPasswordException exception)
                {
                    throw new ACLParsingException($"{exception.Message}");
                }
            }
            else if (op.StartsWith("-@") || op.StartsWith("+@"))
            {
                // Parse category name
                string categoryName = op.Substring(2);

                CommandCategory.Flag category;
                try
                {
                    category = CommandCategory.GetFlagByName(categoryName);
                }
                catch (KeyNotFoundException)
                {
                    throw new ACLCategoryDoesNotExistException(categoryName);
                }

                // Add or remove the category
                if (op[0] == '-')
                {
                    user.RemoveCategory(category);
                }
                else
                {
                    user.AddCategory(category);
                }
            }
            else if ((op == "~*") || (op == "allkeys"))
            {
                // NOTE: No-op, because only wildcard key patterns are currently supported
            }
            else if ((op == "resetkeys"))
            {
                // NOTE: No-op, because only wildcard key patterns are currently supported
            }
            else
            {
                throw new ACLUnknownOperationException(op);
            }
        }
    }
}