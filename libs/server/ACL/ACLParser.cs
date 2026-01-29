// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;

namespace Garnet.server.ACL
{
    class ACLParser
    {
        private static readonly char[] WhitespaceChars = [' ', '\t', '\r', '\n'];

        private static readonly Dictionary<string, RespAclCategories> categoryNames = new Dictionary<string, RespAclCategories>(StringComparer.OrdinalIgnoreCase)
        {
            ["admin"] = RespAclCategories.Admin,
            ["bitmap"] = RespAclCategories.Bitmap,
            ["blocking"] = RespAclCategories.Blocking,
            ["connection"] = RespAclCategories.Connection,
            ["dangerous"] = RespAclCategories.Dangerous,
            ["geo"] = RespAclCategories.Geo,
            ["hash"] = RespAclCategories.Hash,
            ["hyperloglog"] = RespAclCategories.HyperLogLog,
            ["fast"] = RespAclCategories.Fast,
            ["keyspace"] = RespAclCategories.KeySpace,
            ["list"] = RespAclCategories.List,
            ["pubsub"] = RespAclCategories.PubSub,
            ["read"] = RespAclCategories.Read,
            ["scripting"] = RespAclCategories.Scripting,
            ["set"] = RespAclCategories.Set,
            ["sortedset"] = RespAclCategories.SortedSet,
            ["slow"] = RespAclCategories.Slow,
            ["stream"] = RespAclCategories.Stream,
            ["string"] = RespAclCategories.String,
            ["transaction"] = RespAclCategories.Transaction,
            ["vector"] = RespAclCategories.Vector,
            ["write"] = RespAclCategories.Write,
            ["garnet"] = RespAclCategories.Garnet,
            ["custom"] = RespAclCategories.Custom,
            ["all"] = RespAclCategories.All,
        };

        private static readonly Dictionary<RespAclCategories, string> categoryNamesReversed = categoryNames.ToDictionary(static kv => kv.Value, static kv => kv.Key);

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
            string[] tokens = input.Trim().Split(WhitespaceChars, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

            // Sanity check for correctness
            if (tokens.Length < 3)
            {
                throw new ACLParsingException("Malformed ACL rule");
            }

            // Expect keyword USER
            if (!tokens[0].Equals("user", StringComparison.OrdinalIgnoreCase))
            {
                throw new ACLParsingException("ACL rules need to start with the USER keyword");
            }

            // Expect username
            string username = tokens[1];

            // Retrieve/add the user with the username to the access control list, if provided
            User user;
            if (acl != null)
            {
                user = acl.GetUserHandle(username)?.User;

                if (user == null)
                {
                    user = new User(username);
                    acl.AddUserHandle(new UserHandle(user));
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

            if (op.Equals("ON", StringComparison.OrdinalIgnoreCase))
            {
                // Enable user
                user.IsEnabled = true;
            }
            else if (op.Equals("OFF", StringComparison.OrdinalIgnoreCase))
            {
                // Disable user
                user.IsEnabled = false;
            }
            else if (op.Equals("NOPASS", StringComparison.OrdinalIgnoreCase))
            {
                // Make account passwordless
                user.ClearPasswords();
                user.IsPasswordless = true;
            }
            else if (op.Equals("RESET", StringComparison.OrdinalIgnoreCase))
            {
                // Remove all passwords and access rights from the user
                user.Reset();
            }
            else if (op.Equals("RESETPASS", StringComparison.OrdinalIgnoreCase))
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
            else if (op.StartsWith("-@", StringComparison.Ordinal) || op.StartsWith("+@", StringComparison.Ordinal))
            {
                // Parse category name
                string categoryName = op.Substring(2);

                RespAclCategories category;
                try
                {
                    category = ACLParser.GetACLCategoryByName(categoryName);
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
            else if (op.StartsWith('-') || op.StartsWith('+'))
            {
                // Individual commands or command|subcommand pairs
                string commandName = op.Substring(1);

                if (!TryParseCommandForAcl(commandName, out RespCommand command))
                {
                    throw new AclCommandDoesNotExistException(commandName);
                }

                if (op[0] == '-')
                {
                    user.RemoveCommand(command);
                }
                else
                {
                    user.AddCommand(command);
                }
            }
            else if (op.Equals("~*", StringComparison.Ordinal) || op.Equals("ALLKEYS", StringComparison.OrdinalIgnoreCase))
            {
                // NOTE: No-op, because only wildcard key patterns are currently supported
            }
            else if (op.Equals("RESETKEYS", StringComparison.OrdinalIgnoreCase))
            {
                // NOTE: No-op, because only wildcard key patterns are currently supported
            }
            else
            {
                throw new ACLUnknownOperationException(op);
            }

            // There's some fixup that has to be done when parsing a command
            static bool TryParseCommandForAcl(string commandName, out RespCommand command)
            {
                int subCommandSepIx = commandName.IndexOf('|');
                bool isSubCommand = subCommandSepIx != -1;

                string effectiveName = isSubCommand ? commandName[..subCommandSepIx] + "_" + commandName[(subCommandSepIx + 1)..] : commandName;

                if (!Enum.TryParse(effectiveName, ignoreCase: true, out command) || !IsValidParse(command, effectiveName))
                {
                    // We handle these commands specially because blind replacements would cause
                    // us to be too accepting of different values
                    if (commandName.Equals("SLAVEOF", StringComparison.OrdinalIgnoreCase))
                    {
                        command = RespCommand.SECONDARYOF;
                    }
                    else if (commandName.Equals("CLUSTER|SET-CONFIG-EPOCH", StringComparison.OrdinalIgnoreCase))
                    {
                        command = RespCommand.CLUSTER_SETCONFIGEPOCH;
                    }
                    else
                    {
                        return false;
                    }
                }

                // Validate parse results matches the original input expectations
                if (isSubCommand)
                {
                    if (!RespCommandsInfo.TryGetRespCommandInfo(command, out RespCommandsInfo info))
                    {
                        throw new ACLException($"Couldn't load information for {command}, shouldn't be possible");
                    }

                    if (info.Command != command)
                    {
                        return false;
                    }
                }

                return !IsInvalidCommandToAcl(command);
            }

            // Returns true if the parsed value could possibly result in this command
            //
            // Used to handle the weirdness in Enum.TryParse - long term we probably
            // shift to something like IUtf8SpanParsable.
            static bool IsValidParse(RespCommand command, ReadOnlySpan<char> fromStr)
            {
                return command != RespCommand.NONE && command != RespCommand.INVALID && !fromStr.ContainsAnyInRange('0', '9');
            }

            // Some commands aren't really commands, so ACLs shouldn't accept their names
            static bool IsInvalidCommandToAcl(RespCommand command)
            => command == RespCommand.INVALID || command == RespCommand.NONE || command.NormalizeForACLs() != command;
        }

        /// <summary>
        /// Lookup the <see cref="RespAclCategories"/> by equivalent string.
        /// </summary>
        public static RespAclCategories GetACLCategoryByName(string categoryName)
        => ACLParser.categoryNames[categoryName];

        /// <summary>
        /// Lookup the string equivalent to <paramref name="category"/>.
        /// </summary>
        public static string GetNameByACLCategory(RespAclCategories category)
        => ACLParser.categoryNamesReversed[category];

        /// <summary>
        /// Returns a collection of all valid category names.
        /// </summary>
        /// <returns>Collection of valid category names.</returns>
        public static IReadOnlyCollection<string> ListCategories()
        => ACLParser.categoryNames.Keys;
    }
}