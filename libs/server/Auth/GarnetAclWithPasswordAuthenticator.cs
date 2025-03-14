// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using Garnet.server.ACL;
using Microsoft.Extensions.Logging;

namespace Garnet.server.Auth
{
    class GarnetAclWithPasswordAuthenticator : GarnetACLAuthenticator
    {

        public GarnetAclWithPasswordAuthenticator(AccessControlList accessControlList, ILogger logger) : base(accessControlList, logger)
        {
        }

        /// <summary>
        /// Authenticate the given user/password combination.
        /// </summary>
        /// <param name="userHandle"> User details to use for authentication.</param>
        /// <param name="password">Password to authenticate with.</param>
        /// <param name="username">Username to authenticate with. If empty, will authenticate default user.</param>
        /// <returns>true if authentication was successful</returns>
        protected override bool AuthenticateInternal(UserHandle userHandle, ReadOnlySpan<byte> username, ReadOnlySpan<byte> password)
        {
            // Try to authenticate user
            ACLPassword passwordHash = ACLPassword.ACLPasswordFromString(Encoding.ASCII.GetString(password));

            // Authentication and authorization checks must be performed against the effective user.
            var user = userHandle.User;
            if (user.IsEnabled && user.ValidatePassword(passwordHash))
            {
                _userHandle = userHandle;
                return true;
            }
            return false;
        }
    }
}