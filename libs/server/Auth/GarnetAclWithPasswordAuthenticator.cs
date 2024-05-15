// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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
        /// <param name="user"> User details to use for authentication.</param>
        /// <param name="password">Password to authenticate with.</param>
        /// <param name="username">Username to authenticate with. If empty, will authenticate default user.</param>
        /// <returns>true if authentication was successful</returns>
        protected override bool AuthenticateInternal(User user, ReadOnlySpan<byte> username, ReadOnlySpan<byte> password)
        {
                // Try to authenticate user
            ACLPassword passwordHash = ACLPassword.ACLPasswordFromString(Encoding.ASCII.GetString(password));
            if (user.IsEnabled && user.ValidatePassword(passwordHash))
            {
                _user = user;
                return true;
            }
            return false;
        }
    }
}
