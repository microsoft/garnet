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
    class GarnetAclWithAadAuthenticator : GarnetACLAuthenticator
    {
        /// <summary>
        /// Authenticator to validate username and password.
        /// </summary>
        private readonly IGarnetAuthenticator _garnetAuthenticator;
        public GarnetAclWithAadAuthenticator(AccessControlList accessControlList, IGarnetAuthenticator garnetAuthenticator, ILogger logger) : base(accessControlList, logger)
        {
            _garnetAuthenticator = garnetAuthenticator;
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
            if (user.IsEnabled && password.Length > 0 && _garnetAuthenticator.Authenticate(password, username))
            {
                _user = user;
                return true;
            }
            return false;
        }
    }
}