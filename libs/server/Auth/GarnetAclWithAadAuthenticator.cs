// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
        /// <param name="userHandle"> User details to use for authentication.</param>
        /// <param name="password">Password to authenticate with.</param>
        /// <param name="username">Username to authenticate with. If empty, will authenticate default user.</param>
        /// <returns>true if authentication was successful</returns>
        protected override bool AuthenticateInternal(UserHandle userHandle, ReadOnlySpan<byte> username, ReadOnlySpan<byte> password)
        {
            if (userHandle.User.IsEnabled && password.Length > 0 && _garnetAuthenticator.Authenticate(password, username))
            {
                _userHandle = userHandle;
                return true;
            }
            return false;
        }

        public override bool IsAuthenticated
        {
            get
            {
                return this._garnetAuthenticator.IsAuthenticated && base.IsAuthenticated;
            }
        }
    }
}