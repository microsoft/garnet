// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using Garnet.server.ACL;
using Microsoft.Extensions.Logging;

namespace Garnet.server.Auth
{
    public abstract class GarnetACLAuthenticator : IGarnetAuthenticator
    {
        /// <summary>
        /// The Access Control List to authenticate users against
        /// </summary>
        protected readonly AccessControlList _acl;

        /// <summary>
        /// Logger to use to output log messages to
        /// </summary>
        protected readonly ILogger _logger;

        /// <summary>
        /// If authenticated, contains a reference to the authenticated user. Otherwise null.
        /// </summary>
        protected UserHandle _userHandle = null;

        /// <summary>
        /// Initializes a new ACLAuthenticator instance.
        /// </summary>
        /// <param name="accessControlList">Access control list to authenticate against</param>
        /// <param name="logger">The logger to use</param>
        public GarnetACLAuthenticator(AccessControlList accessControlList, ILogger logger)
        {
            _acl = accessControlList;
            _logger = logger;
        }

        /// <summary>
        /// Indicates that this user can authenticate with passed credentials.
        /// </summary>
        public bool CanAuthenticate => true;

        /// <summary>
        /// Check if the user is authorized to execute commands.
        /// </summary>
        public virtual bool IsAuthenticated => _userHandle != null;

        /// <summary>
        /// ACL authenticator is can use ACL.
        /// </summary>
        public bool HasACLSupport => true;

        /// <summary>
        /// Authenticate the given user/password combination.
        /// </summary>
        /// <param name="password">Password to authenticate with.</param>
        /// <param name="username">Username to authenticate with. If empty, will authenticate default user.</param>
        /// <returns>true if authentication was successful</returns>
        public bool Authenticate(ReadOnlySpan<byte> password, ReadOnlySpan<byte> username)
        {
            bool successful = false;
            try
            {
                // Check if user exists and set default user if username is unspecified
                string uname = Encoding.ASCII.GetString(username);
                UserHandle userHandle = string.IsNullOrEmpty(uname) ? _acl.GetDefaultUserHandle() : _acl.GetUserHandle(uname);
                if (userHandle == null)
                {
                    return false;
                }
                successful = AuthenticateInternal(userHandle, username, password);
            }
            catch (Exception ex)
            {
                // If we failed authentication must have failed and we will just exit, leaving the current user reference unchanged.
                _logger?.LogDebug("Authentication failed unexpectedly for user {username}: {msg}", Encoding.ASCII.GetString(username), ex.Message);
            }

            return successful;
        }

        protected abstract bool AuthenticateInternal(UserHandle userHandle, ReadOnlySpan<byte> username, ReadOnlySpan<byte> password);

        /// <summary>
        /// Returns the <see cref="UserHandle"/> of the currently authorized user.
        /// </summary>
        /// <returns>Authorized user's <see cref="UserHandle"/> or null if not authorized</returns>
        public UserHandle GetUserHandle()
        {
            return _userHandle;
        }

        /// <summary>
        /// Return a reference to the access control list used by the authenticator.
        /// 
        /// XXX: There should only be one AccessControlList for the whole server. Make ACL singleton.
        /// </summary>
        /// <returns>The access control list used by this authenticator</returns>
        public AccessControlList GetAccessControlList()
        {
            return _acl;
        }
    }
}