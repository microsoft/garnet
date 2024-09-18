// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.server.ACL;

namespace Garnet.server.Auth
{
    public interface IGarnetAclAuthenticator :  IGarnetAuthenticator 
    {
        /// <summary>
        /// Indicates that this user can authenticate with passed credentials.
        /// </summary>
        bool CanAuthenticate { get; }

        /// <summary>
        /// Check if the user is authorized to execute commands.
        /// </summary>
        bool IsAuthenticated { get; }

        /// <summary>
        /// ACL authenticator is can use ACL.
        /// </summary>
        bool HasACLSupport { get; }

        /// <summary>
        /// Authenticate the given user/password combination.
        /// </summary>
        /// <param name="password">Password to authenticate with.</param>
        /// <param name="username">Username to authenticate with. If empty, will authenticate default user.</param>
        /// <returns>true if authentication was successful</returns>
        bool Authenticate(ReadOnlySpan<byte> password, ReadOnlySpan<byte> username);

        /// <summary>
        /// Returns the currently authorized user.
        /// </summary>
        /// <returns>Authorized user or null if not authorized</returns>
        User GetUser();

        /// <summary>
        /// The Access Control List to authenticate users against
        /// </summary>
        AccessControlList GetAccessControlList();
    }
}
