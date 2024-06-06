// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.server.Auth
{
    /// <summary>
    /// Garnet authenticator interface
    /// </summary>
    public interface IGarnetAuthenticator
    {
        /// <summary>
        /// Is current caller authenticated
        /// </summary>
        bool IsAuthenticated { get; }

        /// <summary>
        /// Can authenticator authenticate
        /// 
        /// The returned value must be constant for the lifetime of the process
        /// </summary>
        bool CanAuthenticate { get; }

        /// <summary>
        /// Whether this authenticator can be used with the ACL
        /// </summary>
        bool HasACLSupport { get; }

        /// <summary>
        /// Authenticate the incoming username and password from AUTH command. Username is optional
        /// </summary>
        bool Authenticate(ReadOnlySpan<byte> password, ReadOnlySpan<byte> username);
    }
}