// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.server.Auth
{
    /// <summary>
    /// Authenticator that uses a single fixed password.
    /// XXX: Deprecated. Should be replaced by ACL authenticator.
    /// </summary>
    class GarnetPasswordAuthenticator : IGarnetAuthenticator
    {
        public bool IsAuthenticated => _authenticated;

        public bool CanAuthenticate => true;

        public bool HasACLSupport => false;

        private readonly byte[] _pwd;
        private bool _authenticated;

        public GarnetPasswordAuthenticator(byte[] pwd)
        {
            _pwd = pwd;
        }

        public bool Authenticate(ReadOnlySpan<byte> password, ReadOnlySpan<byte> username)
        {
            _authenticated = password.SequenceEqual(_pwd);
            return _authenticated;
        }
    }
}