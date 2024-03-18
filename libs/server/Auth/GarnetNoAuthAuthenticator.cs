// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;

namespace Garnet.server.Auth
{
    class GarnetNoAuthAuthenticator : IGarnetAuthenticator
    {
        public bool IsAuthenticated => true;

        public bool CanAuthenticate => false;

        public bool HasACLSupport => false;

        public bool Authenticate(ReadOnlySpan<byte> password, ReadOnlySpan<byte> username)
        {
            Debug.Fail("No auth authenticator should never authenticate.");
            return false;
        }
    }
}