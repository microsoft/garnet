// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server.Auth.Settings
{
    /// <summary>
    /// No auth settings
    /// </summary>
    public class NoAuthSettings : IAuthenticationSettings
    {
        /// <summary>
        /// Creates a no auth authenticator
        /// </summary>
        /// <param name="storeWrapper">The main store the authenticator will be associated with.</param>
        public IGarnetAuthenticator CreateAuthenticator(StoreWrapper storeWrapper)
        {
            return new GarnetNoAuthAuthenticator();
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            // No-op
        }
    }
}