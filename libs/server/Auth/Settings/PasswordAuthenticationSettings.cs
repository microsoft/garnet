// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.server.Auth.Settings
{
    /// <summary>
    /// Password auth settings
    /// </summary>
    public class PasswordAuthenticationSettings : IAuthenticationSettings
    {
        private readonly byte[] _pwd;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="pwd">The password</param>
        public PasswordAuthenticationSettings(string pwd)
        {
            if (string.IsNullOrEmpty(pwd))
            {
                throw new Exception("Password cannot be null.");
            }
            _pwd = System.Text.Encoding.ASCII.GetBytes(pwd);
        }

        /// <summary>
        /// Creates a password auth authenticator
        /// </summary>
        /// <param name="storeWrapper">The main store the authenticator will be associated with.</param>
        public IGarnetAuthenticator CreateAuthenticator(StoreWrapper storeWrapper)
        {
            return new GarnetPasswordAuthenticator(_pwd);
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            // No op
        }
    }
}