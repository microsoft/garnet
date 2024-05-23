// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server.Auth.Settings
{
    /// <summary>
    /// ACL authentication with AAD settings.
    /// </summary>
    public class AclAuthenticationPasswordSettings : AclAuthenticationSettings
    {

        /// <summary>
        /// Creates and initializes new ACL authentication settings
        /// </summary>
        /// <param name="aclConfigurationFile">Location of the ACL configuration file</param>
        /// <param name="defaultPassword">Optional default password, if not defined through aclConfigurationFile</param>
        public AclAuthenticationPasswordSettings(string aclConfigurationFile, string defaultPassword = "") : base(aclConfigurationFile, defaultPassword)
        {
        }

        /// <summary>
        /// Creates an ACL authenticator
        /// </summary>
        /// <param name="storeWrapper">The main store the authenticator will be associated with.</param>

        protected override IGarnetAuthenticator CreateAuthenticatorInternal(StoreWrapper storeWrapper)
        {
            return new GarnetAclWithPasswordAuthenticator(storeWrapper.accessControlList, storeWrapper.logger);
        }
    }
}