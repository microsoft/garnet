// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.server.Auth.Aad;

namespace Garnet.server.Auth.Settings
{
    /// <summary>
    /// Authentication mode
    /// </summary>
    public enum GarnetAuthenticationMode
    {
        /// <summary>
        /// No auth - Garnet accepts any and all connections
        /// </summary>
        NoAuth,

        /// <summary>
        /// Password - Garnet accepts connections with correct connection string
        /// </summary>
        Password,

        /// <summary>
        /// AAD - Garnet accepts connection with correct AAD principal
        /// In AAD mode, token may expire. Clients are expected to periodically refresh token with Garnet by running AUTH command.
        /// </summary>
        Aad,

        /// <summary>
        /// ACL - Garnet validates new connections and commands against configured ACL users and access rules.
        /// </summary>
        ACL,
        /// <summary>
        /// ACL mode using Aad token instead of password. Here username is expected to be ObjectId or a valid Group's Object Id and token will be validated for claims.
        /// </summary>
        AclWithAad
    }

    /// <summary>
    /// Authentication settings
    /// </summary>
    public interface IAuthenticationSettings : IDisposable
    {
        /// <summary>
        /// Create an authenticator using the current settings.
        /// </summary>
        /// <param name="storeWrapper">The main store the authenticator will be associated with.</param>
        IGarnetAuthenticator CreateAuthenticator(StoreWrapper storeWrapper);
    }
}