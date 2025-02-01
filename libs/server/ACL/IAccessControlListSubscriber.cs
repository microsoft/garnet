// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server.ACL
{
    /// <summary>
    /// An interface for types that subscribe to <see cref="AccessControlList"/> changes.
    /// </summary>
    internal interface IAccessControlListSubscriber
    {
        /// <summary>
        /// A key for the <see cref="IAccessControlListSubscriber"/>.
        /// </summary>
        public string AclSubscriberKey { get; }

        /// <summary>
        /// Handle notification received when changes are performed to the <see cref="AccessControlList"/>.
        /// </summary>
        /// <param name="user">The modified <see cref="User"/>.</param>
        public void NotifyAclChange(User user);
    }
}
