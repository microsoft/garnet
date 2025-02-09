// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;

namespace Garnet.server.ACL
{
    /// <summary>
    /// A reference used to access a <see cref="User"/>.
    /// </summary>
    public class UserHandle
    {
        /// <summary>
        /// The <see cref="User"/> referred to by the <see cref="Handle"/>.
        /// </summary>
        public User user;

        /// <summary>
        /// Constructor for a <see cref="UserHandle"/>.
        /// </summary>
        /// <param name="user">The <see cref="User"/> the handle will reference.</param>
        public UserHandle(User user) => this.user = user;

        /// <summary>
        /// Returns the current version of the <see cref="User"/> with the latest modifications.
        /// </summary>
        /// <returns>Returns the current version of the <see cref="User"/> with the latest modifications.</returns>
        public User GetUser() => this.user;

        /// <summary>
        /// Attempts to set the <see cref="User"/> for a handle.
        /// </summary>
        /// <param name="newUser">A <see cref="User"/> that should secede the existing <see cref="User"/> referred to by the handle.</param>
        /// <param name="replacedUser">The <see cref="User"/> expected to be replaced.</param>
        /// <returns>True if the assignment was performed; otherwise false.</returns>
        public bool TrySetUser(User newUser, User replacedUser)
            => Interlocked.CompareExchange(ref this.user, newUser, null) == null ||
                Interlocked.CompareExchange(ref this.user, newUser, replacedUser) == replacedUser;
    }
}
