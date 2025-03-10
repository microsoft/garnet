// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace Garnet.server.ACL
{
    /// <summary>
    /// A reference used to access a <see cref="User"/>.
    /// </summary>
    public class UserHandle
    {
        /// <summary>
        /// The <see cref="User"/> referred to by the <see cref="UserHandle"/>.
        /// </summary>
        private User user;

        /// <summary>
        /// Constructor for a <see cref="UserHandle"/>.
        /// </summary>
        /// <param name="user">The <see cref="User"/> the handle will reference.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="user"/> is <c>null</c>.</exception>
        public UserHandle(User user)
        {
            if (user == null)
            {
                throw new ArgumentNullException(nameof(user));
            }

            this.user = user;
        }


        /// <summary>
        /// Returns the current version of the <see cref="User"/> with the latest modifications.
        /// </summary>
        /// <returns>Returns the current version of the <see cref="User"/> with the latest modifications.</returns>
        public User User => user;

        /// <summary>
        /// Attempts to set the <see cref="User"/> for a handle.
        /// </summary>
        /// <param name="newUser">A <see cref="User"/> that should secede the existing <see cref="User"/> referred to by the handle.</param>
        /// <param name="replacedUser">The <see cref="User"/> expected to be replaced.</param>
        /// <returns>True if the assignment was performed; otherwise false.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="newUser"/> is <c>null</c>.</exception>
        public bool TrySetUser(User newUser, User replacedUser)
        {
            if (newUser == null)
            {
                throw new ArgumentNullException(nameof(newUser));
            }

            return Interlocked.CompareExchange(ref this.user, newUser, replacedUser) == replacedUser;
        }
    }
}