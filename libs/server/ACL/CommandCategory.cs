// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;

namespace Garnet.server.ACL
{
    /// <summary>
    /// Maintains grouping of Garnet RESP commands to ACL command categories.
    /// </summary>
    public class CommandCategory
    {
        /// <summary>
        /// Defines the respective bitmask position for each category.
        /// </summary>
        [Flags]
        public enum Flag : uint
        {
            /// <summary>
            /// No flag defined
            /// </summary>
            None = 0,

            /// <summary>
            /// Administrative commands
            /// </summary>
            Admin = 1 << 0
        };

        /// <summary>
        /// Map of category names to bit flag
        /// </summary>
        static readonly Dictionary<string, Flag> _categoryNames = new()
        {
            { "admin",  Flag.Admin}
        };

        /// <summary>
        /// Reverse category name mapping to speed up name lookups
        /// </summary>
        static readonly Dictionary<Flag, string> _categoryNamesReversed = _categoryNames.ToDictionary(x => x.Value, x => x.Key);

        /// <summary>
        /// Returns the bit flag for the given category name.
        /// </summary>
        /// <param name="name">Command category name.</param>
        /// <returns>Corresponding bit flag.</returns>
        public static Flag GetFlagByName(string name)
        {
            return _categoryNames[name];
        }

        /// <summary>
        /// Given a single bit flag returns the name of the respective category.
        /// </summary>
        /// <param name="mask">Category bit flag.</param>
        /// <returns>Corresponding category name.</returns>
        public static string GetNameByFlag(Flag mask)
        {
            return _categoryNamesReversed[mask];
        }

        /// <summary>
        /// Returns a collection of all valid category names.
        /// </summary>
        /// <returns>Collection of valid category names.</returns>
        public static IReadOnlyCollection<String> ListCategories()
        {
            return _categoryNames.Keys;
        }

    }
}