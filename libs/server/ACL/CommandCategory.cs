// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Numerics;
using System.Reflection;

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
            Admin = 1 << 0,

            /// <summary>
            /// Bitmaps related
            /// </summary>
            Bitmap = 1 << 1,

            /// <summary>
            /// Blocks connection until released by other command
            /// </summary>
            Blocking = 1 << 2,

            /// <summary>
            /// Affecting current or other connections
            /// </summary>
            Connection = 1 << 3,

            /// <summary>
            /// Potentially dangerous commands
            /// </summary>
            Dangerous = 1 << 4,

            /// <summary>
            /// Geospatial index related
            /// </summary>
            Geo = 1 << 5,

            /// <summary>
            /// Hashes related
            /// </summary>
            Hash = 1 << 6,

            /// <summary>
            /// HyperLogLog related
            /// </summary>
            HyperLogLog = 1 << 7,

            /// <summary>
            /// O(1) commands
            /// </summary>
            Fast = 1 << 8,

            /// <summary>
            /// Touching keys, dbs, or metadata in a generic way
            /// </summary>
            KeySpace = 1 << 9,

            /// <summary>
            /// List related
            /// </summary>
            List = 1 << 10,

            /// <summary>
            /// PubSub related
            /// </summary>
            PubSub = 1 << 11,

            /// <summary>
            /// Reading from keys
            /// </summary>
            Read = 1 << 12,

            /// <summary>
            /// Scripting realted
            /// </summary>
            Scripting = 1 << 13,

            /// <summary>
            /// Set related
            /// </summary>
            Set = 1 << 14,

            /// <summary>
            /// SortedSet related
            /// </summary>
            SortedSet = 1 << 15,

            /// <summary>
            /// Not <see cref="Fast"/>
            /// </summary>
            Slow = 1 << 16,

            /// <summary>
            /// Stream related
            /// </summary>
            Stream = 1 << 17,

            /// <summary>
            /// String related
            /// </summary>
            String = 1 << 18,

            /// <summary>
            /// Transaction related
            /// </summary>
            Transaction = 1 << 19,

            /// <summary>
            /// Writing to keys.
            /// </summary>
            Write = 1 << 20,

            /// <summary>
            /// All command categories
            /// </summary>
            All = (Write << 1) - 1
        };

        /// <summary>
        /// Map of category names to bit flag
        /// </summary>
        static readonly Dictionary<string, Flag> _categoryNames = new()
        {
            ["admin"] = Flag.Admin,
            ["bitmap"] = Flag.Bitmap,
            ["blocking"] = Flag.Blocking,
            ["connection"] = Flag.Connection,
            ["dangerous"] = Flag.Dangerous,
            ["geo"] = Flag.Geo,
            ["hash"] = Flag.Hash,
            ["hyperloglog"] = Flag.HyperLogLog,
            ["fast"] = Flag.Fast,
            ["keyspace"] = Flag.KeySpace,
            ["list"] = Flag.List,
            ["pubsub"] = Flag.PubSub,
            ["read"] = Flag.Read,
            ["scripting"] = Flag.Scripting,
            ["set"] = Flag.Set,
            ["sortedset"] = Flag.SortedSet,
            ["slow"] = Flag.Slow,
            ["stream"] = Flag.Stream,
            ["string"] = Flag.String,
            ["transaction"] = Flag.Transaction,
            ["write"] = Flag.Write,
            ["all"] = Flag.All,
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
        public static IReadOnlyCollection<string> ListCategories()
        {
            return _categoryNames.Keys;
        }

        /// <summary>
        /// Maps a <see cref="Flag"/> to the equivalent <see cref="RespAclCategories"/>.
        /// 
        /// Note that special values (like <see cref="Flag.All"/>) have no equivalent.
        /// </summary>
        public static RespAclCategories ToRespAclCategory(Flag flag)
        {
            switch (flag)
            {
                case Flag.Admin: return RespAclCategories.Admin;
                // all is special, does not map
                case Flag.Bitmap: return RespAclCategories.Bitmap;
                case Flag.Blocking: return RespAclCategories.Blocking;
                case Flag.Connection: return RespAclCategories.Connection;
                case Flag.Dangerous: return RespAclCategories.Dangerous;
                case Flag.Fast: return RespAclCategories.Fast;
                case Flag.Geo: return RespAclCategories.Geo;
                case Flag.Hash: return RespAclCategories.Hash;
                case Flag.HyperLogLog: return RespAclCategories.HyperLogLog;
                case Flag.KeySpace: return RespAclCategories.KeySpace;
                case Flag.List: return RespAclCategories.List;
                case Flag.PubSub: return RespAclCategories.PubSub;
                case Flag.Read: return RespAclCategories.Read;
                case Flag.Scripting: return RespAclCategories.Scripting;
                case Flag.Set: return RespAclCategories.Set;
                case Flag.Slow: return RespAclCategories.Slow;
                case Flag.SortedSet: return RespAclCategories.SortedSet;
                case Flag.Stream: return RespAclCategories.Stream;
                case Flag.String: return RespAclCategories.String;
                case Flag.Transaction: return RespAclCategories.Transaction;
                case Flag.Write: return RespAclCategories.Write;
                default:
                    Debug.Fail($"Shouldn't be possible, unknown Flag: {flag}");
                    return RespAclCategories.None;
            }
        }

        public static Flag FromRespAclCategories(RespAclCategories cat)
        {
            switch (cat)
            {
                case RespAclCategories.Admin: return Flag.Admin;
                case RespAclCategories.Bitmap: return Flag.Bitmap;
                case RespAclCategories.Blocking: return Flag.Blocking;
                case RespAclCategories.Connection: return Flag.Connection;
                case RespAclCategories.Dangerous: return Flag.Dangerous;
                case RespAclCategories.Fast: return Flag.Fast;
                case RespAclCategories.Geo: return Flag.Geo;
                case RespAclCategories.Hash: return Flag.Hash;
                case RespAclCategories.HyperLogLog: return Flag.HyperLogLog;
                case RespAclCategories.KeySpace: return Flag.KeySpace;
                case RespAclCategories.List: return Flag.List;
                case RespAclCategories.PubSub: return Flag.PubSub;
                case RespAclCategories.Read: return Flag.Read;
                case RespAclCategories.Scripting: return Flag.Scripting;
                case RespAclCategories.Set: return Flag.Set;
                case RespAclCategories.Slow: return Flag.Slow;
                case RespAclCategories.SortedSet: return Flag.SortedSet;
                case RespAclCategories.Stream: return Flag.Stream;
                case RespAclCategories.String: return Flag.String;
                case RespAclCategories.Transaction: return Flag.Transaction;
                case RespAclCategories.Write: return Flag.Write;
                default:
                    Debug.Fail($"Shouldn't be possible, unknown RespAclCategory: {cat}");
                    return Flag.None;
            }
        }
    }
}