// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// Logic for parsing command info from RESP format
    /// </summary>
    public class RespCommandInfoParser
    {
        /// <summary>
        /// Tries to parse a RespCommandInfo object from RESP format
        /// </summary>
        /// <param name="ptr">Pointer to current RESP chunk to read</param>
        /// <param name="end">Pointer to end of RESP chunk to read</param>
        /// <param name="supportedCommands">Mapping between command name and Garnet RespCommand and ArrayCommand values</param>
        /// <param name="commandInfo">Parsed RespCommandsInfo object</param>
        /// <param name="parentCommand">Name of parent command, null if none</param>
        /// <returns>True if parsing successful</returns>
        public static unsafe bool TryReadFromResp(ref byte* ptr, byte* end, IReadOnlyDictionary<string, (RespCommand, byte?)> supportedCommands, out RespCommandsInfo commandInfo, string parentCommand = null)
        {
            commandInfo = default;

            // Command info is null
            if (new ReadOnlySpan<byte>(ptr, 5).SequenceEqual("$-1\r\n"u8)) return true;

            // Verify command info array length
            RespReadUtils.ReadArrayLength(out var infoElemCount, ref ptr, end);
            if (infoElemCount != 10) return false;

            // 1) Name
            RespReadUtils.ReadStringWithLengthHeader(out var name, ref ptr, end);

            // 2) Arity
            RespReadUtils.ReadIntegerAsString(out var strArity, ref ptr, end);
            if (!int.TryParse(strArity, out var arity)) return false;

            // 3) Flags
            var flags = RespCommandFlags.None;
            RespReadUtils.ReadArrayLength(out var flagCount, ref ptr, end);
            for (var flagIdx = 0; flagIdx < flagCount; flagIdx++)
            {
                RespReadUtils.ReadSimpleString(out var strFlag, ref ptr, end);
                if (!EnumUtils.TryParseEnumFromDescription<RespCommandFlags>(strFlag, out var flag)) return false;
                flags |= flag;
            }

            // 4) First key
            RespReadUtils.ReadIntegerAsString(out var strFirstKey, ref ptr, end);
            if (!int.TryParse(strFirstKey, out var firstKey)) return false;

            // 5) Last key
            RespReadUtils.ReadIntegerAsString(out var strLastKey, ref ptr, end);
            if (!int.TryParse(strLastKey, out var lastKey)) return false;

            // 6) Step
            RespReadUtils.ReadIntegerAsString(out var strStep, ref ptr, end);
            if (!int.TryParse(strStep, out var step)) return false;

            // 7) ACL categories
            var aclCategories = RespAclCategories.None;
            RespReadUtils.ReadArrayLength(out var aclCatCount, ref ptr, end);
            for (var aclCatIdx = 0; aclCatIdx < aclCatCount; aclCatIdx++)
            {
                RespReadUtils.ReadSimpleString(out var strAclCat, ref ptr, end);
                if (!EnumUtils.TryParseEnumFromDescription<RespAclCategories>(strAclCat.TrimStart('@'), out var aclCat))
                    return false;
                aclCategories |= aclCat;
            }

            // 8) Tips
            RespReadUtils.ReadStringArrayWithLengthHeader(out var tips, ref ptr, end);

            // 9) Key specifications
            RespReadUtils.ReadArrayLength(out var ksCount, ref ptr, end);
            var keySpecifications = new RespCommandKeySpecification[ksCount];
            for (var ksIdx = 0; ksIdx < ksCount; ksIdx++)
            {
                if (!RespKeySpecificationParser.TryReadFromResp(ref ptr, end, out var keySpec)) return false;
                keySpecifications[ksIdx] = keySpec;
            }

            // 10) SubCommands
            RespReadUtils.ReadArrayLength(out var scCount, ref ptr, end);
            var subCommands = new List<RespCommandsInfo>();
            for (var scIdx = 0; scIdx < scCount; scIdx++)
            {
                if (!TryReadFromResp(ref ptr, end, supportedCommands, out commandInfo, name))
                    return false;

                subCommands.Add(commandInfo);
            }

            commandInfo = new RespCommandsInfo()
            {
                Command = supportedCommands[parentCommand ?? name].Item1,
                ArrayCommand = supportedCommands[parentCommand ?? name].Item2,
                Name = name.ToUpper(),
                Arity = arity,
                Flags = flags,
                FirstKey = firstKey,
                LastKey = lastKey,
                Step = step,
                AclCategories = aclCategories,
                Tips = tips.Length == 0 ? null : tips,
                KeySpecifications = keySpecifications.Length == 0 ? null : keySpecifications,
                SubCommands = subCommands.Count == 0 ? null : subCommands.OrderBy(sc => sc.Name).ToArray()
            };

            return true;
        }
    }

    /// <summary>
    /// Logic for parsing key specification from RESP format
    /// </summary>
    internal class RespKeySpecificationParser
    {
        /// <summary>
        /// Tries to parse RespCommandKeySpecification from RESP format
        /// </summary>
        /// <param name="ptr">Pointer to current RESP chunk to read</param>
        /// <param name="end">Pointer to end of RESP chunk to read</param>
        /// <param name="keySpec">Parsed RespCommandKeySpecification object</param>
        /// <returns>True if parsing successful</returns>
        internal static unsafe bool TryReadFromResp(ref byte* ptr, byte* end, out RespCommandKeySpecification keySpec)
        {
            keySpec = default;

            string notes = null;
            var flags = KeySpecificationFlags.None;
            KeySpecMethodBase beginSearch = null;
            KeySpecMethodBase findKeys = null;

            RespReadUtils.ReadArrayLength(out var elemCount, ref ptr, end);

            for (var elemIdx = 0; elemIdx < elemCount; elemIdx += 2)
            {
                RespReadUtils.ReadStringWithLengthHeader(out var ksKey, ref ptr, end);

                if (string.Equals(ksKey, "notes", StringComparison.Ordinal))
                {
                    RespReadUtils.ReadStringWithLengthHeader(out notes, ref ptr, end);
                }
                else if (string.Equals(ksKey, "flags", StringComparison.Ordinal))
                {
                    RespReadUtils.ReadArrayLength(out var flagsCount, ref ptr, end);
                    for (var flagIdx = 0; flagIdx < flagsCount; flagIdx++)
                    {
                        RespReadUtils.ReadSimpleString(out var strFlag, ref ptr, end);
                        if (!EnumUtils.TryParseEnumFromDescription<KeySpecificationFlags>(strFlag,
                                out var flag)) return false;
                        flags |= flag;
                    }
                }
                else if (string.Equals(ksKey, "begin_search", StringComparison.Ordinal))
                {
                    if (!RespKeySpecificationTypesParser.TryReadFromResp(ksKey, ref ptr, end, out beginSearch)) return false;
                }
                else if (string.Equals(ksKey, "find_keys", StringComparison.Ordinal))
                {
                    if (!RespKeySpecificationTypesParser.TryReadFromResp(ksKey, ref ptr, end, out findKeys)) return false;
                }
                else
                {
                    return false;
                }
            }

            keySpec = new RespCommandKeySpecification()
            {
                Notes = notes,
                Flags = flags,
                BeginSearch = beginSearch,
                FindKeys = findKeys
            };

            return true;
        }
    }

    /// <summary>
    /// Logic for parsing BeginSearch / FindKeys key specification from RESP format
    /// </summary>
    internal class RespKeySpecificationTypesParser
    {
        /// <summary>
        /// Tries to parse KeySpecMethodBase from RESP format
        /// </summary>
        /// <param name="keySpecKey">Type of key specification ("begin_search" / "find_keys")</param>
        /// <param name="ptr">Pointer to current RESP chunk to read</param>
        /// <param name="end">Pointer to end of RESP chunk to read</param>
        /// <param name="keySpecMethod">Parsed KeySpecMethodBase object</param>
        /// <returns>True if parsing successful</returns>
        public static unsafe bool TryReadFromResp(string keySpecKey, ref byte* ptr, byte* end, out KeySpecMethodBase keySpecMethod)
        {
            keySpecMethod = default;

            if (!TryReadKeySpecHeader(ref ptr, end, out var keySpecType)) return false;

            IKeySpecParser parser;
            if (string.Equals(keySpecKey, "begin_search", StringComparison.Ordinal))
            {
                if (string.Equals(keySpecType, "index", StringComparison.Ordinal))
                    parser = BeginSearchIndexParser.Instance;
                else if (string.Equals(keySpecType, "keyword", StringComparison.Ordinal))
                    parser = BeginSearchKeywordParser.Instance;
                else if (string.Equals(keySpecType, "unknown", StringComparison.Ordinal))
                    parser = BeginSearchUnknownParser.Instance;
                else return false;
            }
            else if (string.Equals(keySpecKey, "find_keys", StringComparison.Ordinal))
            {
                if (string.Equals(keySpecType, "range", StringComparison.Ordinal))
                    parser = FindKeysRangeParser.Instance;
                else if (string.Equals(keySpecType, "keynum", StringComparison.Ordinal))
                    parser = FindKeysKeyNumParser.Instance;
                else if (string.Equals(keySpecType, "unknown", StringComparison.Ordinal))
                    parser = FindKeysUnknownParser.Instance;
                else return false;
            }
            else return false;

            if (!parser.TryReadFromResp(ref ptr, end, out keySpecMethod)) return false;

            return true;
        }

        /// <summary>
        /// Tries to parse key spec header from RESP format
        /// </summary>
        /// <param name="ptr">Pointer to current RESP chunk to read</param>
        /// <param name="end">Pointer to end of RESP chunk to read</param>
        /// <param name="keySpecType">Parsed key spec type</param>
        /// <returns>True if parsing successful</returns>
        private static unsafe bool TryReadKeySpecHeader(ref byte* ptr, byte* end, out string keySpecType)
        {
            keySpecType = default;

            RespReadUtils.ReadArrayLength(out var ksTypeElemCount, ref ptr, end);
            if (ksTypeElemCount != 4) return false;
            RespReadUtils.ReadStringWithLengthHeader(out var ksTypeStr, ref ptr, end);
            if (!string.Equals(ksTypeStr, "type", StringComparison.Ordinal)) return false;
            RespReadUtils.ReadStringWithLengthHeader(out var ksType, ref ptr, end);
            RespReadUtils.ReadStringWithLengthHeader(out var ksSpecStr, ref ptr, end);
            if (!string.Equals(ksSpecStr, "spec", StringComparison.Ordinal)) return false;

            keySpecType = ksType;
            return true;
        }

        /// <summary>
        /// Interface for classes implementing parsing of KeySpecMethodBase objects
        /// </summary>
        internal interface IKeySpecParser
        {
            /// <summary>
            /// Tries to parse KeySpecMethodBase from RESP format
            /// </summary>
            /// <param name="ptr">Pointer to current RESP chunk to read</param>
            /// <param name="end">Pointer to end of RESP chunk to read</param>
            /// <param name="keySpecMethod">Parsed KeySpecMethodBase object</param>
            /// <returns>True if parsing successful</returns>
            unsafe bool TryReadFromResp(ref byte* ptr, byte* end, out KeySpecMethodBase keySpecMethod);
        }

        /// <summary>
        /// Parser for the BeginSearchIndex key specification method
        /// </summary>
        internal sealed class BeginSearchIndexParser : IKeySpecParser
        {
            private static BeginSearchIndexParser instance;

            /// <summary>
            /// Disallow default constructor (singleton)
            // </summary>
            private BeginSearchIndexParser() { }

            /// <summary>
            /// Returns the singleton instance of <see cref="BeginSearchIndexParser">.
            /// </summary>
            public static BeginSearchIndexParser Instance
            {
                get { return instance ??= new BeginSearchIndexParser(); }
            }

            /// <inheritdoc />
            public unsafe bool TryReadFromResp(ref byte* ptr, byte* end, out KeySpecMethodBase keySpecMethod)
            {
                keySpecMethod = default;

                RespReadUtils.ReadArrayLength(out var ksSpecElemCount, ref ptr, end);
                if (ksSpecElemCount != 2) return false;
                RespReadUtils.ReadStringWithLengthHeader(out var ksArgKey, ref ptr, end);
                if (!string.Equals(ksArgKey, "index", StringComparison.Ordinal)) return false;
                RespReadUtils.ReadIntegerAsString(out var strIndex, ref ptr, end);
                if (!int.TryParse(strIndex, out var index)) return false;

                keySpecMethod = new BeginSearchIndex(index);

                return true;
            }
        }

        /// <summary>
        /// Parser for the BeginSearchKeyword key specification method
        /// </summary>

        internal class BeginSearchKeywordParser : IKeySpecParser
        {
            private static BeginSearchKeywordParser instance;

            private BeginSearchKeywordParser()
            {
            }

            public static BeginSearchKeywordParser Instance
            {
                get { return instance ??= new BeginSearchKeywordParser(); }
            }

            /// <inheritdoc />
            public unsafe bool TryReadFromResp(ref byte* ptr, byte* end, out KeySpecMethodBase keySpecMethod)
            {
                keySpecMethod = default;

                RespReadUtils.ReadArrayLength(out var specElemCount, ref ptr, end);
                if (specElemCount != 4) return false;
                RespReadUtils.ReadStringWithLengthHeader(out var argKey, ref ptr, end);
                if (!string.Equals(argKey, "keyword", StringComparison.Ordinal)) return false;
                RespReadUtils.ReadStringWithLengthHeader(out var keyword, ref ptr, end);
                RespReadUtils.ReadStringWithLengthHeader(out argKey, ref ptr, end);
                if (!string.Equals(argKey, "startfrom", StringComparison.Ordinal)) return false;
                RespReadUtils.ReadIntegerAsString(out var strStartFrom, ref ptr, end);
                if (!int.TryParse(strStartFrom, out var startFrom)) return false;

                keySpecMethod = new BeginSearchKeyword(keyword, startFrom);

                return true;
            }
        }

        /// <summary>
        /// Parser for the BeginSearchUnknown key specification method
        /// </summary>
        internal class BeginSearchUnknownParser : IKeySpecParser
        {
            private static BeginSearchUnknownParser instance;

            private BeginSearchUnknownParser()
            {
            }

            public static BeginSearchUnknownParser Instance
            {
                get { return instance ??= new BeginSearchUnknownParser(); }
            }

            /// <inheritdoc />
            public unsafe bool TryReadFromResp(ref byte* ptr, byte* end, out KeySpecMethodBase keySpecMethod)
            {
                keySpecMethod = default;

                RespReadUtils.ReadArrayLength(out var ksSpecElemCount, ref ptr, end);
                if (ksSpecElemCount == 0) return false;

                keySpecMethod = new BeginSearchUnknown();

                return true;
            }
        }

        /// <summary>
        /// Parser for the FindKeysRange key specification method
        /// </summary>
        internal class FindKeysRangeParser : IKeySpecParser
        {
            private static FindKeysRangeParser instance;

            private FindKeysRangeParser()
            {
            }

            public static FindKeysRangeParser Instance
            {
                get { return instance ??= new FindKeysRangeParser(); }
            }

            /// <inheritdoc />
            public unsafe bool TryReadFromResp(ref byte* ptr, byte* end, out KeySpecMethodBase keySpecMethod)
            {
                keySpecMethod = default;

                RespReadUtils.ReadArrayLength(out var specElemCount, ref ptr, end);
                if (specElemCount != 6) return false;
                RespReadUtils.ReadStringWithLengthHeader(out var argKey, ref ptr, end);
                if (!string.Equals(argKey, "lastkey", StringComparison.Ordinal)) return false;
                RespReadUtils.ReadIntegerAsString(out var strLastKey, ref ptr, end);
                if (!int.TryParse(strLastKey, out var lastKey)) return false;
                RespReadUtils.ReadStringWithLengthHeader(out argKey, ref ptr, end);
                if (!string.Equals(argKey, "keystep", StringComparison.Ordinal)) return false;
                RespReadUtils.ReadIntegerAsString(out var strKeyStep, ref ptr, end);
                if (!int.TryParse(strKeyStep, out var keyStep)) return false;
                RespReadUtils.ReadStringWithLengthHeader(out argKey, ref ptr, end);
                if (!string.Equals(argKey, "limit", StringComparison.Ordinal)) return false;
                RespReadUtils.ReadIntegerAsString(out var strLimit, ref ptr, end);
                if (!int.TryParse(strLimit, out var limit)) return false;

                keySpecMethod = new FindKeysRange(lastKey, keyStep, limit);

                return true;
            }
        }

        /// <summary>
        /// Parser for the FindKeysKeyNum key specification method
        /// </summary>
        internal class FindKeysKeyNumParser : IKeySpecParser
        {
            private static FindKeysKeyNumParser instance;

            private FindKeysKeyNumParser()
            {
            }

            public static FindKeysKeyNumParser Instance
            {
                get { return instance ??= new FindKeysKeyNumParser(); }
            }

            /// <inheritdoc />
            public unsafe bool TryReadFromResp(ref byte* ptr, byte* end, out KeySpecMethodBase keySpecMethod)
            {
                keySpecMethod = default;

                RespReadUtils.ReadArrayLength(out var specElemCount, ref ptr, end);
                if (specElemCount != 6) return false;
                RespReadUtils.ReadStringWithLengthHeader(out var argKey, ref ptr, end);
                if (!string.Equals(argKey, "keynumidx", StringComparison.Ordinal)) return false;
                RespReadUtils.ReadIntegerAsString(out var strKeyNumIdx, ref ptr, end);
                if (!int.TryParse(strKeyNumIdx, out var keyNumIdx)) return false;
                RespReadUtils.ReadStringWithLengthHeader(out argKey, ref ptr, end);
                if (!string.Equals(argKey, "firstkey", StringComparison.Ordinal)) return false;
                RespReadUtils.ReadIntegerAsString(out var strFirstKey, ref ptr, end);
                if (!int.TryParse(strFirstKey, out var firstKey)) return false;
                RespReadUtils.ReadStringWithLengthHeader(out argKey, ref ptr, end);
                if (!string.Equals(argKey, "keystep", StringComparison.Ordinal)) return false;
                RespReadUtils.ReadIntegerAsString(out var strKeyStep, ref ptr, end);
                if (!int.TryParse(strKeyStep, out var keyStep)) return false;

                keySpecMethod = new FindKeysKeyNum(keyNumIdx, firstKey, keyStep);

                return true;
            }
        }

        /// <summary>
        /// Parser for the FindKeysUnknown key specification method
        /// </summary>
        internal class FindKeysUnknownParser : IKeySpecParser
        {
            private static FindKeysUnknownParser instance;

            private FindKeysUnknownParser()
            {
            }

            public static FindKeysUnknownParser Instance
            {
                get { return instance ??= new FindKeysUnknownParser(); }
            }

            /// <inheritdoc />
            public unsafe bool TryReadFromResp(ref byte* ptr, byte* end, out KeySpecMethodBase keySpecMethod)
            {
                keySpecMethod = default;

                RespReadUtils.ReadArrayLength(out var ksSpecElemCount, ref ptr, end);
                if (ksSpecElemCount == 0) return false;

                keySpecMethod = new FindKeysUnknown();

                return true;
            }
        }
    }
}