// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Text;
using Garnet.common;

namespace Garnet.server
{
    internal class RespCommandInfoParser
    {
        internal static unsafe bool TryReadFromResp(ref byte* ptr, byte* end, out RespCommandsInfo commandInfo)
        {
            commandInfo = default;

            if (Encoding.ASCII.GetString(ptr, 5) == "$-1\r\n") return true;

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
                if (!TryReadFromResp(ref ptr, end, out commandInfo))
                    return false;

                subCommands.Add(commandInfo);
            }

            commandInfo = new RespCommandsInfo()
            {
                Name = name.ToUpper(),
                Arity = arity,
                Flags = flags,
                FirstKey = firstKey,
                LastKey = lastKey,
                Step = step,
                AclCategories = aclCategories,
                Tips = tips.Length == 0 ? null : tips,
                KeySpecifications = keySpecifications.Length == 0 ? null : keySpecifications,
                SubCommands = subCommands.Count == 0 ? null : subCommands.ToArray()
            };

            return true;
        }
    }

    internal class RespKeySpecificationParser
    {
        internal static unsafe bool TryReadFromResp(ref byte* ptr, byte* end, out RespCommandKeySpecification keySpec)
        {
            keySpec = default;

            string notes = null;
            var flags = KeySpecificationFlags.None;
            KeySpecBase beginSearch = null;
            KeySpecBase findKeys = null;

            RespReadUtils.ReadArrayLength(out var elemCount, ref ptr, end);

            for (var elemIdx = 0; elemIdx < elemCount; elemIdx += 2)
            {
                RespReadUtils.ReadStringWithLengthHeader(out var ksKey, ref ptr, end);
                switch (ksKey)
                {
                    case "notes":
                        RespReadUtils.ReadStringWithLengthHeader(out notes, ref ptr, end);
                        break;
                    case "flags":
                        RespReadUtils.ReadArrayLength(out var flagsCount, ref ptr, end);
                        for (var flagIdx = 0; flagIdx < flagsCount; flagIdx++)
                        {
                            RespReadUtils.ReadSimpleString(out var strFlag, ref ptr, end);
                            if (!EnumUtils.TryParseEnumFromDescription<KeySpecificationFlags>(strFlag,
                                    out var flag)) return false;
                            flags |= flag;
                        }

                        break;
                    case "begin_search":
                        if (!RespKeySpecificationTypesParser.TryReadFromResp(ksKey, ref ptr, end, out beginSearch)) return false;
                        break;
                    case "find_keys":
                        if (!RespKeySpecificationTypesParser.TryReadFromResp(ksKey, ref ptr, end, out findKeys)) return false;
                        break;
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

    internal class RespKeySpecificationTypesParser
    {
        public static unsafe bool TryReadFromResp(string keySpecKey, ref byte* ptr, byte* end, out KeySpecBase keySpec)
        {
            keySpec = default;

            if (!TryReadKeySpecHeader(ref ptr, end, out var keySpecType)) return false;

            IKeySpecParser parser = (keySpecKey, keySpecType) switch
            {
                ("begin_search", "index") => BeginSearchIndexParser.Instance,
                ("begin_search", "keyword") => BeginSearchKeywordParser.Instance,
                ("begin_search", "unknown") => BeginSearchUnknownParser.Instance,
                ("find_keys", "range") => FindKeysRangeParser.Instance,
                ("find_keys", "keynum") => FindKeysKeyNumParser.Instance,
                ("find_keys", "unknown") => FindKeysUnknownParser.Instance,
                _ => null
            };

            if (parser == null || !parser.TryReadFromResp(ref ptr, end, out keySpec)) return false;

            return true;
        }

        private static unsafe bool TryReadKeySpecHeader(ref byte* ptr, byte* end, out string keySpecType)
        {
            keySpecType = default;

            RespReadUtils.ReadArrayLength(out var ksTypeElemCount, ref ptr, end);
            if (ksTypeElemCount != 4) return false;
            RespReadUtils.ReadStringWithLengthHeader(out var ksTypeStr, ref ptr, end);
            if (ksTypeStr != "type") return false;
            RespReadUtils.ReadStringWithLengthHeader(out var ksType, ref ptr, end);
            RespReadUtils.ReadStringWithLengthHeader(out var ksSpecStr, ref ptr, end);
            if (ksSpecStr != "spec") return false;

            keySpecType = ksType;
            return true;
        }

        internal interface IKeySpecParser
        {
            unsafe bool TryReadFromResp(ref byte* ptr, byte* end, out KeySpecBase keySpec);
        }

        internal sealed class BeginSearchIndexParser : IKeySpecParser
        {
            private static BeginSearchIndexParser instance;

            private BeginSearchIndexParser()
            {
            }

            public static BeginSearchIndexParser Instance
            {
                get { return instance ??= new BeginSearchIndexParser(); }
            }

            public unsafe bool TryReadFromResp(ref byte* ptr, byte* end, out KeySpecBase keySpec)
            {
                keySpec = default;

                RespReadUtils.ReadArrayLength(out var ksSpecElemCount, ref ptr, end);
                if (ksSpecElemCount != 2) return false;
                RespReadUtils.ReadStringWithLengthHeader(out var ksArgKey, ref ptr, end);
                if (ksArgKey != "index") return false;
                RespReadUtils.ReadIntegerAsString(out var strIndex, ref ptr, end);
                if (!int.TryParse(strIndex, out var index)) return false;

                keySpec = new BeginSearchIndex(index);

                return true;
            }
        }


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

            public unsafe bool TryReadFromResp(ref byte* ptr, byte* end, out KeySpecBase keySpec)
            {
                keySpec = default;

                RespReadUtils.ReadArrayLength(out var specElemCount, ref ptr, end);
                if (specElemCount != 4) return false;
                RespReadUtils.ReadStringWithLengthHeader(out var argKey, ref ptr, end);
                if (argKey != "keyword") return false;
                RespReadUtils.ReadStringWithLengthHeader(out var keyword, ref ptr, end);
                RespReadUtils.ReadIntegerAsString(out var strStartFrom, ref ptr, end);
                if (!int.TryParse(strStartFrom, out var startFrom)) return false;

                keySpec = new BeginSearchKeyword(keyword, startFrom);

                return true;
            }
        }

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

            public unsafe bool TryReadFromResp(ref byte* ptr, byte* end, out KeySpecBase keySpec)
            {
                keySpec = default;

                RespReadUtils.ReadArrayLength(out var ksSpecElemCount, ref ptr, end);
                if (ksSpecElemCount == 0) return false;

                keySpec = new BeginSearchUnknown();

                return true;
            }
        }

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

            public unsafe bool TryReadFromResp(ref byte* ptr, byte* end, out KeySpecBase keySpec)
            {
                keySpec = default;

                RespReadUtils.ReadArrayLength(out var specElemCount, ref ptr, end);
                if (specElemCount != 6) return false;
                RespReadUtils.ReadStringWithLengthHeader(out var argKey, ref ptr, end);
                if (argKey != "lastkey") return false;
                RespReadUtils.ReadIntegerAsString(out var strLastKey, ref ptr, end);
                if (!int.TryParse(strLastKey, out var lastKey)) return false;
                RespReadUtils.ReadStringWithLengthHeader(out argKey, ref ptr, end);
                if (argKey != "keystep") return false;
                RespReadUtils.ReadIntegerAsString(out var strKeyStep, ref ptr, end);
                if (!int.TryParse(strKeyStep, out var keyStep)) return false;
                RespReadUtils.ReadStringWithLengthHeader(out argKey, ref ptr, end);
                if (argKey != "limit") return false;
                RespReadUtils.ReadIntegerAsString(out var strLimit, ref ptr, end);
                if (!int.TryParse(strLimit, out var limit)) return false;

                keySpec = new FindKeysRange(lastKey, keyStep, limit);

                return true;
            }
        }

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

            public unsafe bool TryReadFromResp(ref byte* ptr, byte* end, out KeySpecBase keySpec)
            {
                keySpec = default;

                RespReadUtils.ReadArrayLength(out var specElemCount, ref ptr, end);
                if (specElemCount != 6) return false;
                RespReadUtils.ReadStringWithLengthHeader(out var argKey, ref ptr, end);
                if (argKey != "keynumidx") return false;
                RespReadUtils.ReadIntegerAsString(out var strKeyNumIdx, ref ptr, end);
                if (!int.TryParse(strKeyNumIdx, out var keyNumIdx)) return false;
                RespReadUtils.ReadStringWithLengthHeader(out argKey, ref ptr, end);
                if (argKey != "firstkey") return false;
                RespReadUtils.ReadIntegerAsString(out var strFirstKey, ref ptr, end);
                if (!int.TryParse(strFirstKey, out var firstKey)) return false;
                RespReadUtils.ReadStringWithLengthHeader(out argKey, ref ptr, end);
                if (argKey != "keystep") return false;
                RespReadUtils.ReadIntegerAsString(out var strKeyStep, ref ptr, end);
                if (!int.TryParse(strKeyStep, out var keyStep)) return false;

                keySpec = new FindKeysKeyNum(keyNumIdx, firstKey, keyStep);

                return true;
            }
        }

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

            public unsafe bool TryReadFromResp(ref byte* ptr, byte* end, out KeySpecBase keySpec)
            {
                keySpec = default;

                RespReadUtils.ReadArrayLength(out var ksSpecElemCount, ref ptr, end);
                if (ksSpecElemCount == 0) return false;

                keySpec = new FindKeysUnknown();

                return true;
            }
        }
    }
}
