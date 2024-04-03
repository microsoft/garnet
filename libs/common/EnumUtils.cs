// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;

namespace Garnet.common
{
    public static class EnumUtils
    {
        private static readonly Dictionary<Type, IDictionary<string, string>> EnumNameToDescriptionCache = new();

        public static IDictionary<string, string> GetEnumNameToDescription<T>() where T : Enum
        {
            if (EnumNameToDescriptionCache.ContainsKey(typeof(T))) return EnumNameToDescriptionCache[typeof(T)];

            var valToDesc = new Dictionary<string, string>();
            foreach (var flagFieldInfo in typeof(T).GetFields())
            {
                var descAttr = (DescriptionAttribute)flagFieldInfo.GetCustomAttributes(typeof(DescriptionAttribute), false).FirstOrDefault();
                if (descAttr != null)
                {
                    valToDesc.Add(flagFieldInfo.Name, descAttr.Description);
                }
            }

            EnumNameToDescriptionCache.Add(typeof(T), valToDesc);

            return valToDesc;
        }

        public static string[] GetEnumDescriptions<T>(T flags) where T : Enum
        {
            if (flags.Equals(default(T))) return Array.Empty<string>();

            var nameToDesc = EnumUtils.GetEnumNameToDescription<T>();
            return flags.ToString().Split(',').Select(f => nameToDesc.ContainsKey(f.Trim()) ? nameToDesc[f.Trim()] : f).ToArray();
        }
    }
}
