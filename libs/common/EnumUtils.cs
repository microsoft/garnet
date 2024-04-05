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
        private static readonly Dictionary<Type, IDictionary<string, string>> EnumDescriptionToNameCache = new();

        private static void AddTypeToCache<T>()
        {
            var valToDesc = new Dictionary<string, string>();
            var descToVal = new Dictionary<string, string>();
            foreach (var flagFieldInfo in typeof(T).GetFields())
            {
                var descAttr = (DescriptionAttribute)flagFieldInfo.GetCustomAttributes(typeof(DescriptionAttribute), false).FirstOrDefault();
                if (descAttr != null)
                {
                    valToDesc.Add(flagFieldInfo.Name, descAttr.Description);
                    descToVal.Add(descAttr.Description, flagFieldInfo.Name);
                }
            }

            EnumNameToDescriptionCache.Add(typeof(T), valToDesc);
            EnumDescriptionToNameCache.Add(typeof(T), descToVal);
        }

        public static IDictionary<string, string> GetEnumNameToDescription<T>() where T : Enum
        {
            if (!EnumNameToDescriptionCache.ContainsKey(typeof(T))) 
                AddTypeToCache<T>();

            return EnumNameToDescriptionCache[typeof(T)];
        }

        public static string[] GetEnumDescriptions<T>(T flags) where T : Enum
        {
            if (flags.Equals(default(T))) return Array.Empty<string>();

            var nameToDesc = EnumUtils.GetEnumNameToDescription<T>();
            return flags.ToString().Split(',').Select(f => nameToDesc.ContainsKey(f.Trim()) ? nameToDesc[f.Trim()] : f).ToArray();
        }

        public static bool TryParseEnumFromDescription<T>(string strVal, out T val) where T : struct, Enum
        {
            val = default;

            if (!EnumDescriptionToNameCache.ContainsKey(typeof(T)))
                AddTypeToCache<T>();

            if (!EnumDescriptionToNameCache[typeof(T)].ContainsKey(strVal)) 
                return Enum.TryParse(strVal, out val);
            
            return Enum.TryParse(EnumDescriptionToNameCache[typeof(T)][strVal], out val);
        }
    }
}
