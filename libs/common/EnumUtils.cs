// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;

namespace Garnet.common
{
    /// <summary>
    /// Utilities for enums
    /// </summary>
    public static class EnumUtils
    {
        private static readonly Dictionary<Type, IDictionary<string, string>> EnumNameToDescriptionCache = new();
        private static readonly Dictionary<Type, IDictionary<string, List<string>>> EnumDescriptionToNameCache = new();

        /// <summary>
        /// Gets a mapping between an enum's string value to its description, for each of the enum's values
        /// </summary>
        /// <typeparam name="T">Enum type</typeparam>
        /// <returns>A dictionary mapping between the enum's string value to its description</returns>
        public static IDictionary<string, string> GetEnumNameToDescription<T>() where T : Enum
        {
            // Check if mapping is already in the cache. If not, add it to the cache.
            if (!EnumNameToDescriptionCache.ContainsKey(typeof(T)))
                AddTypeToCache<T>();

            return EnumNameToDescriptionCache[typeof(T)];
        }

        /// <summary>
        /// If enum does not have the 'Flags' attribute, gets an array of size 1 with the description of the enum's value.
        /// If no description exists, returns the ToString() value of the input value. 
        /// If enum has the 'Flags' attribute, gets an array with all the descriptions of the flags which are turned on in the input value.
        /// If no description exists, returns the ToString() value of the flag.
        /// </summary>
        /// <typeparam name="T">Enum type</typeparam>
        /// <param name="value">Enum value</param>
        /// <returns>Array of descriptions</returns>
        public static string[] GetEnumDescriptions<T>(T value) where T : Enum
        {
            var nameToDesc = GetEnumNameToDescription<T>();
            return value.ToString().Split(',').Select(f => nameToDesc.ContainsKey(f.Trim()) ? nameToDesc[f.Trim()] : f).ToArray();
        }

        /// <summary>
        /// Gets an enum's values based on the description attribute
        /// </summary>
        /// <typeparam name="T">Enum type</typeparam>
        /// <param name="strVal">Enum description</param>
        /// <param name="vals">Enum values</param>
        /// <returns>True if matched more than one value successfully</returns>
        public static bool TryParseEnumsFromDescription<T>(string strVal, out IEnumerable<T> vals) where T : struct, Enum
        {
            vals = new List<T>();

            if (!EnumDescriptionToNameCache.ContainsKey(typeof(T)))
                AddTypeToCache<T>();

            if (!EnumDescriptionToNameCache[typeof(T)].ContainsKey(strVal))
                return false;

            foreach (var enumName in EnumDescriptionToNameCache[typeof(T)][strVal])
            {
                if (Enum.TryParse(enumName, out T enumVal))
                {
                    ((List<T>)vals).Add(enumVal);
                }
            }

            return ((List<T>)vals).Count > 0;
        }

        /// <summary>
        /// Gets an enum's value based on its description attribute
        /// If more than one values match the same description, returns the first one
        /// </summary>
        /// <typeparam name="T">Enum type</typeparam>
        /// <param name="strVal">Enum description</param>
        /// <param name="val">Enum value</param>
        /// <returns>True if successful</returns>
        public static bool TryParseEnumFromDescription<T>(string strVal, out T val) where T : struct, Enum
        {
            var isSuccessful = TryParseEnumsFromDescription(strVal, out IEnumerable<T> vals);
            val = isSuccessful ? vals.First() : default;
            return isSuccessful;
        }


        private static void AddTypeToCache<T>()
        {
            var valToDesc = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            var descToVals = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

            foreach (var flagFieldInfo in typeof(T).GetFields())
            {
                var descAttr = (DescriptionAttribute)flagFieldInfo.GetCustomAttributes(typeof(DescriptionAttribute), false).FirstOrDefault();
                if (descAttr != null)
                {
                    valToDesc.Add(flagFieldInfo.Name, descAttr.Description);
                    if (!descToVals.ContainsKey(descAttr.Description))
                        descToVals.Add(descAttr.Description, new List<string>());
                    descToVals[descAttr.Description].Add(flagFieldInfo.Name);
                }
            }

            EnumNameToDescriptionCache.Add(typeof(T), valToDesc);
            EnumDescriptionToNameCache.Add(typeof(T), descToVals);
        }
    }
}