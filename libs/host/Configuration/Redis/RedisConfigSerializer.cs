// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using Microsoft.Extensions.Logging;

namespace Garnet
{
    internal class RedisConfigSerializer
    {
        // Mapping between Redis key to RedisOptions property
        // Defined by RedisOptionAttribute decorating RedisOptions properties
        private static readonly Lazy<Dictionary<string, PropertyInfo>> KeyToProperty;

        static RedisConfigSerializer()
        {
            KeyToProperty = new Lazy<Dictionary<string, PropertyInfo>>(() =>
            {
                // Initialize Redis key to RedisOptions property mapping
                var keyToProperty = new Dictionary<string, PropertyInfo>();
                foreach (var prop in GetProperties())
                {
                    var attr = (RedisOptionAttribute)prop.GetCustomAttributes(typeof(RedisOptionAttribute), false)
                        .First();
                    if (!keyToProperty.ContainsKey(attr.Key))
                    {
                        keyToProperty.Add(attr.Key, prop);
                    }
                }

                return keyToProperty;
            });
        }

        private static IEnumerable<PropertyInfo> GetProperties()
        {
            // Retrieve all RedisOptions properties that can be deserialized
            return typeof(RedisOptions)
                .GetProperties()
                .Where(prop =>
                    prop.PropertyType.IsGenericType
                    && (prop.PropertyType.GetGenericTypeDefinition() == typeof(Option<>))
                    && Attribute.IsDefined(prop, typeof(RedisOptionAttribute)));
        }

        /// <summary>
        /// Deserializes a RedisOptions object from a StreamReader
        /// </summary>
        /// <param name="reader">The stream reader to deserialize from</param>
        /// <param name="logger">Logger</param>
        /// <returns>The deserialized RedisOptions object</returns>
        /// <exception cref="RedisSerializationException"></exception>
        public static RedisOptions Deserialize(StreamReader reader, ILogger logger)
        {
            var options = new RedisOptions();

            int lineCount = 0;
            string line;
            while ((line = reader.ReadLine()) != null)
            {
                lineCount++;

                // Ignore whitespaces and comments
                if (string.IsNullOrWhiteSpace(line) || line.TrimStart().StartsWith('#'))
                    continue;

                // Expected line format: keyword argument1 argument2 ... argumentN
                var sepIdx = line.IndexOf(' ');
                if (sepIdx == -1)
                    throw new RedisSerializationException(
                        $"Unable to deserialize {nameof(RedisOptions)} object. Line {lineCount} not in expected format (keyword argument1 argument2 ... argumentN).");

                // Ignore key when no matching property found 
                var key = line.Substring(0, sepIdx);
                if (!KeyToProperty.Value.ContainsKey(key))
                {
                    logger?.LogWarning($"Redis configuration option not supported: {key}.");
                    continue;
                }

                var value = line.Substring(sepIdx + 1);

                // Get matching property & the underlying option type (T in Option<T>)
                var prop = KeyToProperty.Value[key];
                var optType = prop.PropertyType.GenericTypeArguments.First();

                // Try to deserialize the value
                if (!TryChangeType(value, typeof(string), optType, out var newVal))
                {
                    // If unsuccessful and if underlying option type is an array, try to deserialize array by elements
                    if (optType.IsArray)
                    {
                        // Split the values in the serialized array
                        var values = value.Split(' ');

                        // Instantiate a new array
                        var elemType = optType.GetElementType();
                        newVal = Array.CreateInstance(elemType, values.Length);

                        // Try deserializing and setting array elements
                        for (var i = 0; i < values.Length; i++)
                        {
                            if (!TryChangeType(values[i], typeof(string), elemType, out var elem))
                                throw new RedisSerializationException(
                                    $"Unable to deserialize {nameof(RedisOptions)} object. Unable to convert object of type {typeof(string)} to object of type {elemType}. (Line: {lineCount}; Key: {key}; Property: {prop.Name}).");


                            ((Array)newVal).SetValue(elem, i);
                        }
                    }
                    else
                    {
                        throw new RedisSerializationException(
                            $"Unable to deserialize {nameof(RedisOptions)} object. Unable to convert object of type {typeof(string)} to object of type {optType}. (Line: {lineCount}; Key: {key}; Property: {prop.Name}).");
                    }
                }

                // Create a new Option<T> object
                var newOpt = Activator.CreateInstance(prop.PropertyType);

                // Set the underlying option value
                var valueProp = prop.PropertyType.GetProperty(nameof(Option<object>.Value));
                valueProp.SetValue(newOpt, newVal);

                // Set the options property to the new option object
                prop.SetValue(options, newOpt);

                // Append usage warning, if defined
                var redisOptionAttr = (RedisOptionAttribute)prop.GetCustomAttributes(typeof(RedisOptionAttribute), false).First();
                if (!string.IsNullOrEmpty(redisOptionAttr.UsageWarning))
                {
                    logger?.LogWarning($"Redis configuration option usage warning ({key}): {redisOptionAttr.UsageWarning}");
                }
            }

            return options;
        }

        /// <summary>
        /// Serializes a RedisOptions object to a string
        /// </summary>
        /// <param name="options">The RedisOptions object to serialize</param>
        /// <returns>A string containing the serialized RedisOptions object</returns>
        public static string Serialize(RedisOptions options)
        {
            var sb = new StringBuilder();
            var properties = GetProperties();
            foreach (var prop in properties)
            {
                // Get underlying value & underlying value type for current option property
                var valueProp = prop.PropertyType.GetProperty(nameof(Option<object>.Value));
                var value = valueProp?.GetValue(options, null);
                var valueType = prop.PropertyType.GenericTypeArguments.First();

                // Get matching attribute
                var attr = (RedisOptionAttribute)prop.GetCustomAttributes(typeof(RedisOptionAttribute), false).First();

                // Try to serialize value
                if (!TryChangeType(value, valueType, typeof(string), out var serializedValue))
                    serializedValue = valueType.IsArray ? string.Join(' ', (IEnumerable)value) : value?.ToString();

                // Write to string in Redis config format
                sb.Append($"{attr.Key} {serializedValue}");
            }

            return sb.ToString();
        }

        /// <summary>
        /// Populate Options object with properties from RedisOptions object 
        /// </summary>
        /// <param name="redisOptions">The RedisOptions object to populate properties from</param>
        /// <param name="options">The Options object to populate</param>
        /// <returns>True if succeeded</returns>
        public static bool TryPopulateOptions(RedisOptions redisOptions, Options options)
        {
            foreach (var prop in GetProperties())
            {
                // Get source option value
                var srcOpt = prop.GetValue(redisOptions);

                // Ignore if source option is not set
                if (srcOpt == null) continue;

                // Get matching Options property defined by RedisOptionAttribute decorating RedisOption property
                var redisOptionAttr = (RedisOptionAttribute)prop.GetCustomAttributes(typeof(RedisOptionAttribute), false).First();
                var dstProp = typeof(Options).GetProperty(redisOptionAttr.GarnetOptionName);

                // Check if destination property exists
                if (dstProp == null)
                    throw new RedisSerializationException(
                        $"Unable to find property in {typeof(Options)} named {redisOptionAttr.GarnetOptionName} that matches {typeof(Options)} property {prop.Name}");

                // Get source option underlying value & type
                var srcOptValueProp = prop.PropertyType.GetProperty(nameof(Option<object>.Value));
                var srcOptValueType = prop.PropertyType.GenericTypeArguments.First();
                var srcOptValue = srcOptValueProp.GetValue(srcOpt);

                // Get destination property type
                var dstPropType = dstProp.PropertyType;

                // Try to convert source option to destination option
                object dstValue = null;

                // If custom transformer specified in RedisOptionAttribute, try to invoke transformer
                if (redisOptionAttr.GarnetCustomTransformer != null)
                {
                    var parameters = new[] { srcOptValue, null, null };
                    var result = (bool?)typeof(RedisConfigSerializer).GetMethod(nameof(TryApplyTransform), BindingFlags.NonPublic | BindingFlags.Static)
                        ?.MakeGenericMethod(srcOptValueType, dstPropType, redisOptionAttr.GarnetCustomTransformer)
                        .Invoke(null, parameters);
                    dstValue = parameters[1];

                    if (!result.HasValue || !result.Value)
                    {
                        throw new RedisSerializationException((string)parameters[2]);
                    }
                }
                // Try to convert to destination type
                else if (srcOptValueType != dstPropType)
                {
                    if (!TryChangeType(srcOptValue, srcOptValueType, dstPropType, out dstValue))
                    {
                        throw new RedisSerializationException(
                            $"Unable to convert property {prop.Name} in {typeof(RedisOptions)} to property {redisOptionAttr.GarnetOptionName} in {typeof(Options)}.");
                    }
                }
                // Source and destination types are the same with no transformer specified
                else
                {
                    dstValue = srcOptValue;
                }

                // Set the destination property with the converted value
                dstProp.SetValue(options, dstValue);
            }

            return true;
        }

        /// <summary>
        /// Tries to apply a custom IGarnetCustomTransformer to transform a RedisOptions property to an Options property
        /// </summary>
        /// <typeparam name="TIn">Type of source value</typeparam>
        /// <typeparam name="TOut">Type of destination value</typeparam>
        /// <typeparam name="TTransformer">Type of transformer</typeparam>
        /// <param name="input">Source value</param>
        /// <param name="output">Transformed source value</param>
        /// <param name="errorMessage">Error message, empty if no error occurred</param>
        /// <returns>True if transformed value successfully</returns>
        internal static bool TryApplyTransform<TIn, TOut, TTransformer>(TIn input, out TOut output, out string errorMessage)
        {
            errorMessage = null;
            output = default;

            // Check if transformer TIn & TOut variables match the source & destination types
            if (!typeof(IGarnetCustomTransformer<TIn, TOut>).IsAssignableFrom(typeof(TTransformer)))
            {
                errorMessage =
                    $"Specified transformer for RedisOptions property does not match given types (Source: {typeof(TIn)}, Destination: {typeof(TOut)}).";
                return false;
            }

            // Instantiate transformer
            var transformer = Activator.CreateInstance<TTransformer>();
            var parameters = new object[] { input, null, null };
            // Invoke Transform method
            var result = (bool?)typeof(TTransformer)
                .GetMethod(nameof(IGarnetCustomTransformer<object, object>.Transform))
                ?.Invoke(transformer, parameters);

            output = (TOut)parameters[1];
            errorMessage = (string)parameters[2];

            return result.HasValue && result.Value;
        }

        /// <summary>
        /// Tries to convert src object of type srcType to type dstType
        /// </summary>
        /// <param name="src">Source object to convert</param>
        /// <param name="srcType">Source object type</param>
        /// <param name="dstType">Converted source object type</param>
        /// <param name="dst">Converted source object</param>
        /// <returns>True if conversion succeeded</returns>
        private static bool TryChangeType(object src, Type srcType, Type dstType, out object dst)
        {
            dst = default;

            // If TDst type has a converter from TSrc, use the converter to deserialize.
            var converter = TypeDescriptor.GetConverter(dstType);
            if (converter.CanConvertFrom(srcType))
            {
                dst = converter.ConvertFrom(src);
                return true;
            }

            // If TSrc type has a converter to TDst, use the converter to deserialize.
            converter = TypeDescriptor.GetConverter(srcType);
            if (converter.CanConvertTo(dstType))
            {
                dst = converter.ConvertTo(src, dstType);
                return true;
            }

            // Try to convert to TDst using Convert.ChangeType
            try
            {
                dst = Convert.ChangeType(src, dstType);
            }
            catch (Exception e) when (e is InvalidCastException
                                      || e is FormatException
                                      || e is OverflowException
                                      || e is ArgumentException)
            {
                return false;
            }

            return true;
        }
    }

    internal class RedisSerializationException : Exception
    {
        public RedisSerializationException()
        {
        }

        public RedisSerializationException(string message)
            : base(message)
        {
        }

        public RedisSerializationException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }

}