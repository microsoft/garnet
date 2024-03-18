// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Garnet
{
    /// <summary>
    /// Defines an interface for custom transformers that transform a RedisOptions property value to an Options property value and vice versa
    /// The custom transformer is specified by the RedisOptionAttribute decorating the RedisOptions property
    /// </summary>
    /// <typeparam name="TIn">RedisOptions property value type</typeparam>
    /// <typeparam name="TOut">Options property value type</typeparam>
    internal interface IGarnetCustomTransformer<TIn, TOut>
    {
        /// <summary>
        /// Transforms a RedisOptions property value to an Options property value
        /// </summary>
        /// <param name="input">RedisOptions property value</param>
        /// <param name="output">Options property value</param>
        /// <param name="errorMessage">Error message, empty if no error occurred</param>
        /// <returns>True if transform successful</returns>
        bool Transform(TIn input, out TOut output, out string errorMessage);

        /// <summary>
        /// Transforms an Options property value back to a RedisOptions property value
        /// </summary>
        /// <param name="input">Options property value</param>
        /// <param name="output">RedisOptions property value</param>
        /// <param name="errorMessage">Error message, empty if no error occurred</param>
        /// <returns>True if transform successful</returns>
        bool TransformBack(TOut input, out TIn output, out string errorMessage);
    }

    /// <summary>
    /// Transforms a string containing a file path to a string containing the contents of the file in the specified path
    /// </summary>
    internal class FileToContentTransformer : IGarnetCustomTransformer<string, string>
    {
        public bool Transform(string path, out string contents, out string errorMessage)
        {
            contents = null;
            errorMessage = null;

            try
            {
                using FileStream stream = File.OpenRead(path);
                using var streamReader = new StreamReader(stream);
                contents = streamReader.ReadToEnd();
            }
            catch (Exception e)
            {
                errorMessage = $"An error occurred while reading from file: {path}. Error message: {e.Message}";
                return false;
            }

            return true;
        }

        public bool TransformBack(string input, out string output, out string errorMessage)
        {
            throw new NotImplementedException();
        }
    }

    /// <summary>
    /// Transforms an array of type T to an object of type T by taking only the first element in the array
    /// </summary>
    /// <typeparam name="T">The type of the array</typeparam>
    internal class ArrayToFirstItemTransformer<T> : IGarnetCustomTransformer<T[], T>
    {
        public bool Transform(T[] input, out T output, out string errorMessage)
        {
            errorMessage = null;
            output = input == null || input.Length == 0 ? default : input.First();
            return true;
        }

        public bool TransformBack(T input, out T[] output, out string errorMessage)
        {
            errorMessage = null;
            output = new[] { input };
            return true;
        }
    }

    /// <summary>
    /// Transforms an object of type T to a nullable boolean by setting boolean to True if object is non-default, and False otherwise
    /// </summary>
    /// <typeparam name="T">The type of the input object</typeparam>
    internal class NonDefaultObjectToBooleanTransformer<T> : IGarnetCustomTransformer<T, bool?>
    {
        public bool Transform(T input, out bool? output, out string errorMessage)
        {
            errorMessage = null;
            output = !EqualityComparer<T>.Default.Equals(input, default);
            return true;
        }

        public bool TransformBack(bool? input, out T output, out string errorMessage)
        {
            throw new NotImplementedException();
        }
    }
}