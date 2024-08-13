// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Serializer interface for server-side processing
    /// </summary>
    /// <typeparam name="TKey">Key</typeparam>
    /// <typeparam name="TValue">Value</typeparam>
    /// <typeparam name="TInput">Input</typeparam>
    /// <typeparam name="TOutput">Output</typeparam>
    public unsafe interface IServerSerializer<TKey, TValue, TInput, TOutput>
    {
        /// <summary>
        /// Write element to given destination, with length bytes of space available
        /// </summary>
        /// <param name="k">Element to write</param>
        /// <param name="dst">Destination memory</param>
        /// <param name="length">Space (bytes) available at destination</param>
        /// <returns>True if write succeeded, false if not (insufficient space)</returns>
        bool Write(ref TKey k, ref byte* dst, int length);

        /// <summary>
        /// Write element to given destination, with length bytes of space available
        /// </summary>
        /// <param name="v">Element to write</param>
        /// <param name="dst">Destination memory</param>
        /// <param name="length">Space (bytes) available at destination</param>
        /// <returns>True if write succeeded, false if not (insufficient space)</returns>
        bool Write(ref TValue v, ref byte* dst, int length);

        /// <summary>
        /// Write element to given destination, with length bytes of space available
        /// </summary>
        /// <param name="o">Element to write</param>
        /// <param name="dst">Destination memory</param>
        /// <param name="length">Space (bytes) available at destination</param>
        /// <returns>True if write succeeded, false if not (insufficient space)</returns>
        bool Write(ref TOutput o, ref byte* dst, int length);

        /// <summary>
        /// Get length of given output
        /// </summary>
        /// <param name="o"></param>
        /// <returns></returns>
        int GetLength(ref TOutput o);

        /// <summary>
        /// Read key by reference, from given location
        /// </summary>
        /// <param name="src">Memory location</param>
        /// <returns>Key</returns>
        ref TKey ReadKeyByRef(ref byte* src);

        /// <summary>
        /// Read value by reference, from given location
        /// </summary>
        /// <param name="src">Memory location</param>
        /// <returns>Value</returns>
        ref TValue ReadValueByRef(ref byte* src);

        /// <summary>
        /// Read input by reference, from given location
        /// </summary>
        /// <param name="src">Memory location</param>
        /// <returns>Input</returns>
        ref TInput ReadInputByRef(ref byte* src);

        /// <summary>
        /// Read memory as output (by reference), at given location
        /// </summary>
        /// <param name="src">Memory location</param>
        /// <param name="length">Length of buffer at memory</param>
        /// <returns>Output</returns>
        ref TOutput AsRefOutput(byte* src, int length);

        /// <summary>
        /// Skip output (increment address)
        /// </summary>
        /// <param name="src">Memory location</param>
        void SkipOutput(ref byte* src);
    }
}