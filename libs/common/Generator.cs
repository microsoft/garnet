// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;

namespace Garnet.common
{
    /// <summary>
    /// Collection of methods generating hex ids
    /// </summary>
    public class Generator
    {
        static ReadOnlySpan<byte> hexchars => "0123456789abcdef"u8;

        /// <summary>
        /// Random hex id
        /// </summary>
        /// <param name="size"></param>
        /// <returns></returns>
        public static string CreateHexId(int size = 40)
        {
            var NodeId = new byte[size];
            new Random(Guid.NewGuid().GetHashCode()).NextBytes(NodeId);
            for (int i = 0; i < NodeId.Length; i++) NodeId[i] = hexchars[NodeId[i] & 0xf];
            return Encoding.ASCII.GetString(NodeId);
        }

        /// <summary>
        /// Default hex id
        /// </summary>
        /// <param name="size"></param>
        /// <returns></returns>
        public static string DefaultHexId(int size = 40)
        {
            var NodeId = new byte[size];
            for (int i = 0; i < NodeId.Length; i++) NodeId[i] = hexchars[0];
            return Encoding.ASCII.GetString(NodeId);
        }
    }
}