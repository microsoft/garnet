// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net;

namespace Garnet.common
{
    /// <summary>
    /// Validation of optional client endpoint advertisements without name resolution.
    /// </summary>
    public static class ClusterEndpointValidation
    {
        /// <summary>
        /// Accepts an unset address or an IPv4 or IPv6 literal without a scope identifier.
        /// </summary>
        public static bool IsValidAddress(string address)
            => string.IsNullOrEmpty(address) ||
               (!address.Contains('%') && !address.Contains('[') &&
                IPAddress.TryParse(address, out IPAddress parsed) &&
                !parsed.Equals(IPAddress.Any) && !parsed.Equals(IPAddress.IPv6Any));

        /// <summary>
        /// Accepts an unset hostname or an ASCII DNS name with an optional final dot.
        /// </summary>
        public static bool IsValidHostname(string hostname)
        {
            if (string.IsNullOrEmpty(hostname))
                return true;

            ReadOnlySpan<char> name = hostname.AsSpan();
            if (name[^1] == '.')
                name = name[..^1];
            if (name.Length is 0 or > 253)
                return false;

            int labelLength = 0;
            char previous = '.';
            foreach (char character in name)
            {
                if (character == '.')
                {
                    if (labelLength == 0 || previous == '-')
                        return false;
                    labelLength = 0;
                }
                else
                {
                    if (!char.IsAsciiLetterOrDigit(character) && character != '-')
                        return false;
                    if ((labelLength == 0 && character == '-') || ++labelLength > 63)
                        return false;
                }
                previous = character;
            }
            return labelLength > 0 && previous != '-';
        }

        /// <summary>
        /// Zero selects the peer port; otherwise the value must be a valid TCP port.
        /// </summary>
        public static bool IsValidPort(int port) => port is >= 0 and <= ushort.MaxValue;
    }
}