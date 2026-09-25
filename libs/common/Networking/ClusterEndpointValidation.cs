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
        /// <param name="address">Client IP address</param>
        /// <returns>True if the address is valid or unset</returns>
        public static bool IsValidAddress(string address)
        {
            if (string.IsNullOrEmpty(address))
                return true;

            return !address.Contains('%') && !address.Contains('[') &&
                   IPAddress.TryParse(address, out var ipAddress) &&
                   !ipAddress.Equals(IPAddress.Any) && !ipAddress.Equals(IPAddress.IPv6Any);
        }

        /// <summary>
        /// Accepts an unset hostname or an ASCII DNS name with an optional final dot.
        /// </summary>
        /// <param name="hostname">Client hostname</param>
        /// <returns>True if the hostname is valid or unset</returns>
        public static bool IsValidHostname(string hostname)
        {
            if (string.IsNullOrEmpty(hostname))
                return true;

            var name = hostname.AsSpan();
            if (name[^1] == '.')
                name = name[..^1];
            if (name.Length is 0 or > 253)
                return false;

            var labelLength = 0;
            var previous = '.';
            foreach (var character in name)
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
        /// Validate a client port, allowing zero to select the peer port.
        /// </summary>
        /// <param name="port">Client port</param>
        /// <returns>True if the port is between 0 and 65535</returns>
        public static bool IsValidPort(int port) => port is >= 0 and <= ushort.MaxValue;
    }
}