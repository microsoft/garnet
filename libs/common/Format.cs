// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;

#if UNIX_SOCKET
using System.Net.Sockets;
#endif

namespace Garnet.common
{
    internal static class ExtensionMethodsInternal
    {
        internal static bool IsNullOrEmpty([NotNullWhen(false)] this string s) =>
            string.IsNullOrEmpty(s);

#pragma warning disable format
        internal static bool IsNullOrWhiteSpace([NotNullWhen(false)] this string s) =>
            string.IsNullOrWhiteSpace(s);
#pragma warning restore format
    }

    /// <summary>
    /// Formatting primitives
    /// </summary>
#pragma warning disable format
    public static class Format
    {
        /// <summary>
        /// Parse Endpoint in the form address:port
        /// </summary>
        /// <param name="addressWithPort"></param>
        /// <param name="endpoint"></param>
        /// <returns></returns>
        /// <exception cref="PlatformNotSupportedException"></exception>
#nullable enable
        public static bool TryParseEndPoint(string addressWithPort, [NotNullWhen(true)] out EndPoint? endpoint)
#nullable disable
        {
            string addressPart;
#pragma warning disable CS8600 // Converting null literal or possible null value to non-nullable type.
            string portPart = null;
#pragma warning restore CS8600 // Converting null literal or possible null value to non-nullable type.
            if (addressWithPort.IsNullOrEmpty())
            {
#pragma warning disable CS8625 // Cannot convert null literal to non-nullable reference type.
                endpoint = null;
#pragma warning restore CS8625 // Cannot convert null literal to non-nullable reference type.
                return false;
            }

            if (addressWithPort[0] == '!')
            {
                if (addressWithPort.Length == 1)
                {
                    endpoint = null;
                    return false;
                }

#if UNIX_SOCKET
                endpoint = new UnixDomainSocketEndPoint(addressWithPort.Substring(1));
                return true;
#else
                throw new PlatformNotSupportedException("Unix domain sockets require .NET Core 3 or above");
#endif
            }
            var lastColonIndex = addressWithPort.LastIndexOf(':');
            if (lastColonIndex > 0)
            {
                // IPv4 with port or IPv6
                var closingIndex = addressWithPort.LastIndexOf(']');
                if (closingIndex > 0)
                {
                    // IPv6 with brackets
                    addressPart = addressWithPort.Substring(1, closingIndex - 1);
                    if (closingIndex < lastColonIndex)
                    {
                        // IPv6 with port [::1]:80
                        portPart = addressWithPort.Substring(lastColonIndex + 1);
                    }
                }
                else
                {
                    // IPv6 without port or IPv4
                    var firstColonIndex = addressWithPort.IndexOf(':');
                    if (firstColonIndex != lastColonIndex)
                    {
                        // IPv6 ::1
                        addressPart = addressWithPort;
                    }
                    else
                    {
                        // IPv4 with port 127.0.0.1:123
                        addressPart = addressWithPort.Substring(0, firstColonIndex);
                        portPart = addressWithPort.Substring(firstColonIndex + 1);
                    }
                }
            }
            else
            {
                // IPv4 without port
                addressPart = addressWithPort;
            }

            int? port = 0;
            if (portPart != null)
            {
                if (TryParseInt32(portPart, out var portVal))
                {
                    port = portVal;
                }
                else
                {
                    // Invalid port, return
                    endpoint = null;
                    return false;
                }
            }

            if (IPAddress.TryParse(addressPart, out IPAddress address))
            {
                endpoint = new IPEndPoint(address, port ?? 0);
                return true;
            }
            else
            {
                IPHostEntry host = Dns.GetHostEntryAsync(addressPart).Result;
                var ip = host.AddressList.First(x => x.AddressFamily == AddressFamily.InterNetwork);
                endpoint = new IPEndPoint(ip, port ?? 0);
                return true;
            }
        }

        /// <summary>
        /// TryParseInt32
        /// </summary>
        /// <param name="s"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static bool TryParseInt32(string s, out int value) =>
            int.TryParse(s, NumberStyles.Integer, NumberFormatInfo.InvariantInfo, out value);

        /// <summary>
        /// Resolve host from Ip
        /// </summary>
        /// <param name="logger"></param>
        /// <returns></returns>
#nullable enable
        public static string GetHostName(ILogger? logger = null)
#nullable disable
        {
            try
            {
                var serverName = Environment.MachineName; //host name sans domain
                var fqhn = Dns.GetHostEntry(serverName).HostName; //fully qualified hostname
                return fqhn;
            }
            catch (SocketException ex)
            {
                logger?.LogError(ex, "GetHostName threw an error");
            }

            return "";
        }
    }
#pragma warning restore format
}