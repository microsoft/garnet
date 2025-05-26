// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
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
        static EndPoint[] defaultBindAny(int port)
            => Socket.OSSupportsIPv6 ? [new IPEndPoint(IPAddress.Any, port), new IPEndPoint(IPAddress.IPv6Any, port)] : [new IPEndPoint(IPAddress.Any, port)];

        static EndPoint[] defaultBindLoopBack(int port)
            => Socket.OSSupportsIPv6 ? [new IPEndPoint(IPAddress.Loopback, port), new IPEndPoint(IPAddress.IPv6Loopback, port)] : [new IPEndPoint(IPAddress.Loopback, port)];

        /// <summary>
        /// Parse address list string containing address separated by whitespace
        /// </summary>
        /// <param name="addressList">Space separated string of IP addresses</param>
        /// <param name="port">Endpoint Port</param>
        /// <param name="endpoints">List of endpoints generated from the input IPs</param>
        /// <param name="errorHostnameOrAddress">Output error if any</param>
        /// <param name="protectedMode">Is protected mode enabled?</param>
        /// <param name="logger">Logger</param>
        /// <returns>True if parse and address validation was successful, otherwise false</returns>
        public static bool TryParseAddressList(string addressList, int port, out EndPoint[] endpoints, out string errorHostnameOrAddress,
                                               bool protectedMode = false, ILogger logger = null)
        {
            endpoints = null;
            errorHostnameOrAddress = null;
            // Check if input null or empty
            if (string.IsNullOrEmpty(addressList) || string.IsNullOrWhiteSpace(addressList))
            {
                endpoints = protectedMode ? defaultBindLoopBack(port) : defaultBindAny(port);
                return true;
            }

            var addresses = addressList.Split([',',' '], StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            var endpointList = new List<EndPoint>();
            // Validate addresses and create endpoints
            foreach (var singleAddressOrHostname in addresses)
            {
                var e = TryCreateEndpoint(singleAddressOrHostname, port, tryConnect: false, logger).Result;
                if(e == null)
                {
                    endpoints = null;
                    errorHostnameOrAddress = singleAddressOrHostname;
                    return false;
                }
                endpointList.AddRange(e);
            }
            endpoints = [.. endpointList];

            return true;
        }

        /// <summary>
        /// Try to create an endpoint from address and port
        /// </summary>
        /// <param name="singleAddressOrHostname">This could be an address or a hostname that the method tries to resolve</param>
        /// <param name="port">Port number to use for the endpoints</param>
        /// <param name="tryConnect">Whether to try to connect to the created endpoints to ensure that it is reachable</param>
        /// <param name="logger">Logger</param>
        /// <returns></returns>
        public static async Task<EndPoint[]> TryCreateEndpoint(string singleAddressOrHostname, int port, bool tryConnect = false, ILogger logger = null)
        {
            if (string.IsNullOrEmpty(singleAddressOrHostname) || string.IsNullOrWhiteSpace(singleAddressOrHostname))
                return defaultBindAny(port);

            if (singleAddressOrHostname[0] == '-')
                singleAddressOrHostname = singleAddressOrHostname.Substring(1);

            if (singleAddressOrHostname.Equals("localhost", StringComparison.CurrentCultureIgnoreCase))
                return defaultBindLoopBack(port);

            if (IPAddress.TryParse(singleAddressOrHostname, out var ipAddress))
                return [new IPEndPoint(ipAddress, port)];

            // Sanity check, there should be at least one ip address available
            try
            {
                var ipAddresses = Dns.GetHostAddresses(singleAddressOrHostname);
                if (ipAddresses.Length == 0)
                {
                    logger?.LogError("No IP address found for hostname:{hostname}", singleAddressOrHostname);
                    return null;
                }

                if (tryConnect)
                {
                    foreach (var entry in ipAddresses)
                    {
                        var endpoint = new IPEndPoint(entry, port);
                        var IsListening = await TryConnect(endpoint);
                        if (IsListening) return [endpoint];
                    }
                }
                else
                {
                    var machineHostname = GetHostName();

                    // User-provided hostname does not match the machine hostname
                    if (!singleAddressOrHostname.Equals(machineHostname, StringComparison.OrdinalIgnoreCase))
                    {
                        logger?.LogError("Provided hostname does not much acquired machine name {addressOrHostname} {machineHostname}!", singleAddressOrHostname, machineHostname);
                        return null;
                    }

                    return ipAddresses.Select(ip => new IPEndPoint(ip, port)).ToArray();
                }
                logger?.LogError("No reachable IP address found for hostname:{hostname}", singleAddressOrHostname);
            }
            catch (Exception ex)
            {
                logger?.LogError("Error while trying to resolve hostname: {exMessage} [{hostname}]", ex.Message, singleAddressOrHostname);
            }

            return null;

            async Task<bool> TryConnect(IPEndPoint endpoint)
            {
                using (var tcpClient = new TcpClient())
                {
                    try
                    {
                        await tcpClient.ConnectAsync(endpoint.Address, endpoint.Port);
                        logger?.LogTrace("Reachable {ip} {port}", endpoint.Address, endpoint.Port);
                        return true;
                    }
                    catch
                    {
                        logger?.LogTrace("Unreachable {ip} {port}", endpoint.Address, endpoint.Port);
                        return false;
                    }
                }
            }
        }

        /// <summary>
        /// Parse address (hostname) and port to endpoint
        /// </summary>
        /// <param name="address"></param>
        /// <param name="port"></param>
        /// <param name="endpoint"></param>
        /// <returns></returns>
        public static bool TryValidateAddress(string address, int port, out EndPoint endpoint)
        {
            endpoint = null;

            if (string.IsNullOrWhiteSpace(address))
            {
                endpoint = new IPEndPoint(IPAddress.Any, port);
                return true;
            }

            if (IPAddress.TryParse(address, out var ipAddress))
            {
                endpoint = new IPEndPoint(ipAddress, port);
                return true;
            }

            var machineHostname = GetHostName();

            // Hostname does match then one acquired from machine name
            if (!address.Equals(machineHostname, StringComparison.OrdinalIgnoreCase))                
                return false;

            // Sanity check, there should be at least one ip address available
            var ipAddresses = Dns.GetHostAddresses(address);
            if (ipAddresses.Length == 0)
                return false;

            // Listen to any since we were given a valid hostname
            endpoint = new IPEndPoint(IPAddress.Any, port);
            return true;
        }

        /// <summary>
        /// Resolve host from Ip
        /// </summary>
        /// <param name="logger"></param>
        /// <returns></returns>
        public static string GetHostName(ILogger logger = null)
        {
            try
            {
                var serverName = Environment.MachineName; // host name sans domain
                var fqhn = Dns.GetHostEntry(serverName).HostName; // fully qualified hostname
                return fqhn;
            }
            catch (SocketException ex)
            {
                logger?.LogError(ex, "GetHostName threw an error");
            }

            return "";
        }

        public static string MemoryBytes(long size)
        {
            if( size < (1 << 20))
                return KiloBytes(size);
            else if (size < (1 << 30))
                return MegaBytes(size);
            else return GigaBytes(size);
        }

        public static string GigaBytes(long size) => (((size - 1) >> 30) + 1).ToString("n0") + "GB";

        public static string MegaBytes(long size) => (((size - 1) >> 20) + 1).ToString("n0") + "MB";

        public static string KiloBytes(long size) =>(((size - 1) >> 10) + 1).ToString("n0") + "KB";
    }
#pragma warning restore format
}