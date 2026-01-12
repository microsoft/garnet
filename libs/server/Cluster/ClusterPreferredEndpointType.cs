// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.ComponentModel;

namespace Garnet.server;

/// <summary>
/// Use IP address for cluster redirection(MOVED/ASK) messages
/// </summary>
public enum ClusterPreferredEndpointType
{
    /// <summary>
    /// ex -MOVED 12182 127.0.0.1:7000
    /// </summary>
    [Description("ip")]
    Ip,

    /// <summary>
    /// ex -MOVED 12182 localhost:7000
    /// if hostname is not exist
    /// ex -MOVED 12182 ?:7000
    /// </summary>
    [Description("hostname")]
    Hostname,

    /// <summary>
    /// ex -MOVED 12182 ?:7000
    /// </summary>
    [Description("unknown-endpoint")]
    UnknownEndpoint
}