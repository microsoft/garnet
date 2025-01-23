// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet;

public class GarnetApplicationOptions
{
    /// <summary>
    /// The command line arguments.
    /// </summary>
    public string[] Args { get; init; }

    /// <summary>
    /// The environment name.
    /// </summary>
    public string EnvironmentName { get; init; }

    /// <summary>
    /// The application name.
    /// </summary>
    public string ApplicationName { get; init; }
}