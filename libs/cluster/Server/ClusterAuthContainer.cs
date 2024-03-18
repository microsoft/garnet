// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.cluster
{
    /// <summary>
    /// Container to hold cluster username and password for backend cluster communications.
    /// Having a container allows for atomic switching to a new username and password.
    /// </summary>
    class ClusterAuthContainer
    {
        public string ClusterUsername;
        public string ClusterPassword;
    }
}