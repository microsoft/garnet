// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Globalization;
using System.Text.RegularExpressions;

namespace Tsavorite.test
{
    static class NameValidator
    {
        const int ContainerShareQueueTableMinLength = 3;
        const int ContainerShareQueueTableMaxLength = 63;
        static readonly RegexOptions RegexOptions = RegexOptions.Singleline | RegexOptions.ExplicitCapture | RegexOptions.CultureInvariant;
        static readonly Regex ShareContainerQueueRegex = new("^[a-z0-9]+(-[a-z0-9]+)*$", RegexOptions);

        /// <summary>
        /// Checks if a container name is valid.
        /// </summary>
        /// <param name="containerName">A string representing the container name to validate.</param>
        public static void ValidateContainerName(string containerName)
        {
            if (!("$root".Equals(containerName, StringComparison.Ordinal) || "$logs".Equals(containerName, StringComparison.Ordinal)))
            {
                ValidateShareContainerQueueHelper(containerName, "Container");
            }
        }

        static void ValidateShareContainerQueueHelper(string resourceName, string resourceType)
        {
            if (string.IsNullOrWhiteSpace(resourceName))
            {
                throw new ArgumentException(string.Format(CultureInfo.InvariantCulture, "ResourceNameEmpty", resourceType));
            }

            if (resourceName.Length < ContainerShareQueueTableMinLength || resourceName.Length > ContainerShareQueueTableMaxLength)
            {
                throw new ArgumentException(string.Format(CultureInfo.InvariantCulture, "InvalidResourceNameLength", resourceType, ContainerShareQueueTableMinLength, ContainerShareQueueTableMaxLength));
            }

            if (!ShareContainerQueueRegex.IsMatch(resourceName))
            {
                throw new ArgumentException(string.Format(CultureInfo.InvariantCulture, "InvalidResourceName", resourceType));
            }
        }
    }
}