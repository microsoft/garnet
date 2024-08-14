// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.common
{
    /// <summary>
    /// Specifies that utility methods for generating enum descriptions should be generated for the target enum.
    /// </summary>
    [AttributeUsage(AttributeTargets.Enum)]
    public sealed class GenerateEnumDescriptionUtilsAttribute : Attribute
    {
    }
}