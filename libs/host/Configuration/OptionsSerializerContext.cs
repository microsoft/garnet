// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text.Json.Serialization;

namespace Garnet
{
    /// <summary>
    /// Json serializer context for Garnet configuration
    /// </summary>
    [JsonSerializable(typeof(Options), TypeInfoPropertyName = "OptionsType")]
    [JsonSourceGenerationOptions(WriteIndented = true)]
    internal partial class OptionsSerializerContext : JsonSerializerContext;
}