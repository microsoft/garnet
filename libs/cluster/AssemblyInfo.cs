﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("BDN.benchmark" + ClusterAssemblyRef.GarnetPublicKey)]

/// <summary>
/// Sets public key string for friend assemblies.
/// </summary>
static class ClusterAssemblyRef
{
    internal const string GarnetPublicKey = ", PublicKey=" +
        "0024000004800000940000000602000000240000525341310004000001000100011b1661238d3d" +
        "3c76232193c8aa2de8c05b8930d6dfe8cd88797a8f5624fdf14a1643141f31da05c0f67961b0e3" +
        "a64c7120001d2f8579f01ac788b0ff545790d44854abe02f42bfe36a056166a75c6a694db8c5b6" +
        "609cff8a2dbb429855a1d9f79d4d8ec3e145c74bfdd903274b7344beea93eab86b422652f8dd8e" +
        "ecf530d2";
}