// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("Garnet.test" + AssemblyRef.GarnetPublicKey)]
[assembly: InternalsVisibleTo("Garnet.fuzz" + AssemblyRef.GarnetPublicKey)]
[assembly: InternalsVisibleTo("Embedded.perftest" + AssemblyRef.GarnetPublicKey)]
[assembly: InternalsVisibleTo("BDN.benchmark" + AssemblyRef.GarnetPublicKey)]
