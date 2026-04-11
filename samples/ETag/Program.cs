// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading.Tasks;

namespace ETag;

class Program
{
    static async Task Main(string[] args)
    {
        // Uncomment whichever example you want to run

        await OccSimulation.RunSimulation();
        // await Caching.RunSimulation();
    }
}