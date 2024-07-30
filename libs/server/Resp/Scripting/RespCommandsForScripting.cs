// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    internal unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// Entry point method for executing commands from a Lua Script
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        public object ProcessCommandFromScripting(string[] args)
        {
            var status = GarnetStatus.OK;

            scratchBufferManager.Reset();

            switch (args[0].ToUpper())
            {
                case "SET":
                    {
                        var key = scratchBufferManager.CreateArgSlice(args[1]);
                        var value = scratchBufferManager.CreateArgSlice(args[2]);
                        status = basicGarnetApi.SET(key, value);
                        return "OK";
                    }
                case "GET":
                    {
                        var key = scratchBufferManager.CreateArgSlice(args[1]);
                        status = basicGarnetApi.GET(key, out var value);
                        if (status == GarnetStatus.OK)
                            return value.ToString();
                        return null;
                    }
                case "ZADD":
                    {
                        var key = scratchBufferManager.CreateArgSlice(args[1]);
                        int count = (args.Length - 2) / 2;
                        (ArgSlice score, ArgSlice member)[] values = new (ArgSlice score, ArgSlice member)[count];
                        for (int i = 0; i < count; i++)
                        {
                            values[i].score = scratchBufferManager.CreateArgSlice(args[2 * i + 2]);
                            values[i].member = scratchBufferManager.CreateArgSlice(args[2 * i + 3]);
                        }
                        basicGarnetApi.SortedSetAdd(key, values, out int zaddCount);
                        return zaddCount;
                    }
                default:
                    return null;
            }
        }
    }
}
