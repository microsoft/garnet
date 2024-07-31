// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.server
{
    internal unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// Entry point method for executing commands from a Lua Script
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        public object ProcessCommandFromScripting(string cmd, params object[] args)
        {
            var status = GarnetStatus.OK;

            scratchBufferManager.Reset();

            switch (cmd.ToUpper())
            {
                case "SET":
                    {
                        var key = scratchBufferManager.CreateArgSlice((string)args[0]);
                        var value = scratchBufferManager.CreateArgSlice(Convert.ToString(args[1]));
                        status = basicGarnetApi.SET(key, value);
                        return "OK";
                    }
                case "GET":
                    {
                        var key = scratchBufferManager.CreateArgSlice((string)args[0]);
                        status = basicGarnetApi.GET(key, out var value);
                        if (status == GarnetStatus.OK)
                            return value.ToString();
                        return null;
                    }
                case "ZADD":
                    {
                        var key = scratchBufferManager.CreateArgSlice((string)args[0]);
                        int count = (args.Length - 1) / 2;
                        (ArgSlice score, ArgSlice member)[] values = new (ArgSlice score, ArgSlice member)[count];
                        for (int i = 0; i < count; i++)
                        {
                            values[i].score = scratchBufferManager.CreateArgSlice((string)args[2 * i + 1]);
                            values[i].member = scratchBufferManager.CreateArgSlice((string)args[2 * i + 2]);
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
