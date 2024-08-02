// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using NLua;

namespace Garnet.server
{
    internal unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// Entry point method for executing commands from a Lua Script
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        public object ProcessCommandFromScripting(Lua state, string cmd, params object[] args)
        {
            var status = GarnetStatus.OK;

            scratchBufferManager.Reset();

            switch (cmd.ToUpper())
            {
                case "SET":
                    {
                        var key = scratchBufferManager.CreateArgSlice(Convert.ToString(args[0]));
                        var value = scratchBufferManager.CreateArgSlice(Convert.ToString(args[1]));
                        status = basicGarnetApi.SET(key, value);
                        return "OK";
                    }
                case "GET":
                    {
                        var key = scratchBufferManager.CreateArgSlice(Convert.ToString(args[0]));
                        status = basicGarnetApi.GET(key, out var value);
                        if (status == GarnetStatus.OK)
                            return value.ToString();
                        return null;
                    }
                case "ZADD":
                    {
                        var key = scratchBufferManager.CreateArgSlice(Convert.ToString(args[0]));
                        int zaddCount = 0;
                        if (args.Length == 2 && args[1] is LuaTable t)
                        {
                            var d = state.GetTableDict(t);
                            (ArgSlice score, ArgSlice member)[] values = new (ArgSlice score, ArgSlice member)[d.Count / 2];
                            int i = 0;
                            foreach (var value in d.Values)
                            {
                                if (i % 2 == 0)
                                {
                                    values[i / 2].score = scratchBufferManager.CreateArgSlice(Convert.ToString(value));
                                }
                                else
                                {
                                    values[i / 2].member = scratchBufferManager.CreateArgSlice(Convert.ToString(value));
                                }
                                i++;
                            }
                            basicGarnetApi.SortedSetAdd(key, values, out zaddCount);
                        }
                        else if (args.Length > 2 && args.Length % 2 == 1)
                        {
                            int count = (args.Length - 1) / 2;
                            (ArgSlice score, ArgSlice member)[] values = new (ArgSlice score, ArgSlice member)[count];
                            for (int i = 0; i < count; i++)
                            {
                                values[i].score = scratchBufferManager.CreateArgSlice(Convert.ToString(args[2 * i + 1]));
                                values[i].member = scratchBufferManager.CreateArgSlice(Convert.ToString(args[2 * i + 2]));
                            }
                            basicGarnetApi.SortedSetAdd(key, values, out zaddCount);
                        }
                        return zaddCount;
                    }
                default:
                    return null;
            }
        }
    }
}