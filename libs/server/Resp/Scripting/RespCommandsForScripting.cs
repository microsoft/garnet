// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;

namespace Garnet.server
{
    internal unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// Entry point method for executing commands from a Lua Script
        /// </summary>
        /// <param name="cmd"></param>
        /// <param name="lockable">Indicates if the operation requires a lockable or a basic context</param>
        /// <param name="key"></param>
        /// <param name="args"></param>
        /// <returns></returns>
        public object ProcessCommandFromScripting(string cmd, bool lockable, (ArgSlice, bool) key, params object[] args)
        {
            var status = GarnetStatus.OK;

            scratchBufferManager.Reset();

            switch (cmd)
            {
                case var testCommand when string.Equals(testCommand, "SET", StringComparison.InvariantCultureIgnoreCase):
                    if (args[0] != null)
                    {
                        status = TryGetArgSliceFromArg(args[0], out ArgSlice valueArg)
                                ? lockableGarnetApi.SET(key.Item1, valueArg)
                                : GarnetStatus.NOTFOUND;
                    }
                    break;
                case var testCommand when string.Equals(testCommand, "GET", StringComparison.InvariantCultureIgnoreCase):
                    status = lockable ? lockableGarnetApi.GET(key.Item1, out var value) : basicGarnetApi.GET(key.Item1, out value);
                    if (status == GarnetStatus.OK)
                    {
                        // TODO: Why is this ToString() method using ASCII encoding?, should it be UTF-8?
                        return value.ToString();
                    }
                    break;
                case var testCommand when string.Equals(testCommand, "ZADD", StringComparison.InvariantCultureIgnoreCase):
                    return TryZADDForScript(key.Item1, lockable, args);
                default:
                    break;
            }
            return status == GarnetStatus.OK ? "OK" : "ERR";
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="args"></param>
        /// <param name="lockable"></param>
        /// <returns></returns>
        internal int TryZADDForScript(ArgSlice key, bool lockable, params object[] args)
        {
            int nOptions = 0; //zadd command options
            int count;

            (ArgSlice score, ArgSlice member)[] pairs;

            if (args == null || args.Length == 0)
            {
                return 0;
            }

            if (args.Length == 1)
            {
                var pairsObjects = args[0];

                // This casting covers the mapping from a ZADD executed inside a script with a LuaTable type from Lua code.
                var nValues = ((NLua.LuaTable)pairsObjects).Values.Count / 2;
                pairs = new (ArgSlice score, ArgSlice member)[nValues];
                int validPairs = 0;
                int indexValues = 1;

                ArgSlice scoreArgSlice = default;
                // Cast object values so these can be used as ArgSlice
                foreach (var pair in ((NLua.LuaTable)pairsObjects).Values)
                {
                    if (indexValues % 2 == 0)
                    {
                        if (TryGetArgSliceFromArg(pair, out ArgSlice memberArgSlice))
                        {
                            pairs[validPairs++] = (scoreArgSlice, memberArgSlice);
                        }
                    }
                    else
                    {
                        _ = TryGetArgSliceFromArg(pair, out scoreArgSlice);
                    }
                    indexValues++;
                }
            }
            else
            {
                // TODO: Read any ZADD options
                // Get number of actual pairs
                count = (args.Length - nOptions) / 2;

                pairs = new (ArgSlice score, ArgSlice member)[count];

                var i = nOptions;
                var indexPairs = 0;
                while (i < args.Length)
                {
                    // TODO: Support other types
                    Debug.Assert(args[i] != null);
                    Debug.Assert(args[i + 1] != null);
                    if (TryGetArgSliceFromArg(args[i], out var score) && TryGetArgSliceFromArg(args[i + 1], out var member))
                    {
                        pairs[indexPairs] = (score, member);
                        indexPairs++;
                        i += 2;
                    }
                }
                Debug.Assert(indexPairs == count);
            }

            // TODO: Is basic context api needed for this operation?
            var status = lockable ? lockableGarnetApi.SortedSetAdd(key, pairs, out int zaddCount) : basicGarnetApi.SortedSetAdd(key, pairs, out zaddCount);
            if (status != GarnetStatus.OK)
            {
                zaddCount = -1;
            }

            // All pairs were added
            return zaddCount;
        }

        #region Utilities Methods

        private bool TryGetArgSliceFromArg(object arg, out ArgSlice valueArg)
        {
            valueArg = default;
            try
            {
                // note this is using UTF-8 encoding
                valueArg = scratchBufferManager.CreateArgSlice(arg.ToString());
            }
            catch
            {
                return false;
            }
            return true;
        }
        #endregion
    }
}
