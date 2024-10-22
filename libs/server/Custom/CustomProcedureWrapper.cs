// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// Base class for custom command
    /// </summary>
    public abstract class CustomProcedure : CustomFunctions
    {
        internal ScratchBufferManager scratchBufferManager;

        //unsafe object CustomOperation<TGarnetApi>(TGarnetApi api, string cmd, params object[] args)
        //    where TGarnetApi : IGarnetApi
        //unsafe object CustomOperation<TGarnetApi>(TGarnetApi api, string cmd, params object[] args)
        //    where TGarnetApi : IGarnetApi
        //{
        //switch (cmd)
        //{
        //    // We special-case a few performance-sensitive operations to directly invoke via the storage API
        //    case "SET" when args.Length == 2:
        //    case "set" when args.Length == 2:
        //        {
        //            if (!respServerSession.CheckACLPermissions(RespCommand.SET))
        //                return Encoding.ASCII.GetString(CmdStrings.RESP_ERR_NOAUTH);
        //            var key = scratchBufferManager.CreateArgSlice(Convert.ToString(args[0]));
        //            var value = scratchBufferManager.CreateArgSlice(Convert.ToString(args[1]));
        //            _ = api.SET(key, value);
        //            return "OK";
        //        }
        //    case "GET":
        //    case "get":
        //        {
        //            if (!respServerSession.CheckACLPermissions(RespCommand.GET))
        //                throw new Exception(Encoding.ASCII.GetString(CmdStrings.RESP_ERR_NOAUTH));
        //            var key = scratchBufferManager.CreateArgSlice(Convert.ToString(args[0]));
        //            var status = api.GET(key, out var value);
        //            if (status == GarnetStatus.OK)
        //                return value.ToString();
        //            return null;
        //        }
        //    // As fallback, we use RespServerSession with a RESP-formatted input. This could be optimized
        //    // in future to provide parse state directly.
        //    default:
        //        {
        //var request = scratchBufferManager.FormatCommandAsResp(cmd, args, null);
        //_ = respServerSession.TryConsumeMessages(request.ptr, request.length);
        //var response = scratchBufferNetworkSender.GetResponse();
        //var result = ProcessResponse(response.ptr, response.length);
        //scratchBufferNetworkSender.Reset();
        //return result;
        //        }
        //}
        //}


        /// <summary>
        /// Custom command implementation
        /// </summary>
        /// <param name="garnetApi"></param>
        /// <param name="procInput"></param>
        /// <param name="output"></param>
        public abstract bool Execute<TGarnetApi>(TGarnetApi garnetApi, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
            where TGarnetApi : IGarnetApi;
    }

    class CustomProcedureWrapper
    {
        public readonly string NameStr;
        public readonly byte[] Name;
        public readonly byte Id;
        public readonly Func<CustomProcedure> CustomProcedure;

        internal CustomProcedureWrapper(string name, byte id, Func<CustomProcedure> customProcedure, CustomCommandManager customCommandManager)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentNullException(nameof(name));

            if (customProcedure == null)
                throw new ArgumentNullException(nameof(customProcedure));

            Debug.Assert(customCommandManager != null);

            NameStr = name.ToUpperInvariant();
            Name = System.Text.Encoding.ASCII.GetBytes(NameStr);
            Id = id;
            CustomProcedure = customProcedure;
        }
    }
}