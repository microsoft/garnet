// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Threading.Tasks;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// Server session for RESP protocol - basic commands are in this file
    /// </summary>
    internal sealed partial class RespServerSession : ServerSessionBase
    {
        void NetworkGETPending<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            unsafe
            {
                while (!RespWriteUtils.WriteError($"ASYNC {asyncStarted++}", ref dcurr, dend))
                    SendAndReset();
            }
            if (asyncStarted == 1)
            {
                asyncWaiter = new()
                {
                    RunContinuationsAsynchronously = true
                };
                var _storageApi = storageApi;
                _ = Task.Run(async () => await AsyncGetProcessor(_storageApi));
            }
            else
            {
                Debug.Assert(asyncWaiter != null);
                asyncWaiter.Signal();
            }
        }

        async Task AsyncGetProcessor<TGarnetApi>(TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            while (true)
            {
                while (asyncCompleted < asyncStarted)
                {
                    // First complete all pending ops
                    storageApi.GET_CompletePending(out var completedOutputs, true);

                    try
                    {
                        unsafe
                        {
                            networkSender.EnterAndGetResponseObject(out dcurr, out dend);

                            // Send async replies with completed outputs
                            while (completedOutputs.Next())
                            {
                                asyncCompleted++;
                                var o = completedOutputs.Current.Output;
                                RespWriteUtils.WriteArrayLength(3, ref dcurr, dend);
                                RespWriteUtils.WriteBulkString("async"u8, ref dcurr, dend);
                                RespWriteUtils.WriteIntegerAsBulkString((int)completedOutputs.Current.Context, ref dcurr, dend);
                                if (completedOutputs.Current.Status.Found)
                                {
                                    Debug.Assert(!o.IsSpanByte);
                                    sessionMetrics?.incr_total_found();
                                    SendAndReset(o.Memory, o.Length);
                                }
                                else
                                {
                                    sessionMetrics?.incr_total_notfound();
                                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                                        SendAndReset();
                                }
                            }
                            completedOutputs.Dispose();

                            if (dcurr > networkSender.GetResponseObjectHead())
                                Send(networkSender.GetResponseObjectHead());
                        }
                    }
                    finally
                    {
                        networkSender.ExitAndReturnResponseObject();
                    }
                }
                asyncDone?.Release();
                await asyncWaiter.WaitAsync();
            }
        }
    }
}