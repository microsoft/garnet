// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Text;
using Garnet.common;

namespace Garnet.server
{
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        private bool NetworkINFO()
        {
            var count = parseState.Count;
            HashSet<InfoMetricsType> sections = null;
            bool invalid = false;
            bool reset = false;
            bool help = false;
            string invalidSection = null;
            if (count > 0)
            {
                sections = new HashSet<InfoMetricsType>();
                for (var i = 0; i < count; i++)
                {
                    var sbSection = parseState.GetArgSliceByRef(i).ReadOnlySpan;

                    if (sbSection.EqualsUpperCaseSpanIgnoringCase("RESET"u8))
                        reset = true;
                    else if (sbSection.EqualsUpperCaseSpanIgnoringCase("HELP"u8))
                        help = true;
                    else if (!sbSection.EqualsUpperCaseSpanIgnoringCase("ALL"u8))
                    {
                        if (parseState.TryGetInfoMetricsType(i, out var sectionType))
                        {
                            sections.Add(sectionType);
                        }
                        else
                        {
                            invalid = true;
                            invalidSection = parseState.GetString(i);
                        }
                    }
                }
            }

            if (invalid)
            {
                while (!RespWriteUtils.TryWriteError($"ERR Invalid section {invalidSection}. Try INFO HELP", ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (help)
            {
                GetHelpMessage();
            }
            else if (reset)
            {
                if (storeWrapper.monitor != null)
                    storeWrapper.monitor.resetEventFlags[InfoMetricsType.STATS] = true;
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                var sectionsArr = sections == null ? GarnetInfoMetrics.DefaultInfo : [.. sections];
                var garnetInfo = new GarnetInfoMetrics();
                var info = garnetInfo.GetRespInfo(sectionsArr, activeDbId, storeWrapper);
                if (!string.IsNullOrEmpty(info))
                {
                    WriteVerbatimString(Encoding.ASCII.GetBytes(info));
                }
                else
                {
                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_EMPTY, ref dcurr, dend))
                        SendAndReset();
                }
            }
            return true;
        }

        private void GetHelpMessage()
        {
            List<string> sectionsHelp = InfoHelp.GetInfoTypeHelpMessage();
            while (!RespWriteUtils.TryWriteArrayLength(sectionsHelp.Count, ref dcurr, dend))
                SendAndReset();
            foreach (var sectionInfo in sectionsHelp)
            {
                while (!RespWriteUtils.TryWriteAsciiBulkString(sectionInfo, ref dcurr, dend))
                    SendAndReset();
            }
        }
    }
}