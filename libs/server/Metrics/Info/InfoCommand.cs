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
                WriteError($"ERR Invalid section {invalidSection}. Try INFO HELP");
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
                WriteOK();
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
                    WriteDirect(CmdStrings.RESP_EMPTY);
                }
            }
            return true;

        }

        private void GetHelpMessage()
        {
            List<string> sectionsHelp = InfoHelp.GetInfoTypeHelpMessage();

            WriteArrayLength(sectionsHelp.Count);
            foreach (var sectionInfo in sectionsHelp)
            {
                WriteAsciiBulkString(sectionInfo);
            }
        }
    }
}