// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
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
                for (int i = 0; i < count; i++)
                {
                    var section = parseState.GetString(i).ToUpper();

                    switch (section)
                    {
                        case InfoHelp.RESET:
                            reset = true;
                            break;
                        case InfoHelp.HELP:
                            help = true;
                            break;
                        case InfoHelp.ALL:
                            break;
                        default:
                            if (Enum.TryParse(section, out InfoMetricsType sectionType))
                            {
                                sections.Add(sectionType);
                            }
                            else
                            {
                                invalid = true;
                                invalidSection = section;
                            }
                            break;
                    }
                }
            }

            if (invalid)
            {
                while (!RespWriteUtils.WriteError($"ERR Invalid section {invalidSection}. Try INFO HELP", ref dcurr, dend))
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
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                var sectionsArr = sections == null ? GarnetInfoMetrics.defaultInfo : [.. sections];
                var garnetInfo = new GarnetInfoMetrics();
                var info = garnetInfo.GetRespInfo(sectionsArr, storeWrapper);
                if (!string.IsNullOrEmpty(info))
                {
                    while (!RespWriteUtils.WriteAsciiBulkString(info, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                        SendAndReset();
                }
            }
            return true;

        }

        private void GetHelpMessage()
        {
            List<string> sectionsHelp = InfoHelp.GetInfoTypeHelpMessage();
            while (!RespWriteUtils.WriteArrayLength(sectionsHelp.Count, ref dcurr, dend))
                SendAndReset();
            foreach (var sectionInfo in sectionsHelp)
            {
                while (!RespWriteUtils.WriteAsciiBulkString(sectionInfo, ref dcurr, dend))
                    SendAndReset();
            }
        }
    }
}