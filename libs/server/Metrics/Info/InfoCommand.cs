// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Garnet.common;

namespace Garnet.server
{
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        private bool ProcessInfoCommand(int count)
        {
            HashSet<InfoMetricsType> sections = null;
            bool invalid = false;
            bool reset = false;
            bool help = false;
            string invalidSection = null;
            if (count > 1)
            {
                var ptr = recvBufferPtr + readHead;
                sections = new HashSet<InfoMetricsType>();
                for (int i = 0; i < count - 1; i++)
                {
                    if (!RespReadUtils.ReadStringWithLengthHeader(out var section, ref ptr, recvBufferPtr + bytesRead))
                        return false;

                    section = section.ToUpper();
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
                            try
                            {
                                var sectionType = (InfoMetricsType)Enum.Parse(typeof(InfoMetricsType), section);
                                sections.Add(sectionType);
                            }
                            catch
                            {
                                invalid = true;
                                invalidSection = section;
                            }
                            break;
                    }
                }
                readHead = (int)(ptr - recvBufferPtr);
            }

            if (invalid)
            {
                while (!RespWriteUtils.WriteResponse(new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR Invalid section {invalidSection}. Try INFO HELP\r\n")), ref dcurr, dend))
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
                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                InfoMetricsType[] sectionsArr = sections == null ? GarnetInfoMetrics.defaultInfo : sections.ToArray();
                GarnetInfoMetrics garnetInfo = new();
                string info = garnetInfo.GetRespInfo(sectionsArr, storeWrapper);
                while (!RespWriteUtils.WriteBulkString(Encoding.ASCII.GetBytes(info), ref dcurr, dend))
                    SendAndReset();
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
                while (!RespWriteUtils.WriteBulkString(Encoding.ASCII.GetBytes(sectionInfo), ref dcurr, dend))
                    SendAndReset();
            }
        }
    }
}