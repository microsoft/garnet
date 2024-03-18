// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;

namespace Garnet.server
{
    /// <summary>
    /// Info on what latency commands are supported
    /// </summary>
    static class RespLatencyHelp
    {
        /// <summary>
        /// Get supported latency commands and a short description
        /// </summary>
        /// <returns></returns>
        public static List<string> GetLatencyCommands()
        {
            return new List<string>()
            {
                "LATENCY<subcommand>[< arg > [value][opt]...]. Subcommands are:",
                "HISTOGRAM [EVENT [EVENT...]]",
                "\t Return latency histogram of or more <event> classes.",
                "\tIf no commands are specified then all histograms are replied",
                "RESET [EVENT [EVENT...]]" +
                "\tReset latency data of one or more <event> classes." +
                "\t(default: reset all data for all event classes).",
                "HELP",
                "\tPrints this help"
            };
        }
    }
}