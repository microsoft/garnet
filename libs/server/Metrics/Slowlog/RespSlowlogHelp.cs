// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;

namespace Garnet.server
{
    /// <summary>
    /// Info on what slow log commands are supported
    /// </summary>
    static class RespSlowLogHelp
    {
        /// <summary>
        /// Get supported latency commands and a short description
        /// </summary>
        /// <returns></returns>
        public static List<string> GetSlowLogCommands()
        {
            return new List<string>()
            {
                "SLOWLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "GET [<count>]",
                "\tReturn top <count> entries from the slowlog (default: 10, -1 mean all).",
                "\tEntries are made of:",
                "\tid, timestamp, time in microseconds, arguments array, client IP and port,",
                "\tclient name",
                "LEN",
                "\tReturn the length of the slowlog.",
                "RESET",
                "\tReset the slowlog.",
                "HELP",
                "\tPrints this help"
            };
        }
    }
}