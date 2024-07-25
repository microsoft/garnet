// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Bridge class between LUA and C#
    /// </summary>
    public class ScriptApi
    {
        internal RespServerSession session { get; set; }
        readonly bool _lockableContext;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="s">the server session</param>
        /// <param name="lockableContext">The type of context to be used</param>
        internal ScriptApi(RespServerSession s, bool lockableContext)
        {
            session = s;
            _lockableContext = lockableContext;
        }

        /// <summary>
        /// Entry point for garnet.call method calls from LUA scripts
        /// </summary>
        /// <param name="cmd">Command to execute</param>
        /// <param name="key"></param>
        /// <param name="args">Command parameters</param>
        /// <returns></returns>
        public object call(string cmd, (ArgSlice, bool) key, params object[] args) => session.ProcessCommandFromScripting(cmd, _lockableContext, key, args);
    }
}