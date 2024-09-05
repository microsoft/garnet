// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Transactions;
using Azure.Messaging;

namespace Garnet.server
{
    [Serializable]
    public class LuaResultException: Exception
    {
        public LuaResultException() : base() { }
        public LuaResultException(String message) : base(message) { }            
        public LuaResultException(string message, Exception innerException) : base(message, innerException) { }
    }
}
