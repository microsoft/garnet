// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;


namespace Garnet.server
{
    class LuaSimpleString
    {
        private string contents;

        public LuaSimpleString(string contents)
        {
            this.contents = contents;
        }

        public string getContents()
        {
            return contents;
        }
    }
}