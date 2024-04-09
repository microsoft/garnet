// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    public abstract class RespSerializableBase
    {
        public string RespFormat
        {
            get => respFormat ??= ToRespFormat();
        }

        private string respFormat;

        protected RespSerializableBase(string respFormat)
        {
            FromRespFormat(respFormat);
        }

        protected RespSerializableBase()
        {
            
        }

        protected abstract void FromRespFormat(string respFormat);

        protected abstract string ToRespFormat();
    }
}
