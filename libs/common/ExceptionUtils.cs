// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Garnet.common
{
    public static class ExceptionUtils
    {
        [MethodImpl(MethodImplOptions.NoInlining)]
        public static void ThrowException(Exception e) => throw e;
    }
}