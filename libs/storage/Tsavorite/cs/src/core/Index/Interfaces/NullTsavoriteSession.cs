﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    struct NullTsavoriteSession : ITsavoriteSession
    {
        public static readonly NullTsavoriteSession Instance = new();

        public void UnsafeResumeThread()
        {
        }

        public void UnsafeSuspendThread()
        {
        }
    }
}