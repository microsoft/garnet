﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Provides thread management and callback to checkpoint completion (called state machine).
    /// </summary>
    /// <remarks>This is broken out into a non-generic base interface to allow the use of <see cref="NullSession"/> 
    /// in <see cref="TsavoriteKV{Key, Value}.ThreadStateMachineStep"/>.</remarks>
    internal interface ISessionEpochControl
    {
        void UnsafeResumeThread();
        void UnsafeSuspendThread();
    }
}