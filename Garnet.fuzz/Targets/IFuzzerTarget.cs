// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.fuzz.Targets
{
    /// <summary>
    /// Common interface for all fuzzer targets.
    /// </summary>
    internal interface IFuzzerTarget
    {
        /// <summary>
        /// Fuzzer entry point.
        /// 
        /// Crashes, exceptions, etc. should be allowed to bubble out.
        /// </summary>
        static abstract void Fuzz(ReadOnlySpan<byte> input);
    }
}
