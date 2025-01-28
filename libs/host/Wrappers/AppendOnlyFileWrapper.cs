// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet;

internal class AppendOnlyFileWrapper : IDisposable
{
    readonly IDevice device;
    public readonly TsavoriteLog appendOnlyFile;

    public AppendOnlyFileWrapper(IDevice device, TsavoriteLog appendOnlyFile)
    {
        this.device = device;
        this.appendOnlyFile = appendOnlyFile;
    }

    public void Dispose()
    {
        device?.Dispose();
        appendOnlyFile?.Dispose();
    }
}