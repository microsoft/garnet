// Reverting content to its previous state.
﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using static Tsavorite.core.Utility;

namespace Tsavorite.core
{
    /// <summary>
    /// Fixed 8-byte header describing the data layout of the record. Atomic assignment is guaranteed on...
