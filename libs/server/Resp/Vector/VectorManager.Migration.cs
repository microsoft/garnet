// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.server
{
    /// <summary>
    /// Methods related to migrating Vector Sets between different primaries.
    /// 
    /// This is bespoke because normal migration is key based, but Vector Set migration has to move whole namespaces first.
    /// </summary>
    public sealed partial class VectorManager
    {
        // This is a V8 GUID based on 'GARNET MIGRATION' ASCII string
        // It cannot collide with processInstanceIds because it's v8
        // It's unlikely other projects will select the value, so it's unlikely to collide with other v8s
        // If it ends up in logs, it's ASCII equivalent looks suspicious enough to lead back here
        private static readonly Guid MigratedInstanceId = new("4e524147-5445-8d20-8947-524154494f4e");

        // TODO: Migration! Move that data over!
    }
}