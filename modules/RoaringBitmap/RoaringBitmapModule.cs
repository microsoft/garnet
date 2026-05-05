// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.server;
using Microsoft.Extensions.Logging;

namespace GarnetRoaringBitmap
{
    /// <summary>
    /// Garnet module that registers the Roaring Bitmap custom object type and the
    /// associated <c>R.*</c> RESP commands. Load via <c>MODULE LOADCS</c>.
    /// </summary>
    public class RoaringBitmapModule : ModuleBase
    {
        /// <inheritdoc/>
        public override void OnLoad(ModuleLoadContext context, string[] args)
        {
            var status = context.Initialize("GarnetRoaringBitmap", 1);
            if (status != ModuleActionStatus.Success)
            {
                context.Logger?.LogError("Failed to initialize GarnetRoaringBitmap. Error {status}", status);
                return;
            }

            var factory = new RoaringBitmapFactory();
            status = context.RegisterType(factory);
            if (status != ModuleActionStatus.Success)
            {
                context.Logger?.LogError("Failed to register RoaringBitmap object factory. Error {status}", status);
                return;
            }

            context.RegisterCommand("R.SETBIT", factory, new RSetBit(), CommandType.ReadModifyWrite);
            context.RegisterCommand("R.GETBIT", factory, new RGetBit(), CommandType.Read);
            context.RegisterCommand("R.BITCOUNT", factory, new RBitCount(), CommandType.Read);
            context.RegisterCommand("R.BITPOS", factory, new RBitPos(), CommandType.Read);
        }
    }
}