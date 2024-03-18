// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.server
{
    /// <summary>
    /// Command registration API
    /// </summary>
    public class RegisterApi
    {
        readonly GarnetProvider provider;

        /// <summary>
        /// Construct new Register API instance
        /// </summary>
        public RegisterApi(GarnetProvider provider)
        {
            this.provider = provider;
        }

        /// <summary>
        /// Register custom command with Garnet
        /// </summary>
        /// <param name="name">Name of command</param>
        /// <param name="numParams">Numer of parameters (excluding the key, which is always the first parameter)</param>
        /// <param name="type">Type of command (e.g., read)</param>
        /// <param name="customFunctions">Custom functions for command logic</param>
        /// <param name="expirationTicks">
        /// Expiration for value, in ticks.
        /// -1 => remove existing expiration metadata;
        ///  0 => retain whatever it is currently (or no expiration if this is a new entry) - this is the default;
        /// >0 => set expiration to given value.
        /// </param>
        /// <returns>ID of the registered command</returns>
        public int NewCommand(string name, int numParams, CommandType type, CustomRawStringFunctions customFunctions, long expirationTicks = 0)
            => provider.StoreWrapper.customCommandManager.Register(name, numParams, type, customFunctions, expirationTicks);

        /// <summary>
        /// Register transaction procedure with Garnet
        /// </summary>
        /// <param name="name"></param>
        /// <param name="numParams"></param>
        /// <param name="proc"></param>
        /// <returns></returns>
        public int NewTransactionProc(string name, int numParams, Func<CustomTransactionProcedure> proc)
            => provider.StoreWrapper.customCommandManager.Register(name, numParams, proc);

        /// <summary>
        /// Register object type with server
        /// </summary>
        /// <param name="factory">Factory for object type</param>
        /// <returns></returns>
        public int NewType(CustomObjectFactory factory)
            => provider.StoreWrapper.customCommandManager.RegisterType(factory);

        /// <summary>
        /// Register object type with server, with specific type ID [0-55]
        /// </summary>
        /// <param name="type">Type ID for factory</param>
        /// <param name="factory">Factory for object type</param>
        public void NewType(int type, CustomObjectFactory factory)
            => provider.StoreWrapper.customCommandManager.RegisterType(type, factory);

        /// <summary>
        /// Register custom command with Garnet
        /// </summary>
        /// <param name="name">Name of command</param>
        /// <param name="numParams">Numer of parameters (excluding the key, which is always the first parameter)</param>
        /// <param name="commandType">Type of command (e.g., read)</param>
        /// <param name="type">Type ID for factory, registered using RegisterType</param>
        /// <returns>ID of the registered command</returns>
        public int NewCommand(string name, int numParams, CommandType commandType, int type)
            => provider.StoreWrapper.customCommandManager.Register(name, numParams, commandType, type);

        /// <summary>
        /// Register custom command with Garnet
        /// </summary>
        /// <param name="name">Name of command</param>
        /// <param name="numParams">Numer of parameters (excluding the key, which is always the first parameter)</param>
        /// <param name="commandType">Type of command (e.g., read)</param>
        /// <param name="factory">Custom factory for object</param>
        /// <returns>ID of the registered command</returns>
        public (int, int) NewCommand(string name, int numParams, CommandType commandType, CustomObjectFactory factory)
            => provider.StoreWrapper.customCommandManager.Register(name, numParams, commandType, factory);

    }
}