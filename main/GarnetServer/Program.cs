// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Garnet.server;

namespace Garnet
{
    /// <summary>
    /// Garnet server entry point
    /// </summary>
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                using var server = new GarnetServer(args);

                // Optional: register custom extensions
                RegisterExtensions(server);

                // Start the server
                server.Start();

                Thread.Sleep(Timeout.Infinite);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Unable to initialize server due to exception: {ex.Message}");
            }
        }

        /// <summary>
        /// Register new commands with the server. You can access these commands from clients using
        /// commands such as db.Execute in StackExchange.Redis. Example:
        ///   db.Execute("SETIFPM", key, value, prefix);
        /// </summary>
        static void RegisterExtensions(GarnetServer server)
        {
            // Register custom command on raw strings (SETIFPM = "set if prefix match")
            // Add RESP command info to registration for command to appear when client runs COMMAND / COMMAND INFO
            var setIfPmCmdInfo = new RespCommandsInfo
            {
                Name = "SETIFPM",
                Arity = 4,
                FirstKey = 1,
                LastKey = 1,
                Step = 1,
                Flags = RespCommandFlags.DenyOom | RespCommandFlags.Write,
                AclCategories = RespAclCategories.String | RespAclCategories.Write,
            };
            server.Register.NewCommand("SETIFPM", 2, CommandType.ReadModifyWrite, new SetIfPMCustomCommand(), setIfPmCmdInfo);

            // Register custom command on raw strings (SETWPIFPGT = "set with prefix, if prefix greater than")
            server.Register.NewCommand("SETWPIFPGT", 2, CommandType.ReadModifyWrite, new SetWPIFPGTCustomCommand());

            // Register custom command on raw strings (DELIFM = "delete if value matches")
            server.Register.NewCommand("DELIFM", 1, CommandType.ReadModifyWrite, new DeleteIfMatchCustomCommand());

            // Register custom commands on objects
            var factory = new MyDictFactory();
            server.Register.NewCommand("MYDICTSET", 2, CommandType.ReadModifyWrite, factory);
            server.Register.NewCommand("MYDICTGET", 1, CommandType.Read, factory);

            // Register stored procedure to run a transactional command
            // Add RESP command info to registration for command to appear when client runs COMMAND / COMMAND INFO
            var readWriteTxCmdInfo = new RespCommandsInfo
            {
                Name = "READWRITETX",
                Arity = 4,
                FirstKey = 1,
                LastKey = 3,
                Step = 1,
                Flags = RespCommandFlags.DenyOom | RespCommandFlags.Write,
                AclCategories = RespAclCategories.Write,
            };
            server.Register.NewTransactionProc("READWRITETX", 3, () => new ReadWriteTxn(), readWriteTxCmdInfo);

            // Register stored procedure to run a transactional command
            server.Register.NewTransactionProc("MSETPX", () => new MSetPxTxn());

            // Register stored procedure to run a transactional command
            server.Register.NewTransactionProc("MGETIFPM", () => new MGetIfPM());

            // Register stored procedure to run a non-transactional command
            server.Register.NewTransactionProc("GETTWOKEYSNOTXN", 2, () => new GetTwoKeysNoTxn());

            // Register sample transactional procedures
            server.Register.NewTransactionProc("SAMPLEUPDATETX", 8, () => new SampleUpdateTxn());
            server.Register.NewTransactionProc("SAMPLEDELETETX", 5, () => new SampleDeleteTxn());
        }
    }
}