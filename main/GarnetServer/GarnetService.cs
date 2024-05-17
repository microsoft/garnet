// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Hosting;

namespace Garnet
{
    /// <summary>
    /// Garnet service entrypoint
    /// </summary>
    /// <param name="args">Server arguments</param>
    class GarnetService(string[] args) : BackgroundService
    {
        private static readonly string CustomRespCommandInfoJsonFileName = "CustomRespCommandsInfo.json";

        private readonly ManualResetEventSlim stopEvent = new();

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                stopEvent.Reset();
                using var server = new GarnetServer(args);

                // Optional: register custom extensions
                if (!TryRegisterExtensions(server))
                {
                    Console.WriteLine("Unable to register server extensions.");
                    return Task.CompletedTask;
                }

                // Start the server
                server.Start();
                stopEvent.Wait(stoppingToken);
            }
            catch (OperationCanceledException)
            {
                // Server exited.
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Unable to initialize server due to exception: {ex.Message}");
            }

            return Task.CompletedTask;
        }

        /// <summary>
        /// Register new commands with the server. You can access these commands from clients using
        /// commands such as db.Execute in StackExchange.Redis. Example:
        ///   db.Execute("SETIFPM", key, value, prefix);
        /// </summary>
        static bool TryRegisterExtensions(GarnetServer server)
        {
            var binPath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);

            if (!TryGetRespCommandsInfo(Path.Combine(binPath!, CustomRespCommandInfoJsonFileName), out var customCommandsInfo))
                return false;

            // Register custom command on raw strings (SETIFPM = "set if prefix match")
            server.Register.NewCommand("SETIFPM", 2, CommandType.ReadModifyWrite, new SetIfPMCustomCommand(), customCommandsInfo["SETIFPM"]);

            // Register custom command on raw strings (SETWPIFPGT = "set with prefix, if prefix greater than")
            server.Register.NewCommand("SETWPIFPGT", 2, CommandType.ReadModifyWrite, new SetWPIFPGTCustomCommand(), customCommandsInfo["SETWPIFPGT"]);

            // Register custom command on raw strings (DELIFM = "delete if value matches")
            server.Register.NewCommand("DELIFM", 1, CommandType.ReadModifyWrite, new DeleteIfMatchCustomCommand(), customCommandsInfo["DELIFM"]);

            // Register custom commands on objects
            var factory = new MyDictFactory();
            server.Register.NewCommand("MYDICTSET", 2, CommandType.ReadModifyWrite, factory, customCommandsInfo["MYDICTSET"]);
            server.Register.NewCommand("MYDICTGET", 1, CommandType.Read, factory, customCommandsInfo["MYDICTGET"]);

            // Register stored procedure to run a transactional command
            server.Register.NewTransactionProc("READWRITETX", 3, () => new ReadWriteTxn());

            // Register stored procedure to run a transactional command
            server.Register.NewTransactionProc("MSETPX", () => new MSetPxTxn());

            // Register stored procedure to run a transactional command
            server.Register.NewTransactionProc("MGETIFPM", () => new MGetIfPM());

            // Register stored procedure to run a non-transactional command
            server.Register.NewTransactionProc("GETTWOKEYSNOTXN", 2, () => new GetTwoKeysNoTxn());

            // Register sample transactional procedures
            server.Register.NewTransactionProc("SAMPLEUPDATETX", 8, () => new SampleUpdateTxn());
            server.Register.NewTransactionProc("SAMPLEDELETETX", 5, () => new SampleDeleteTxn());

            return true;
        }

        private static bool TryGetRespCommandsInfo(string path, out IReadOnlyDictionary<string, RespCommandsInfo> commandsInfo)
        {
            var streamProvider = StreamProviderFactory.GetStreamProvider(FileLocationType.Local);
            var commandsInfoProvider = RespCommandsInfoProviderFactory.GetRespCommandsInfoProvider();
            return commandsInfoProvider.TryImportRespCommandsInfo(path, streamProvider, out commandsInfo);
        }
    }
}