// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.server;

namespace Garnet
{
    /// <summary>
    /// Garnet server entry point
    /// </summary>
    public class Program
    {
        static async Task Main(string[] args)
        {
            GarnetServer server = null;
            var shutdownCts = new CancellationTokenSource();
            int shutdownInitiated = 0; // Guard to ensure single shutdown/dispose

            try
            {
                server = new GarnetServer(args);

                // Optional: register custom extensions
                RegisterExtensions(server);

                // Set up graceful shutdown handlers for Ctrl+C and SIGTERM
                Console.CancelKeyPress += (sender, e) =>
                {
                    e.Cancel = true; // Prevent immediate termination
                    Console.WriteLine("Shutdown signal received. Starting graceful shutdown...");
                    shutdownCts.Cancel();
                };

                AppDomain.CurrentDomain.ProcessExit += (sender, e) =>
                {
                    // Only initiate shutdown if not already done
                    if (Interlocked.Exchange(ref shutdownInitiated, 1) == 0)
                    {
                        Console.WriteLine("Process exit signal received. Starting graceful shutdown...");
                        shutdownCts.Cancel();
                        // Wait for graceful shutdown with timeout
                        server?.ShutdownAsync(TimeSpan.FromSeconds(3), CancellationToken.None)
                            .GetAwaiter().GetResult();
                        server?.Dispose();
                    }
                };

                // Start the server
                server.Start();

                // Wait for shutdown signal
                try
                {
                    await Task.Delay(Timeout.Infinite, shutdownCts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Normal shutdown path
                }

                // Only initiate shutdown if not already done by ProcessExit handler
                if (Interlocked.Exchange(ref shutdownInitiated, 1) == 0)
                {
                    // Block synchronously for shutdown - ensures cleanup completes before process exits
                    server.ShutdownAsync(TimeSpan.FromSeconds(5), CancellationToken.None)
                        .GetAwaiter().GetResult();
                    server?.Dispose();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Unable to initialize server due to exception: {ex.Message}");
                // Ensure cleanup on exception if shutdown wasn't initiated
                if (Interlocked.Exchange(ref shutdownInitiated, 1) == 0)
                {
                    server?.Dispose();
                }
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
            server.Register.NewCommand("SETIFPM", CommandType.ReadModifyWrite, new SetIfPMCustomCommand(), setIfPmCmdInfo);

            // Register custom command on raw strings (SETWPIFPGT = "set with prefix, if prefix greater than")
            server.Register.NewCommand("SETWPIFPGT", CommandType.ReadModifyWrite, new SetWPIFPGTCustomCommand());

            // Register custom command on raw strings (DELIFM = "delete if value matches")
            server.Register.NewCommand("DELIFM", CommandType.ReadModifyWrite, new DeleteIfMatchCustomCommand());

            // Register custom commands on objects
            var factory = new MyDictFactory();
            server.Register.NewType(factory);
            server.Register.NewCommand("MYDICTSET", CommandType.ReadModifyWrite, factory, new MyDictSet(), new RespCommandsInfo { Arity = 4 });
            server.Register.NewCommand("MYDICTGET", CommandType.Read, factory, new MyDictGet(), new RespCommandsInfo { Arity = 3 });

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
            server.Register.NewTransactionProc("READWRITETX", () => new ReadWriteTxn(), readWriteTxCmdInfo);

            // Register stored procedure to run a transactional command
            server.Register.NewTransactionProc("MSETPX", () => new MSetPxTxn());

            // Register stored procedure to run a transactional command
            server.Register.NewTransactionProc("MGETIFPM", () => new MGetIfPM());

            // Register stored procedure to run a non-transactional command
            server.Register.NewTransactionProc("GETTWOKEYSNOTXN", () => new GetTwoKeysNoTxn(), new RespCommandsInfo { Arity = 3 });

            // Register sample transactional procedures
            server.Register.NewTransactionProc("SAMPLEUPDATETX", () => new SampleUpdateTxn(), new RespCommandsInfo { Arity = 9 });
            server.Register.NewTransactionProc("SAMPLEDELETETX", () => new SampleDeleteTxn(), new RespCommandsInfo { Arity = 6 });

            server.Register.NewProcedure("SUM", () => new Sum());
            server.Register.NewProcedure("SETMAINANDOBJECT", () => new SetStringAndList());
        }
    }
}