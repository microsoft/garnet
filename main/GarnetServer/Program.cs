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

            // Register Roaring Bitmap object type and its commands (issue #1270).
            // R.SETBIT key offset value     -> previous bit (0/1)
            // R.GETBIT key offset           -> current bit (0/1)
            // R.BITCOUNT key                -> population count (long)
            // R.BITPOS key bit [from]       -> first matching position, or -1 (long); arity is variadic so use -3
            var roaringFactory = new Garnet.Extensions.RoaringBitmap.RoaringBitmapFactory();
            server.Register.NewType(roaringFactory);
            server.Register.NewCommand("R.SETBIT", CommandType.ReadModifyWrite, roaringFactory, new Garnet.Extensions.RoaringBitmap.RSetBit(), new RespCommandsInfo { Arity = 4 });
            server.Register.NewCommand("R.GETBIT", CommandType.Read, roaringFactory, new Garnet.Extensions.RoaringBitmap.RGetBit(), new RespCommandsInfo { Arity = 3 });
            server.Register.NewCommand("R.BITCOUNT", CommandType.Read, roaringFactory, new Garnet.Extensions.RoaringBitmap.RBitCount(), new RespCommandsInfo { Arity = 2 });
            server.Register.NewCommand("R.BITPOS", CommandType.Read, roaringFactory, new Garnet.Extensions.RoaringBitmap.RBitPos(), new RespCommandsInfo { Arity = -3 });

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