using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace ETag
{
    /*
        This code sample shows how to use ETags to implement lock-free synchronization for non-atomic operations

        OCC is like using a CAS loop to make sure the data we are writing has not had a change in between the time
        we have read it and written back.

        Scenario 1: Lock free Json manipulation, we are using JSON as our value but this could essentially be
        any data that you falls in the below category that is not provided by the objects API of Garnet.

        Granular Data Structure: Refers to data that is divided into small, independent parts that can be manipulated individually. For example, MongoDB documents allow granular updates on individual fields.

        Mutable Object: If the object allows you to modify its individual components without recreating the entire object, it’s referred to as mutable. For example, Python dictionaries and lists are mutable.

        Partial Updatable Data: This term is used in contexts like databases where updates can target specific fields without affecting the entire record.

        Modular Data Structure: If the object is designed to have independent, self-contained modules (like classes or subcomponents), you might describe it as modular.

        Composable Data: This term applies when different parts of the data can be independently composed, used, or updated, often seen in functional programming.

        Hierarchical Data Structure: Refers to objects with nested components, like JSON or XML, where parts of the hierarchy can be accessed and modified independently

                Simulation Description:

                We have 2 different clients that are updating the same value for a key, but different parts of it concurrently
                We want to make sure we don't lose the updates between these 2 clients.

                Client 1: Updates the number of cats a user has
                Client 2: Changes the flag for the user for whether or not the user has too many cats.
                    Client 2 only considers that a user has too many cats when the number of cats is divisible by 5,
                    otherwise it marks the user as false for not having too many cats
    */
    class OccSimulation
    {
        public static async Task RunSimulation()
        {
            using var redis = await ConnectionMultiplexer.ConnectAsync(GarnetConnectionStr);
            var db = redis.GetDatabase(0);

            ContosoUserInfo userInfo = new ContosoUserInfo
            {
                FirstName = "Hamdaan",
                LastName = "Khalid",
                NumberOfCats = 1,
                TooManyCats = true,
                Basket = new List<string>()
            };

            string userKey = "hkhalid";
            string serializedUserInfo = JsonSerializer.Serialize(userInfo);

            // Seed the item in the database
            long initialEtag = (long)await db.ExecuteAsync("EXECWITHETAG", "SET", userKey, serializedUserInfo);

            // Cancellation token is used to exit program on end of interactive repl
            var cts = new CancellationTokenSource();
            // Clone user info so they are task local
            var client1Task = Task.Run(() => Client1(userKey, initialEtag, (ContosoUserInfo)userInfo.Clone(), cts.Token));
            var client2Task = Task.Run(() => Client2(userKey, initialEtag, (ContosoUserInfo)userInfo.Clone(), cts.Token));

            // Interactive REPL to change any property in the ContosoUserInfo
            while (true)
            {
                Console.WriteLine("Enter the property to change (FirstName, LastName, NumberOfCats, TooManyCats, AddToBasket, RemoveFromBasket) or 'exit' to quit:");
                Console.WriteLine($"Initial User Info: {JsonSerializer.Serialize(userInfo)}");
                string input = Console.ReadLine()!;

                if (input.ToLower() == "exit")
                {
                    cts.Cancel();
                    break;
                }

                Action<ContosoUserInfo> userUpdateAction = (userInfo) => { };
                switch (input)
                {
                    case "FirstName":
                        Console.WriteLine("Enter new FirstName:");
                        string newFirstName = Console.ReadLine()!;
                        userUpdateAction = (info) => info.FirstName = newFirstName;
                        break;
                    case "LastName":
                        Console.WriteLine("Enter new LastName:");
                        string newLastName = Console.ReadLine()!;
                        userUpdateAction = (info) => info.FirstName = newLastName;
                        break;
                    case "NumberOfCats":
                        Console.WriteLine("Enter new NumberOfCats:");
                        if (int.TryParse(Console.ReadLine(), out int numberOfCats))
                        {
                            userUpdateAction = (info) => info.NumberOfCats = numberOfCats;
                        }
                        else
                        {
                            Console.WriteLine("Invalid number.");
                        }
                        break;
                    case "TooManyCats":
                        Console.WriteLine("Enter new TooManyCats (true/false):");
                        if (bool.TryParse(Console.ReadLine(), out bool tooManyCats))
                        {
                            userUpdateAction = (info) => info.TooManyCats = tooManyCats;
                        }
                        else
                        {
                            Console.WriteLine("Invalid boolean.");
                        }
                        break;
                    case "AddToBasket":
                        Console.WriteLine("Enter item to add to basket:");
                        string addItem = Console.ReadLine()!;
                        userUpdateAction = (info) => info.Basket.Add(addItem);
                        break;
                    case "RemoveFromBasket":
                        Console.WriteLine("Enter item to remove from basket:");
                        string removeItem = Console.ReadLine()!;
                        userUpdateAction = (info) => info.Basket.Remove(removeItem);
                        break;
                    default:
                        Console.WriteLine("Unknown property.");
                        break;
                }

                // Update the user info in the database, and then for the REPL
                (initialEtag, userInfo) = await ETagAbstractions.PerformLockFreeSafeUpdate<ContosoUserInfo>(db, userKey, initialEtag, userInfo, userUpdateAction);
                Console.WriteLine($"Updated User Info: {JsonSerializer.Serialize(userInfo)}");
            }

            cts.Cancel();

            try
            {
                await Task.WhenAll(client1Task, client2Task);
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("All clients killed.");
            }
        }

        static async Task Client1(string userKey, long initialEtag, ContosoUserInfo initialUserInfo, CancellationToken token)
        {
            Random random = new Random();
            using var redis = await ConnectionMultiplexer.ConnectAsync(GarnetConnectionStr);
            var db = redis.GetDatabase(0);

            long etag = initialEtag;
            ContosoUserInfo userInfo = initialUserInfo;
            while (true)
            {
                token.ThrowIfCancellationRequested();
                (etag, userInfo) = await ETagAbstractions.PerformLockFreeSafeUpdate<ContosoUserInfo>(db, userKey, etag, userInfo, (ContosoUserInfo userInfo) =>
                {
                    userInfo.NumberOfCats++;
                });
                await Task.Delay(TimeSpan.FromSeconds(random.Next(0, 15)), token);
            }
        }

        static async Task Client2(string userKey, long initialEtag, ContosoUserInfo initialUserInfo, CancellationToken token)
        {
            Random random = new Random();
            using var redis = await ConnectionMultiplexer.ConnectAsync(GarnetConnectionStr);
            var db = redis.GetDatabase(0);

            long etag = initialEtag;
            ContosoUserInfo userInfo = initialUserInfo;
            while (true)
            {
                token.ThrowIfCancellationRequested();
                (etag, userInfo) = await ETagAbstractions.PerformLockFreeSafeUpdate<ContosoUserInfo>(db, userKey, etag, userInfo, (ContosoUserInfo userInfo) =>
                {
                    userInfo.TooManyCats = userInfo.NumberOfCats % 5 == 0;
                });
                await Task.Delay(TimeSpan.FromSeconds(random.Next(0, 15)), token);
            }
        }

        static string GarnetConnectionStr = "localhost:6379,connectTimeout=999999,syncTimeout=999999";
    }

    class ContosoUserInfo : ICloneable
    {
        [JsonPropertyName("first_name")]
        public required string FirstName { get; set; }

        [JsonPropertyName("last_name")]
        public required string LastName { get; set; }

        [JsonPropertyName("number_of_cats")]
        public required int NumberOfCats { get; set; }

        [JsonPropertyName("too_many_cats")]
        public required bool TooManyCats { get; set; }

        [JsonPropertyName("basket")]
        public required List<string> Basket { get; set; }

        public object Clone()
        {
            return new ContosoUserInfo
            {
                FirstName = this.FirstName,
                LastName = this.LastName,
                NumberOfCats = this.NumberOfCats,
                TooManyCats = this.TooManyCats,
                Basket = new List<string>(this.Basket)
            };
        }
    }
}