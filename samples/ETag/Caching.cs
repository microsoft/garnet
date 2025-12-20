using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace ETag;

public class Caching
{
    /*
    The whole idea of using ETag based commands for caching purposes is to reduce network utilization by only sending and recieving
    what is needed over the network.

    Scenario:
        We are in an application, cache, and database setup. 
        In the read path the application always attempts to read from the cache and based on a hit or a miss it reaches into the database.
        In the write path the application may use "write-through" or "write-back" to internally update the cache. The write path will be simulated in it's own thread.
        The read path will be interacted with via a REPL.

        Anytime the client stores a local  "copy" of data that may exist on the cache, it will first make a call to the Cache and based on a hit or miss it will reach into the database.
        Everything till now describes your commonly used caching use-case.

        ETags help further speed up your Cache-Hit use case. When the client uses the Garnet GETIFNOTMATCH command they send their current ETag and only incur the extra network bandwidth
        of recieving the entire payload when their local version is different from what is there on the server. With large payloads this can help reduce latency in your cache-hit route.
    */

    public static async Task RunSimulation()
    {
        // LocalApplicationState represents the data that you keep within your Client's reach
        Dictionary<int, (long, MovieReview)> localApplicationState = new Dictionary<int, (long, MovieReview)>();

        Console.WriteLine("Seeding server and local state...");
        await SeedCache(localApplicationState);

        Console.WriteLine("Booting up fake server threads...");
        // run fake server threads in the background that invalidates entries in the cache and changes things either in write-through or write-back manner
        CancellationTokenSource cts = new CancellationTokenSource();
        Task srvThread1 = Task.Run(() => FakeServerThread(cts.Token), cts.Token);
        Task srvThread2 = Task.Run(() => FakeServerThread(cts.Token), cts.Token);
        Task srvThread3 = Task.Run(() => FakeServerThread(cts.Token), cts.Token);

        // Run interactive repl (application)
        await InteractiveRepl(localApplicationState);

        cts.Cancel();
        try
        {
            await Task.WhenAll(srvThread1, srvThread2, srvThread3);
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Server threads killed.");
        }
    }

    static async Task InteractiveRepl(Dictionary<int, (long, MovieReview)> localApplicationState)
    {
        using var redis = await ConnectionMultiplexer.ConnectAsync(GarnetConnectionStr);
        var db = redis.GetDatabase(0);

        while (true)
        {
            Console.WriteLine("Enter a review ID (0-19) to fetch or type 'exit' to quit:");
            string input = Console.ReadLine()!;

            if (input.ToLower() == "exit")
            {
                break;
            }

            if (int.TryParse(input, out int reviewId) && reviewId >= 0 && reviewId <= 19)
            {
                var (existingEtag, existingItem) = localApplicationState[reviewId];
                var (etag, movieReview) = await ETagAbstractions.GetIfNotMatch<MovieReview>(db, reviewId.ToString(), existingEtag, existingItem);

                if (movieReview != null)
                {
                    // update local application state/in-memory cache
                    localApplicationState[reviewId] = (etag, movieReview);
                    Console.WriteLine($"Movie Name: {movieReview.MovieName}");
                    Console.WriteLine($"Reviewer Name: {movieReview.ReviewerName}");
                    Console.WriteLine($"Rating: {movieReview.Rating}");
                    Console.WriteLine($"Review: {movieReview.Review.Substring(0, 50)}...");
                }
                else
                {
                    Console.WriteLine("Review not found.");
                }
            }
            else
            {
                Console.WriteLine("Invalid input. Please enter a number between 0 and 19.");
            }
        }
    }

    static async Task SeedCache(Dictionary<int, (long, MovieReview)> localApplicationState)
    {
        Random random = new Random();
        using var redis = await ConnectionMultiplexer.ConnectAsync(GarnetConnectionStr);
        var db = redis.GetDatabase(0);
        // Add a bunch of things with sufficiently large payloads into your cache, the maximum size of your values depends on your pagesize config on Garnet
        for (int i = 0; i < 20; i++)
        {
            string key = i.ToString();
            MovieReview movieReview = MovieReview.CreateRandomReview(random);
            string value = JsonSerializer.Serialize(movieReview);
            long etag = (long)await db.ExecuteAsync("EXECWITHETAG", "SET", key, value);
            localApplicationState.Add(i, (etag, movieReview));
            Console.WriteLine($"Seeded {i}");
        }
    }

    static async Task FakeServerThread(CancellationToken token)
    {
        Random random = new Random();
        using var redis = await ConnectionMultiplexer.ConnectAsync(GarnetConnectionStr);
        var db = redis.GetDatabase(0);

        // Run a loop where you are updating the items every now and then
        while (true)
        {
            token.ThrowIfCancellationRequested();
            // choose a random number [0 - 19] aka review ID in our database
            // change the review and rating for it
            string serverToMessWith = random.Next(19).ToString();
            var (etag, movieReview) = await ETagAbstractions.GetWithEtag<MovieReview>(db, serverToMessWith);
            await ETagAbstractions.PerformLockFreeSafeUpdate<MovieReview>(db, serverToMessWith, etag, movieReview!,
            (movieReview) =>
            {
                // the application server decides to reduce or increase the movie review rating
                movieReview.Review += random.Next(-2, 2);
            });

            // sleep anywhere from 10-60 seconds
            await Task.Delay(TimeSpan.FromSeconds(random.Next(10, 60)));
        }
    }

    static string GarnetConnectionStr = "localhost:6379,connectTimeout=999999,syncTimeout=999999";
}

class MovieReview
{
    [JsonPropertyName("movie_name")]
    public required string MovieName { get; set; }

    [JsonPropertyName("reviewer_name")]
    public required string ReviewerName { get; set; }

    [JsonPropertyName("rating")]
    public required int Rating { get; set; }

    [JsonPropertyName("review")]
    public required string Review { get; set; }

    public static MovieReview CreateRandomReview(Random random)
    {
        var movieName = $"{CommonWords[random.Next(CommonWords.Length)]} {CommonWords[random.Next(CommonWords.Length)]}";
        var reviewerName = $"{CommonWords[random.Next(CommonWords.Length)]} {CommonWords[random.Next(CommonWords.Length)]}";
        var rating = random.Next(0, 101);
        var review = GenerateLargeLoremIpsumText(1 * 1024 * 1024); // 1MB of text

        return new MovieReview
        {
            MovieName = movieName,
            ReviewerName = reviewerName,
            Rating = rating,
            Review = review
        };
    }

    private static string GenerateLargeLoremIpsumText(int sizeInBytes)
    {
        const string loremIpsum = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. ";
        var stringBuilder = new StringBuilder();

        while (Encoding.UTF8.GetByteCount(stringBuilder.ToString()) < sizeInBytes)
        {
            stringBuilder.Append(loremIpsum);
        }

        return stringBuilder.ToString();
    }

    private static readonly string[] CommonWords = ["The", "Amazing", "Incredible", "Fantastic", "Journey", "Adventure", "Mystery", "Legend", "Quest", "Saga", "John", "Jane", "Smith", "Doe", "Alice", "Bob", "Charlie", "David", "Eve", "Frank"];
}