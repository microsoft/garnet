// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Globalization;
using System.Linq;
using System.Text;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    [TestFixture]
    public class RespSortedSetGeoTests
    {
        GarnetServer server;

        readonly string[,] cities = new string[,] { {"-74.0059413", "40.7127837", "New York"},
                {"-118.2436849", "34.0522342", "Los Angeles"},
                {"-87.6297982", "41.8781136","Chicago"},
                {"-95.3698028", "29.7604267", "Houston"},
                {"-75.1652215 ", "39.9525839", "Philadelphia"},
                {"-112.0740373 ", "33.4483771", "Phoenix" },
                {"-98.49362819999999 ", "29.4241219", "San Antonio"},
                {"-117.1610838 ", "32.715738", "San Diego" },
                {"-96.79698789999999 ", "32.7766642", "Dallas"},
                {"-121.8863286 ", "37.3382082", "San Jose"},
                {"-97.7430608 ", "30.267153", "Austin" },
                {"-86.158068 ", "39.768403", "Indianapolis"},
                {"-81.65565099999999 ", "30.3321838", "Jacksonville" },
                {"-122.4194155 ", "37.7749295", "San Francisco"},
                {"-82.99879419999999 ", "39.9611755", "Columbus" },
                {"-80.8431267 ", "35.2270869", "Charlotte" },
                {"-97.3307658 ", "32.7554883", "Fort Worth" },
                {"-83.0457538 ", "42.331427", "Detroit" },
                {"-106.4424559 ", "31.7775757", "El Paso" },
                {"-90.0489801 ", "35.1495343", "Memphis" },
                {"-122.3320708 ", "47.6062095", "Seattle" },
                {"-104.990251 ", "39.7392358", "Denver" },
                {"-77.0368707 ", "38.9071923", "Washington"}
                };


        readonly string[,] worldcities = new string[,] {
                {"5.30310439999999" ,"51.6889350991489" ,"Hertogenbosch"},
                {"31.3647679" ,"29.832041799755" ,"15th of May City"},
                {"30.9409205" ,"29.9723457997422" ,"6th of October"},
                {"-8.3958768" ,"43.3712091005418" ,"A Coruña"},
                {"-13.5369586999999" ,"9.690232799424" ,"ANSOUMANIYA PLATEAU"},
                {"-13.5414648" ,"9.71045299942484" ,"ANSOUMANYAH VILLAGE"},
                {"6.08386199999999" ,"50.7763509992898" ,"Aachen"},
                {"9.92152629999999" ,"57.0462626002815" ,"Aalborg"},
                {"10.2134045999999" ,"56.1496277998528" ,"Aarhus"},
                {"-52.8512975999999" ,"68.7095879972654" ,"Aasiaat"},
                {"7.364349" ,"5.11273499949875" ,"Aba"},
                {"48.2591475" ,"30.3636096997085" ,"Abadan"},
                {"-48.8788429999999" ,"-1.72182799983967" ,"Abaetetuba"},
                {"114.972247299999" ,"44.018620200532" ,"Abag Banner"},
                {"8.1133202" ,"6.32088969942957" ,"Abakaliki"},
                {"91.4403553" ,"53.7206496991441" ,"Abakan"},
                {"91.4390755124224" ,"53.6940294823123" ,"Abakan"},
                {"-72.8788743999999" ,"-13.637348200271" ,"Abancay"},
                {"144.2732035" ,"44.0206027005319" ,"Abashiri"},
                {"-122.329479" ,"49.0521161996794" ,"Abbotsford"},
                {"73.2139122" ,"34.1436588996066" ,"Abbottabad"},
                {"21.3044437" ,"12.8081571996591" ,"Abdi"},
                {"-3.49684309999999" ,"6.7269041994137" ,"Abengourou"},
                {"-98.487813" ,"45.4649805004172" ,"Aberdeen"},
                {"-4.016107" ,"5.32035699948468" ,"Abidjan"},
                {"140.0280653" ,"35.8639989997222" ,"Abiko"},
                {"-99.7475904999999" ,"32.446449999595" ,"Abilene"},
                {"-4.029007" ,"5.43548699947724" ,"Abobo"},
                {"74.1956596999999" ,"30.1450542997269" ,"Abohar"},
                {"1.993632" ,"7.18200119940068" ,"Abomey"},
                {"2.354245" ,"6.45386369942394" ,"Abomey-Calavi"},
                {"13.1738695" ,"3.98962929958837" ,"Abong-Mbang"},
                {"19.2776143999999" ,"11.450623499533" ,"Aboudéïa"},
                {"-8.19736179999999" ,"39.4631905001925" ,"Abrantes"},
                {"-34.898389" ,"-7.90071899975481" ,"Abreu e Lima"},
                {"30.3406751" ,"20.7930384003999" ,"Abri"},
                {"33.8087818" ,"15.8990206000128" ,"Abu Delelq"},
                {"44.343966" ,"32.5271069995933" ,"Abu Gharaq"},
                {"33.3248470999999" ,"19.5378928003486" ,"Abu Hamad"},
                {"31.2339837999999" ,"11.4646720995342" ,"Abu Jibeha"},
                {"31.6708449" ,"30.7251699996804" ,"Abu Kabir"},
                {"40.9170588" ,"34.4505111996198" ,"Abu Kamal"},
                {"31.6156401296687" ,"22.3567809439398" ,"Abu Simbel City"},
                {"33.104062" ,"29.051162999832" ,"Abu Zenima"},
                {"7.48929739999999" ,"9.064330499403" ,"Abuja"},
                {"20.8283652" ,"13.8280294997704" ,"Abéché"},
                {"-89.8297600999999" ,"13.5897966997435" ,"Acajutla"},
                {"-99.8940181999999" ,"16.8680495001213" ,"Acapulco"},
                {"-69.2032822999999" ,"9.55079609941846" ,"Acarigua"},
                {"-0.201237599999999" ,"5.55710959946969" ,"Accra"},
                {"-68.2203481" ,"7.78882479939142" ,"Achaguas"},
                {"77.5086427999999" ,"21.254670000408" ,"Achalpur"},
                {"126.9694447" ,"45.5362499004082" ,"Acheng"},
                {"90.4953964" ,"56.2694845999064" ,"Achinsk"},
                {"-68.1710147" ,"-16.5680995004585" ,"Achocalla"},
                {"35.0839770628131" ,"32.9240047205798" ,"Acre"},
                {"26.1310939" ,"11.4611730995339" ,"Ad Da'ein"},
                {"33.4133374" ,"12.8661159996651" ,"Ad Dali"},
                {"44.9236266" ,"31.9853303996091" ,"Ad Diwaniyah"},
                {"34.3656297999999" ,"11.8073635995631" ,"Ad-Damazin"},
                {"139.795319" ,"35.7837029997147" ,"Adachi"},
                {"39.2705461" ,"8.54102609939314" ,"Adama"},
                {"35.3252861" ,"36.9863598998466" ,"Adana"},
                {"38.7612524999999" ,"9.01079339940166" ,"Addis Ababa"},
                {"21.8944538" ,"12.6590872996439" ,"Addé"},
                {"138.5999312" ,"-34.9281804998736" ,"Adelaide"},
                {"78.5339894999999" ,"19.6759452003563" ,"Adilabad"},
                {"38.2768591999999" ,"37.763953199949" ,"Adiyaman"},
                {"-4.0222008" ,"5.35312109948253" ,"Adjamé"},
                {"5.22274" ,"7.62324819939301" ,"Ado Ekiti"},
                {"77.2730893999999" ,"15.6253312999809" ,"Adoni"},
                {"22.1977187" ,"13.4664562997297" ,"Adré"},
                {"45.1190378999999" ,"2.14602869977114" ,"Afgooye"},
                {"2.10028999999999" ,"34.1125199996055" ,"Aflou"},
                {"-0.997583755299874" ,"6.37343724316886" ,"Afosu"},
                {"36.8700889999999" ,"36.5083794997896" ,"Afrin"},
                     };



        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, lowMemory: true);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        #region SE Tests

        [Test]
        public void CanUseGeoAdd()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var entries = new GeoEntry[cities.GetLength(0)];
            for (int j = 0; j < cities.GetLength(0); j++)
            {
                entries[j] = new GeoEntry(
                    double.Parse(cities[j, 0], CultureInfo.InvariantCulture),
                    double.Parse(cities[j, 1], CultureInfo.InvariantCulture),
                    new RedisValue(cities[j, 2]));
            }
            var response = db.GeoAdd(new RedisKey("cities"), entries, CommandFlags.None);
            ClassicAssert.AreEqual(23, response);

            var memresponse = db.Execute("MEMORY", "USAGE", "cities");
            var actualValue = ResultType.Integer == memresponse.Resp2Type ? int.Parse(memresponse.ToString()) : -1;
            var expectedResponse = 3944;
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }


        [Test]
        public void CanUseGeoAddWhenLTM()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var entries = new GeoEntry[worldcities.GetLength(0)];
            for (int j = 0; j < worldcities.GetLength(0); j++)
            {
                entries[j] = new GeoEntry(
                    double.Parse(worldcities[j, 0], CultureInfo.InvariantCulture),
                    double.Parse(worldcities[j, 1], CultureInfo.InvariantCulture),
                    new RedisValue($"{worldcities[j, 2]}"));
            }

            //Number of objects that will trigger pending status in object store
            for (int j = 0; j < 1300; j++)
            {
                var response = db.GeoAdd(new RedisKey($"worldcities-{j}"), entries, CommandFlags.None);
            }

            var nkeys = db.Execute("DBSIZE");

            ClassicAssert.IsTrue(((RedisValue)nkeys) == 1300);
        }


        [Test]
        public void CanUseGeoPos()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            db.GeoAdd(new RedisKey("Sicily"), 13.361389, 38.115556, new RedisValue("Palermo"), CommandFlags.None);
            var response = db.GeoPosition(new RedisKey("Sicily"), ["Palermo", "Unknown"]);
            ClassicAssert.AreEqual(2, response.Length);
            ClassicAssert.AreEqual(default(GeoPosition), response[1]);

            var memresponse = db.Execute("MEMORY", "USAGE", "Sicily");
            var actualValue = ResultType.Integer == memresponse.Resp2Type ? Int32.Parse(memresponse.ToString()) : -1;
            var expectedResponse = 344;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            db.GeoAdd(new RedisKey("SecondKey"), 13.361389, 38.115556, new RedisValue("Palermo"));
            response = db.GeoPosition(new RedisKey("SecondKey"), ["Palermo"]);
            ClassicAssert.AreEqual(1, response.Length);
            ClassicAssert.IsNotNull(response[0]);

            memresponse = db.Execute("MEMORY", "USAGE", "SecondKey");
            actualValue = ResultType.Integer == memresponse.Resp2Type ? Int32.Parse(memresponse.ToString()) : -1;
            expectedResponse = 352;
            ClassicAssert.AreEqual(expectedResponse, actualValue);

            var responseHash = db.GeoHash(new RedisKey("SecondKey"), ["Palermo"]);
            ClassicAssert.AreEqual(1, responseHash.Length);
            ClassicAssert.AreEqual("sqc8b49rnys", responseHash[0]);

            memresponse = db.Execute("MEMORY", "USAGE", "SecondKey");
            actualValue = ResultType.Integer == memresponse.Resp2Type ? Int32.Parse(memresponse.ToString()) : -1;
            expectedResponse = 352;
            ClassicAssert.AreEqual(expectedResponse, actualValue);
        }

        [Test]
        public void CheckGeoSortedSetOperationsOnWrongTypeObjectSE()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var keys = new[] { new RedisKey("user1:obj1"), new RedisKey("user1:obj2") };
            RedisValue[][] values =
            [
                [new RedisValue("Tel Aviv"), new RedisValue("Haifa")],
                [new RedisValue("Athens"), new RedisValue("Thessaloniki")]
            ];
            double[][][] coords =
            [
                [[2.0853, 34.7818], [32.7940, 34.9896]],
                [[7.9838, 23.7275], [40.6401, 22.9444]],
            ];

            var geoEntries = values.Select((h, idx) => h
                .Zip(coords[idx], (v, c) => new GeoEntry(c[0], c[1], v)).ToArray()).ToArray();

            // Set up different type objects
            RespTestsUtils.SetUpTestObjects(db, GarnetObjectType.Set, keys, values);

            // GEOADD
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.GeoAdd(keys[0], geoEntries[0]));
            // GEOHASH
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.GeoHash(keys[0], values[0]));
            // GEODIST
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.GeoDistance(keys[0], values[0][1], values[0][1]));
            // GEOPOS
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.GeoPosition(keys[0], values[0]));
            // GEORADIUS
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.GeoRadius(keys[0], values[0][1], 800, GeoUnit.Kilometers));
            // GEOSEARCH
            RespTestsUtils.CheckCommandOnWrongTypeObjectSE(() => db.GeoSearch(keys[0], values[0][1], new GeoSearchBox(800, 800, GeoUnit.Kilometers)));
        }

        [Test]
        public void CanUseGeoSearch()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var entries = new GeoEntry[cities.GetLength(0)];
            var key = new RedisKey("cities");
            var destinationKey = new RedisKey("newCities");
            for (var j = 0; j < cities.GetLength(0); j++)
            {
                entries[j] = new GeoEntry(
                    double.Parse(cities[j, 0], CultureInfo.InvariantCulture),
                    double.Parse(cities[j, 1], CultureInfo.InvariantCulture),
                    new RedisValue(cities[j, 2]));
            }
            var response = db.GeoAdd(key, entries, CommandFlags.None);

            var res = db.GeoSearch(key, new RedisValue("Washington"), new GeoSearchBox(800, 800, GeoUnit.Kilometers),
                                   order: Order.Ascending, options: GeoRadiusOptions.None);
            ClassicAssert.AreEqual(3, res.Length);
            ClassicAssert.AreEqual("Washington", (string)res[0].Member);
            ClassicAssert.AreEqual(res[0].Distance, null);
            ClassicAssert.AreEqual(res[0].Position, null);
            ClassicAssert.AreEqual("Philadelphia", (string)res[1].Member);
            ClassicAssert.AreEqual(res[1].Distance, null);
            ClassicAssert.AreEqual(res[1].Position, null);
            ClassicAssert.AreEqual("New York", (string)res[2].Member);
            ClassicAssert.AreEqual(res[2].Distance, null);
            ClassicAssert.AreEqual(res[2].Position, null);

            res = db.GeoSearch(key, new RedisValue("Washington"), new GeoSearchBox(800, 800, GeoUnit.Kilometers),
                               order: Order.Ascending,
                               options: GeoRadiusOptions.WithDistance);
            ClassicAssert.AreEqual(3, res.Length);
            ClassicAssert.AreEqual("Washington", (string)res[0].Member);
            Assert.That(res[0].Distance, Is.EqualTo(0).Within(1.0 / Math.Pow(10, 6)));
            ClassicAssert.AreEqual(res[0].Position, null);
            ClassicAssert.AreEqual("Philadelphia", (string)res[1].Member);
            Assert.That(res[1].Distance, Is.EqualTo(198.424300439725).Within(1.0 / Math.Pow(10, 6)));
            ClassicAssert.AreEqual(res[1].Position, null);
            ClassicAssert.AreEqual("New York", (string)res[2].Member);
            Assert.That(res[2].Distance, Is.EqualTo(327.676458633557).Within(1.0 / Math.Pow(10, 6)));
            ClassicAssert.AreEqual(res[2].Position, null);

            res = db.GeoSearch(key, new RedisValue("Washington"), new GeoSearchBox(800, 800, GeoUnit.Kilometers),
                               order: Order.Ascending,
                               options: GeoRadiusOptions.WithCoordinates);
            ClassicAssert.AreEqual(3, res.Length);
            ClassicAssert.AreEqual("Washington", (string)res[0].Member);
            ClassicAssert.AreEqual(res[0].Distance, null);
            Assert.That(res[0].Position.Value.Longitude, Is.EqualTo(-77.03687042).Within(1.0 / Math.Pow(10, 6)));
            Assert.That(res[0].Position.Value.Latitude, Is.EqualTo(38.9071919).Within(1.0 / Math.Pow(10, 6)));
            ClassicAssert.AreEqual("Philadelphia", (string)res[1].Member);
            ClassicAssert.AreEqual(res[1].Distance, null);
            Assert.That(res[1].Position.Value.Longitude, Is.EqualTo(-75.1652196).Within(1.0 / Math.Pow(10, 6)));
            Assert.That(res[1].Position.Value.Latitude, Is.EqualTo(39.95258287).Within(1.0 / Math.Pow(10, 6)));
            ClassicAssert.AreEqual("New York", (string)res[2].Member);
            ClassicAssert.AreEqual(res[2].Distance, null);
            Assert.That(res[2].Position.Value.Longitude, Is.EqualTo(-74.00594205).Within(1.0 / Math.Pow(10, 6)));
            Assert.That(res[2].Position.Value.Latitude, Is.EqualTo(40.71278259).Within(1.0 / Math.Pow(10, 6)));

            res = db.GeoSearch(key, new RedisValue("Washington"), new GeoSearchBox(800, 800, GeoUnit.Kilometers),
                               order: Order.Ascending,
                               options: GeoRadiusOptions.WithDistance | GeoRadiusOptions.WithCoordinates);
            ClassicAssert.AreEqual(3, res.Length);
            ClassicAssert.AreEqual("Washington", (string)res[0].Member);
            Assert.That(res[0].Distance, Is.EqualTo(0).Within(1.0 / Math.Pow(10, 6)));
            Assert.That(res[0].Position.Value.Longitude, Is.EqualTo(-77.03687042).Within(1.0 / Math.Pow(10, 6)));
            Assert.That(res[0].Position.Value.Latitude, Is.EqualTo(38.9071919).Within(1.0 / Math.Pow(10, 6)));
            ClassicAssert.AreEqual("Philadelphia", (string)res[1].Member);
            Assert.That(res[1].Distance, Is.EqualTo(198.424300439725).Within(1.0 / Math.Pow(10, 6)));
            Assert.That(res[1].Position.Value.Longitude, Is.EqualTo(-75.1652196).Within(1.0 / Math.Pow(10, 6)));
            Assert.That(res[1].Position.Value.Latitude, Is.EqualTo(39.95258287).Within(1.0 / Math.Pow(10, 6)));
            ClassicAssert.AreEqual("New York", (string)res[2].Member);
            Assert.That(res[2].Distance, Is.EqualTo(327.676458633557).Within(1.0 / Math.Pow(10, 6)));
            Assert.That(res[2].Position.Value.Longitude, Is.EqualTo(-74.00594205).Within(1.0 / Math.Pow(10, 6)));
            Assert.That(res[2].Position.Value.Latitude, Is.EqualTo(40.71278259).Within(1.0 / Math.Pow(10, 6)));

            res = db.GeoSearch(key, new RedisValue("Washington"), new GeoSearchCircle(530, GeoUnit.Kilometers),
                               order: Order.Descending,
                               options: GeoRadiusOptions.WithDistance | GeoRadiusOptions.WithCoordinates);
            ClassicAssert.AreEqual(4, res.Length);
            ClassicAssert.AreEqual("Columbus", (string)res[0].Member);
            Assert.That(res[0].Distance, Is.EqualTo(525.298997019908).Within(1.0 / Math.Pow(10, 6)));
            Assert.That(res[0].Position.Value.Longitude, Is.EqualTo(-82.99879419999999).Within(1.0 / Math.Pow(10, 6)));
            Assert.That(res[0].Position.Value.Latitude, Is.EqualTo(39.9611766).Within(1.0 / Math.Pow(10, 6)));
            ClassicAssert.AreEqual("New York", (string)res[1].Member);
            Assert.That(res[1].Distance, Is.EqualTo(327.676458633557).Within(1.0 / Math.Pow(10, 6)));
            Assert.That(res[1].Position.Value.Longitude, Is.EqualTo(-74.00594205).Within(1.0 / Math.Pow(10, 6)));
            Assert.That(res[1].Position.Value.Latitude, Is.EqualTo(40.71278259).Within(1.0 / Math.Pow(10, 6)));
            ClassicAssert.AreEqual("Philadelphia", (string)res[2].Member);
            Assert.That(res[2].Distance, Is.EqualTo(198.424300439725).Within(1.0 / Math.Pow(10, 6)));
            Assert.That(res[2].Position.Value.Longitude, Is.EqualTo(-75.1652196).Within(1.0 / Math.Pow(10, 6)));
            Assert.That(res[2].Position.Value.Latitude, Is.EqualTo(39.95258287).Within(1.0 / Math.Pow(10, 6)));
            ClassicAssert.AreEqual("Washington", (string)res[3].Member);
            Assert.That(res[3].Distance, Is.EqualTo(0).Within(1.0 / Math.Pow(10, 6)));
            Assert.That(res[3].Position.Value.Longitude, Is.EqualTo(-77.03687042).Within(1.0 / Math.Pow(10, 6)));
            Assert.That(res[3].Position.Value.Latitude, Is.EqualTo(38.9071919).Within(1.0 / Math.Pow(10, 6)));

            res = db.GeoSearch(key, new RedisValue("Washington"), new GeoSearchCircle(530, GeoUnit.Kilometers),
                               options: GeoRadiusOptions.WithDistance | GeoRadiusOptions.WithCoordinates);
            ClassicAssert.AreEqual(4, res.Length);

            // Test infinity value
            res = db.GeoSearch(key, new RedisValue("Columbus"), new GeoSearchBox(2, double.PositiveInfinity, GeoUnit.Kilometers),
                               order: Order.Descending,
                               options: GeoRadiusOptions.WithDistance | GeoRadiusOptions.WithCoordinates);
            ClassicAssert.AreEqual(2, res.Length);
            ClassicAssert.AreEqual("Philadelphia", (string)res[0].Member);
            Assert.That(res[0].Distance, Is.EqualTo(667.66131901076665).Within(1.0 / Math.Pow(10, 6)));
            Assert.That(res[0].Position.Value.Longitude, Is.EqualTo(-75.1652196).Within(1.0 / Math.Pow(10, 6)));
            Assert.That(res[0].Position.Value.Latitude, Is.EqualTo(39.95258287).Within(1.0 / Math.Pow(10, 6)));
        }

        [Test]
        public void CanUseGeoSearchStore()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var entries = new GeoEntry[cities.GetLength(0)];
            var key = new RedisKey("cities");
            var destinationKey = new RedisKey("newCities");
            string lat = "0", lon = "0";

            for (int j = 0; j < cities.GetLength(0); j++)
            {
                if (string.Compare(cities[j, 2], "Washington",
                                   CultureInfo.InvariantCulture, CompareOptions.IgnoreCase) == 0)
                {
                    lon = cities[j, 0].TrimEnd();
                    lat = cities[j, 1].TrimEnd();
                }

                entries[j] = new GeoEntry(
                    double.Parse(cities[j, 0], CultureInfo.InvariantCulture),
                    double.Parse(cities[j, 1], CultureInfo.InvariantCulture),
                    new RedisValue(cities[j, 2]));
            }
            db.GeoAdd(key, entries, CommandFlags.None);

            db.SortedSetAdd(destinationKey, "OldValue", 10); // Add a value to be replaced

            var actualCount = db.GeoSearchAndStore(key, destinationKey, new RedisValue("Washington"), new GeoSearchBox(800, 800, GeoUnit.Kilometers), storeDistances: true);
            ClassicAssert.AreEqual(3, actualCount);

            var actualValues = db.SortedSetRangeByScoreWithScores(destinationKey);
            ClassicAssert.AreEqual(3, actualValues.Length);
            ClassicAssert.AreEqual("Washington", (string)actualValues[0].Element);
            Assert.That(actualValues[0].Score, Is.EqualTo(0).Within(1.0 / Math.Pow(10, 6)));
            ClassicAssert.AreEqual("Philadelphia", (string)actualValues[1].Element);
            Assert.That(actualValues[1].Score, Is.EqualTo(198.424300439725).Within(1.0 / Math.Pow(10, 6)));
            ClassicAssert.AreEqual("New York", (string)actualValues[2].Element);
            Assert.That(actualValues[2].Score, Is.EqualTo(327.676458633557).Within(1.0 / Math.Pow(10, 6)));

            _ = db.Execute("GEORADIUS", [key, lon, lat, 500, "KM", "COUNT", "300", "STOREDIST", destinationKey]);
            actualValues = db.SortedSetRangeByScoreWithScores(destinationKey);
            ClassicAssert.AreEqual(3, actualValues.Length);
            ClassicAssert.AreEqual("Washington", (string)actualValues[0].Element);
            Assert.That(actualValues[0].Score, Is.EqualTo(0).Within(1.0 / Math.Pow(10, 4)));
            ClassicAssert.AreEqual("Philadelphia", (string)actualValues[1].Element);
            Assert.That(actualValues[1].Score, Is.EqualTo(198.424300439725).Within(1.0 / Math.Pow(10, 4)));
            ClassicAssert.AreEqual("New York", (string)actualValues[2].Element);
            Assert.That(actualValues[2].Score, Is.EqualTo(327.676458633557).Within(1.0 / Math.Pow(10, 4)));
        }

        [Test]
        public void CanUseGeoSearchStoreWithDeleteKeyWhenSourceNotFound()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var entries = new GeoEntry[cities.GetLength(0)];
            var key = new RedisKey("cities");
            var destinationKey = new RedisKey("newCities");

            db.SortedSetAdd(destinationKey, "OldValue", 10);

            var actualCount = db.GeoSearchAndStore(key, destinationKey, new RedisValue("Washington"), new GeoSearchBox(800, 800, GeoUnit.Kilometers), storeDistances: true);
            ClassicAssert.AreEqual(0, actualCount);

            var actualValues = db.SortedSetRangeByScoreWithScores(destinationKey);
            ClassicAssert.AreEqual(0, actualValues.Length);

            actualCount = db.GeoSearchAndStore(key, destinationKey, new RedisValue("Washington"), new GeoSearchBox(800, 800, GeoUnit.Kilometers));
            ClassicAssert.AreEqual(0, actualCount);

            actualValues = db.SortedSetRangeByScoreWithScores(destinationKey);
            ClassicAssert.AreEqual(0, actualValues.Length);
        }

        //end region of SE tests
        #endregion

        #region LightClientTests

        [Test]
        [TestCase(10)]
        [TestCase(50)]
        [TestCase(100)]
        public void CanUseGeoSearchWithCities(int bytesSent)
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);
            var entries = new GeoEntry[cities.GetLength(0)];

            string lat = "0", lon = "0";
            for (var j = 0; j < cities.GetLength(0); j++)
            {
                if (string.Compare(cities[j, 2], "Washington",
                                   CultureInfo.InvariantCulture, CompareOptions.IgnoreCase) == 0)
                {
                    lon = cities[j, 0].TrimEnd();
                    lat = cities[j, 1].TrimEnd();
                }

                entries[j] = new GeoEntry(
                    double.Parse(cities[j, 0], CultureInfo.InvariantCulture),
                    double.Parse(cities[j, 1], CultureInfo.InvariantCulture),
                    new RedisValue(cities[j, 2]));
            }
            var response = db.GeoAdd(new RedisKey("cities"), entries, CommandFlags.None);
            ClassicAssert.AreEqual(23, response);

            //TODO: Assert values for latitude and longitude
            //TODO: Review precision to use for all framework versions
            using var lightClientRequest = TestUtils.CreateRequest();
            var responseBuf = lightClientRequest.SendCommands("GEOSEARCH cities FROMMEMBER Washington BYBOX 800 800 km WITHCOORD WITHDIST ASC", "PING");
            var expectedResponse = "*3\r\n*3\r\n$10\r\nWashington\r\n$1\r\n0\r\n*2\r\n$17\r\n-77.0368704199791\r\n$17\r\n38.90719190239906\r\n*3\r\n$12\r\nPhiladelphia\r\n$17\r\n198.4242996738795\r\n*2\r\n$18\r\n-75.16521960496902\r\n$18\r\n39.952582865953445\r\n*3\r\n$8\r\nNew York\r\n$18\r\n327.67645879712575\r\n*2\r\n$17\r\n-74.0059420466423\r\n$18\r\n40.712782591581345\r\n+PONG\r\n";
            var actualValue = Encoding.ASCII.GetString(responseBuf, 0, expectedResponse.Length);
            ClassicAssert.IsTrue(actualValue.IndexOf("Washington") != -1);

            //Send command in chunks
            responseBuf = lightClientRequest.SendCommandChunks("GEOSEARCH cities FROMMEMBER Washington BYBOX 800 800 km COUNT 3 ANY WITHCOORD WITHDIST ASC", bytesSent, 16);
            expectedResponse = "*3\r\n*3\r\n$10\r\nWashington\r\n$1\r\n0\r\n*2\r\n$17\r\n-77.0368704199791\r\n$17\r\n38.90719190239906\r\n*3\r\n$12\r\nPhiladelphia\r\n$17\r\n198.4242996738795\r\n*2\r\n$18\r\n-75.16521960496902\r\n$18\r\n39.952582865953445\r\n*3\r\n$8\r\nNew York\r\n$18\r\n327.67645879712575\r\n*2\r\n$17\r\n-74.0059420466423\r\n$18\r\n40.712782591581345\r\n+PONG\r\n";
            actualValue = Encoding.ASCII.GetString(responseBuf, 0, expectedResponse.Length);
            ClassicAssert.IsTrue(actualValue.IndexOf("Washington") != -1);

            using var lightClientRequest2 = TestUtils.CreateRequest();
            responseBuf = lightClientRequest2.SendCommands("GEOSEARCH cities FROMMEMBER Washington BYRADIUS 500 km WITHCOORD WITHDIST ASC", "PING");
            expectedResponse = "*3\r\n*3\r\n$10\r\nWashington\r\n$1\r\n0\r\n*2\r\n$17\r\n-77.0368704199791\r\n$17\r\n38.90719190239906\r\n*3\r\n$12\r\nPhiladelphia\r\n$17\r\n198.4242996738795\r\n*2\r\n$18\r\n-75.16521960496902\r\n$18\r\n39.952582865953445\r\n*3\r\n$8\r\nNew York\r\n$18\r\n327.67645879712575\r\n*2\r\n$17\r\n-74.0059420466423\r\n$18\r\n40.712782591581345\r\n+PONG\r\n";
            actualValue = Encoding.ASCII.GetString(responseBuf, 0, expectedResponse.Length);
            ClassicAssert.IsTrue(actualValue.IndexOf("Washington") != -1);

            responseBuf = lightClientRequest2.SendCommandChunks($"GEOSEARCH cities FROMLONLAT {lon} {lat} BYRADIUS 500 km COUNT 3 ANY WITHCOORD WITHDIST ASC", bytesSent, 16);
            expectedResponse = "*3\r\n*3\r\n$10\r\nWashington\r\n$1\r\n0\r\n*2\r\n$17\r\n-77.0368704199791\r\n$17\r\n38.90719190239906\r\n*3\r\n$12\r\nPhiladelphia\r\n$17\r\n198.4242996738795\r\n*2\r\n$18\r\n-75.16521960496902\r\n$18\r\n39.952582865953445\r\n*3\r\n$8\r\nNew York\r\n$18\r\n327.67645879712575\r\n*2\r\n$17\r\n-74.0059420466423\r\n$18\r\n40.712782591581345\r\n+PONG\r\n";
            actualValue = Encoding.ASCII.GetString(responseBuf, 0, expectedResponse.Length);
            ClassicAssert.IsTrue(actualValue.IndexOf("Washington") != -1);

            using var lightClientRequest3 = TestUtils.CreateRequest();
            responseBuf = lightClientRequest3.SendCommands("GEORADIUSBYMEMBER_RO cities Washington 500 km WITHCOORD WITHDIST ASC", "PING");
            expectedResponse = "*3\r\n*3\r\n$10\r\nWashington\r\n$1\r\n0\r\n*2\r\n$17\r\n-77.0368704199791\r\n$17\r\n38.90719190239906\r\n*3\r\n$12\r\nPhiladelphia\r\n$17\r\n198.4242996738795\r\n*2\r\n$18\r\n-75.16521960496902\r\n$18\r\n39.952582865953445\r\n*3\r\n$8\r\nNew York\r\n$18\r\n327.67645879712575\r\n*2\r\n$17\r\n-74.0059420466423\r\n$18\r\n40.712782591581345\r\n+PONG\r\n";
            actualValue = Encoding.ASCII.GetString(responseBuf, 0, expectedResponse.Length);
            ClassicAssert.IsTrue(actualValue.IndexOf("Washington") != -1);

            responseBuf = lightClientRequest3.SendCommandChunks($"GEORADIUS_RO cities {lon} {lat} 500 km COUNT 3 ANY WITHCOORD WITHDIST ASC", bytesSent, 16);
            expectedResponse = "*3\r\n*3\r\n$10\r\nWashington\r\n$1\r\n0\r\n*2\r\n$17\r\n-77.0368704199791\r\n$17\r\n38.90719190239906\r\n*3\r\n$12\r\nPhiladelphia\r\n$17\r\n198.4242996738795\r\n*2\r\n$18\r\n-75.16521960496902\r\n$18\r\n39.952582865953445\r\n*3\r\n$8\r\nNew York\r\n$18\r\n327.67645879712575\r\n*2\r\n$17\r\n-74.0059420466423\r\n$18\r\n40.712782591581345\r\n+PONG\r\n";
            actualValue = Encoding.ASCII.GetString(responseBuf, 0, expectedResponse.Length);
            ClassicAssert.IsTrue(actualValue.IndexOf("Washington") != -1);

            using var lightClientRequest4 = TestUtils.CreateRequest();
            responseBuf = lightClientRequest4.SendCommands($"GEORADIUS cities {lon} {lat} 500 km WITHCOORD WITHDIST ASC", "PING");
            actualValue = Encoding.ASCII.GetString(responseBuf, 0, expectedResponse.Length);
            ClassicAssert.IsTrue(actualValue.IndexOf("Washington") != -1);

            responseBuf = lightClientRequest4.SendCommandChunks("GEORADIUSBYMEMBER cities Washington 500 km WITHCOORD WITHDIST ASC", bytesSent, 16);
            actualValue = Encoding.ASCII.GetString(responseBuf, 0, expectedResponse.Length);
            ClassicAssert.IsTrue(actualValue.IndexOf("Washington") != -1);
        }

        [Test]
        [TestCase(10)]
        [TestCase(50)]
        [TestCase(100)]
        [TestCase(1000)]
        public void CanDoGeoAddWhenInvalidPairLC(int bytesSent)
        {
            using var lightClientRequest = TestUtils.CreateRequest();

            // Check GEOADD without members
            var response = lightClientRequest.SendCommandChunks("GEOADD Sicily NX", bytesSent);
            var expectedResponse = $"-{string.Format(CmdStrings.GenericErrWrongNumArgs, nameof(RespCommand.GEOADD))}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("GEOADD Sicily NX XX CH", bytesSent);
            expectedResponse = $"-ERR syntax error\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("GEOADD Sicily NX 13.361389 38.115556 Palermo 15.087269 37.502669 Catania", bytesSent);
            expectedResponse = ":2\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Add new elements, return only the elements changed
            response = lightClientRequest.SendCommandChunks("GEOADD Sicily NX CH 14.361389 39.115556 Palermo 15.087269 37.502669 Catania 38.0350 14.0212 Cefalu 37.8545 15.2889 Taormina", bytesSent);
            expectedResponse = ":2\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Only update elements, return only the elements changed
            response = lightClientRequest.SendCommandChunks("GEOADD Sicily XX CH 15.361389 39.115556 Palermo 15.087269 37.502669 Catania", bytesSent);
            expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // This should work too
            response = lightClientRequest.SendCommandChunks("GEOADD Sicily CH XX XX XX CH 15.361380 39.115556 Palermo 15.087269 37.502669 Catania", bytesSent);
            expectedResponse = ":1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            // Add and update
            response = lightClientRequest.SendCommandChunks("GEOADD Sicily CH 13.361389 38.115556 Palermo 15.087269 37.502669 Catania 13.583333 37.316667 Agrigento", bytesSent);
            expectedResponse = ":2\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        [TestCase(10)]
        [TestCase(50)]
        [TestCase(100)]
        public void CanUseGeoHash(int bytesSent)
        {
            using var lightClientRequest = TestUtils.CreateRequest(countResponseType: CountResponseType.Bytes);

            var expectedResponse = ":2\r\n+PONG\r\n";
            var response = lightClientRequest.Execute("GEOADD Sicily 13.361389 38.115556 Palermo 15.087269 37.502669 Catania", "PING", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);

            expectedResponse = "*3\r\n$11\r\nsqc8b49rnys\r\n$11\r\nsqdtr74hyu0\r\n$-1\r\n+PONG\r\n";
            response = lightClientRequest.Execute("GEOHASH Sicily Palermo Catania Unknown", "PING", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);

            expectedResponse = "*3\r\n$11\r\nsqc8b49rnys\r\n$11\r\nsqdtr74hyu0\r\n$-1\r\n";
            response = lightClientRequest.Execute("GEOHASH Sicily Palermo Catania Unknown", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);

            // Execute command in chunks
            expectedResponse = "*1\r\n$11\r\nsqc8b49rnys\r\n";
            response = lightClientRequest.Execute("GEOHASH Sicily Palermo", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);
        }

        [Test]
        [TestCase(10)]
        [TestCase(50)]
        [TestCase(100)]
        public void CanUseGeoDist(int bytesSent)
        {
            using var lightClientRequest = TestUtils.CreateRequest(countResponseType: CountResponseType.Bytes);

            var expectedResponse = ":2\r\n+PONG\r\n";
            var response = lightClientRequest.Execute("GEOADD Sicily 13.361389 38.115556 Palermo 15.087269 37.502669 Catania", "PING", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);

            // Defaults to meters
            expectedResponse = "$18\r\n166274.12635918456\r\n";
            response = lightClientRequest.Execute("GEODIST Sicily Palermo Catania", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);

            expectedResponse = "$18\r\n166274.12635918456\r\n";
            response = lightClientRequest.Execute("GEODIST Sicily Palermo Catania M", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);

            expectedResponse = "$-1\r\n";
            response = lightClientRequest.Execute("GEODIST Sicily Foo Bar", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);

            expectedResponse = "$18\r\n166.27412635918458\r\n";
            response = lightClientRequest.Execute("GEODIST Sicily Palermo Catania km", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);

            expectedResponse = "$18\r\n166.27412635918458\r\n";
            response = lightClientRequest.Execute("GEODIST Sicily Palermo Catania km", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);

            expectedResponse = "$18\r\n103.31792016993288\r\n";
            response = lightClientRequest.Execute("GEODIST Sicily Palermo Catania MI", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);
        }

        [Test]
        [TestCase(10)]
        [TestCase(50)]
        [TestCase(100)]
        public void CanReturnNullGeoDistLC(int bytesSent)
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommandChunks("GEOADD Sicily 13.361389 38.115556 Palermo 15.087269 37.502669 Catania", bytesSent);
            var expectedResponse = ":2\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommands("GEODIST Sicily Palermo Unknown", "PING");
            expectedResponse = "$-1\r\n+PONG\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("GEODIST Sicily Palermo Unknown", bytesSent);
            expectedResponse = "$-1\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        [TestCase(10)]
        [TestCase(40)]
        [TestCase(100)]
        public void CanUseGeoPosLC(int bytesSent)
        {
            using var lightClientRequest = TestUtils.CreateRequest(countResponseType: CountResponseType.Bytes);

            var expectedResponse = ":2\r\n+PONG\r\n";
            var response = lightClientRequest.Execute("GEOADD Sicily 13.361389 38.115556 Palermo 15.087269 37.502669 Catania", "PING", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);

            // GEOPOS with unknown key
            response = lightClientRequest.Execute("GEOPOS Unknown Palermo Catania", expectedResponse.Length, bytesSent);
            expectedResponse = "*2\r\n*-1\r\n*-1\r\n";
            ClassicAssert.AreEqual(expectedResponse, response);

            expectedResponse = "*3\r\n*2\r\n$18\r\n13.361389338970184\r\n$17\r\n38.11555668711662\r\n*2\r\n$18\r\n15.087267458438873\r\n$18\r\n37.502669245004654\r\n*-1\r\n+PONG\r\n";
            response = lightClientRequest.Execute("GEOPOS Sicily Palermo Catania Unknown", "PING", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);

            expectedResponse = "*3\r\n*2\r\n$18\r\n13.361389338970184\r\n$17\r\n38.11555668711662\r\n*2\r\n$18\r\n15.087267458438873\r\n$18\r\n37.502669245004654\r\n*-1\r\n";
            response = lightClientRequest.Execute("GEOPOS Sicily Palermo Catania Unknown", expectedResponse.Length, bytesSent);
            ClassicAssert.AreEqual(expectedResponse, response);
        }

        [Test]
        [TestCase(10)]
        [TestCase(50)]
        [TestCase(100)]
        public void CanContinueWhenNotEnoughParametersInGeoAdd(int bytesSent)
        {
            using var lightClientRequest = TestUtils.CreateRequest();
            var response = lightClientRequest.SendCommands("GEOADD Sicily 13.361389 38.115556", "PING");
            var expectedResponse = $"-{string.Format(CmdStrings.GenericErrWrongNumArgs, "GEOADD")}\r\n+PONG\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommandChunks("GEOADD Sicily 13.361389 38.115556", bytesSent);
            expectedResponse = $"-{string.Format(CmdStrings.GenericErrWrongNumArgs, "GEOADD")}\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }

        [Test]
        public void InvalidGeoSearches()
        {
            using var lightClientRequest = TestUtils.CreateRequest();

            var response = lightClientRequest.SendCommand("GEOADD Sicily NX 13.361389 38.115556 Palermo 15.087269 37.502669 Catania");
            var expectedResponse = ":2\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("GEOSEARCH Sicily FROMMEMBER a BYRADIUS 1");
            expectedResponse = "-ERR wrong number of arguments for 'GEOSEARCH' command\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("GEOSEARCH Sicily FROMLONLAT 15 37 FROMMEMBER a BYRADIUS 1 km");
            expectedResponse = "-ERR syntax error\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("GEOSEARCH Sicily FROMMEMBER a BYRADIUS -1 km");
            expectedResponse = "-ERR radius cannot be negative\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("GEOSEARCH Sicily FROMMEMBER nx BYRADIUS 1 KM");
            expectedResponse = "-ERR could not decode requested zset member\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("GEOSEARCH Sicily FROMLONLAT 15 37 BYRADIUS 100 km BYBOX 400 400 km");
            expectedResponse = "-ERR syntax error\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("GEOSEARCH Sicily FROMLONLAT 15 37 BYBOX 400 400 km COUNT 0 ANY");
            expectedResponse = "-ERR COUNT must be > 0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("GEOSEARCH Sicily FROMLONLAT 181 37 BYBOX 400 400 km COUNT 1 ANY");
            expectedResponse = "-ERR invalid longitude,latitude pair 181.000000,37.000000\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("GEORADIUS Sicily 15 37 100 km WITHCOORD STORE a");
            expectedResponse = "-ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("GEORADIUSBYMEMBER Sicily member 100 km WITHHASH STOREDIST a");
            expectedResponse = "-ERR STORE option in GEORADIUSBYMEMBER is not compatible with WITHDIST, WITHHASH and WITHCOORD options\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("GEORADIUS_RO Sicily 15 37 100 km STORE a COUNT 5");
            expectedResponse = "-ERR syntax error\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("GEORADIUS Sicily 15 37 100 km STORE");
            expectedResponse = "-ERR wrong number of arguments for 'GEORADIUS' command\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("GEORADIUSBYMEMBER Sicily member lotsa km");
            expectedResponse = "-ERR need numeric radius\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("GEORADIUSBYMEMBER Sicily member 50 NM");
            expectedResponse = "-ERR unsupported unit provided. please use M, KM, FT, MI\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("GEODIST Sicily Catania Palermo NM");
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("GEOSEARCHSTORE bar foo FROMMEMBER nx BYRADIUS 1 FT ANY COUNT 1");
            expectedResponse = "-ERR syntax error\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("GEOSEARCH foo FROMMEMBER bar BYBOX wide tall mi");
            expectedResponse = "-ERR need numeric width\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("GEOSEARCH foo FROMMEMBER bar BYBOX 12345 -12345 KM");
            expectedResponse = "-ERR height or width cannot be negative\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("GEOSEARCH foo FROMMEMBER bar BYRADIUS 0 m");
            expectedResponse = "*0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);

            response = lightClientRequest.SendCommand("GEOSEARCH Sicily FROMLONLAT 180 90 COUNT 1 ANY BYRADIUS 100 M");
            expectedResponse = "*0\r\n";
            TestUtils.AssertEqualUpToExpectedLength(expectedResponse, response);
        }
        #endregion
    }
}