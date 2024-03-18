// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;

namespace Tsavorite.core.Tests
{
    [TestFixture]
    public class ConcurrentCounterTests
    {
        [Test]
        public void Increment_IncreasesCounterValue()
        {
            // Arrange
            var counter = new ConcurrentCounter();

            // Act
            counter.Increment(5);

            // Assert
            Assert.AreEqual(5, counter.Total);
        }

        [Test]
        public void Increment_WithZeroValue_DoesNotChangeCounterValue()
        {
            // Arrange
            var counter = new ConcurrentCounter();

            // Act
            counter.Increment(0);

            // Assert
            Assert.AreEqual(0, counter.Total);
        }

        [Test]
        public void Total_ReturnsSumOfCounterValues()
        {
            // Arrange
            var counter = new ConcurrentCounter();
            counter.Increment(3);
            counter.Increment(7);

            // Act
            var total = counter.Total;

            // Assert
            Assert.AreEqual(10, total);
        }
        [Test]
        public void Increment_WithMaxValue_IncreasesCounterValue()
        {
            // Arrange
            var counter = new ConcurrentCounter();

            // Act
            counter.Increment(long.MaxValue);

            // Assert
            Assert.AreEqual(long.MaxValue, counter.Total);
        }

        [Test]
        public void Increment_WithMinValue_IncreasesCounterValue()
        {
            // Arrange
            var counter = new ConcurrentCounter();

            // Act
            counter.Increment(long.MinValue);

            // Assert
            Assert.AreEqual(long.MinValue, counter.Total);
        }

        [Test]
        public void IncrementndTotal_WithMultipleThreads_ReturnsCorrectValue()
        {
            // Arrange
            var numThreads = 10;
            var counter = new ConcurrentCounter();
            var threads = new System.Threading.Thread[numThreads];
            for (int i = 0; i < threads.Length; i++)
            {
                threads[i] = new System.Threading.Thread(() =>
                {
                    for (int j = 0; j < 1000; j++)
                    {
                        counter.Increment(1);
                    }
                });
            }

            // Act
            foreach (var thread in threads)
            {
                thread.Start();
            }
            foreach (var thread in threads)
            {
                thread.Join();
            }

            // Assert
            Assert.AreEqual(10000, counter.Total);
        }
    }
}