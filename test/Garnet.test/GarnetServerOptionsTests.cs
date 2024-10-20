// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.server;
using NUnit.Framework;
using Tsavorite.core;

namespace Garnet.test
{
    [TestFixture]
    public class GarnetServerOptionsTests
    {
        [Test]
        public void TestDeviceFactoryCreator_ReturnSameInstance()
        {
            // Arrange
            var options = new GarnetServerOptions
            {
                DeviceFactoryCreator = () => new LocalStorageNamedDeviceFactory()
            };

            // Act
            var device1 = options.GetDeviceFactory();
            var device2 = options.GetDeviceFactory();

            // Assert
            Assert.That(device1, Is.SameAs(device2));
        }
    }
}