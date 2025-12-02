// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test
{
    [AllureNUnit]
    [TestFixture]
    internal class SimpleTests : AllureTestBase
    {
        [Test]
        [Category("TsavoriteKV")]
        public unsafe void AddressInfoTest()
        {
            AddressInfo info;

            AddressInfo.WriteInfo(&info, 44, 55);
            ClassicAssert.AreEqual(44, info.Address);
            ClassicAssert.AreEqual(512, info.Size);

            AddressInfo.WriteInfo(&info, 44, 512);
            ClassicAssert.AreEqual(44, info.Address);
            ClassicAssert.AreEqual(512, info.Size);

            AddressInfo.WriteInfo(&info, 44, 513);
            ClassicAssert.AreEqual(44, info.Address);
            ClassicAssert.AreEqual(1024, info.Size);

            if (sizeof(IntPtr) > 4)
            {
                AddressInfo.WriteInfo(&info, 44, 1L << 20);
                ClassicAssert.AreEqual(44, info.Address);
                ClassicAssert.AreEqual(1L << 20, info.Size);

                AddressInfo.WriteInfo(&info, 44, 511 * (1L << 20));
                ClassicAssert.AreEqual(44, info.Address);
                ClassicAssert.AreEqual(511 * (1L << 20), info.Size);

                AddressInfo.WriteInfo(&info, 44, 512 * (1L << 20));
                ClassicAssert.AreEqual(44, info.Address);
                ClassicAssert.AreEqual(512 * (1L << 20), info.Size);

                AddressInfo.WriteInfo(&info, 44, 555555555L);
                ClassicAssert.AreEqual(44, info.Address);
                ClassicAssert.AreEqual((1 + (555555555L / 512)) * 512, info.Size);

                AddressInfo.WriteInfo(&info, 44, 2 * 555555555L);
                ClassicAssert.AreEqual(44, info.Address);
                ClassicAssert.AreEqual((1 + (2 * 555555555L / 1048576)) * 1048576, info.Size);
            }
        }
    }
}