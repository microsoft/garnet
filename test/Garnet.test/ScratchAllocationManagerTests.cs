// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    public class ScratchAllocationManagerTests
    {
        [Test]

        public void CreateArgSliceAndRewindTest([Values(-1, 127, 8192)] int backupBufferMaxSize)
        {
            var string1 = "Hello";
            var string2 = "My name is Inigo Montoya. You killed my father. Prepare to die.";
            var string3 = "Lorem ipsum dolor sit amet consectetur adipiscing elit. Quisque faucibus ex sapien vitae pellentesque sem placerat. In id cursus mi pretium tellus duis convallis. Tempus leo eu aenean sed diam urna tempor. Pulvinar vivamus fringilla lacus nec metus bibendum egestas. Iaculis massa nisl malesuada lacinia integer nunc posuere. Ut hendrerit semper vel class aptent taciti sociosqu. Ad litora torquent per conubia nostra inceptos himenaeos.\r\n\r\nLorem ipsum dolor sit amet consectetur adipiscing elit. Quisque faucibus ex sapien vitae pellentesque sem placerat. In id cursus mi pretium tellus duis convallis. Tempus leo eu aenean sed diam urna tempor. Pulvinar vivamus fringilla lacus nec metus bibendum egestas. Iaculis massa nisl malesuada lacinia integer nunc posuere. Ut hendrerit semper vel class aptent taciti sociosqu. Ad litora torquent per conubia nostra inceptos himenaeos.\r\n\r\nLorem ipsum dolor sit amet consectetur adipiscing elit. Quisque faucibus ex sapien vitae pellentesque sem placerat. In id cursus mi pretium tellus duis convallis. Tempus leo eu aenean sed diam urna tempor. Pulvinar vivamus fringilla lacus nec metus bibendum egestas. Iaculis massa nisl malesuada lacinia integer nunc posuere. Ut hendrerit semper vel class aptent taciti sociosqu. Ad litora torquent per conubia nostra inceptos himenaeos.\r\n\r\nLorem ipsum dolor sit amet consectetur adipiscing elit. Quisque faucibus ex sapien vitae pellentesque sem placerat. In id cursus mi pretium tellus duis convallis. Tempus leo eu aenean sed diam urna tempor. Pulvinar vivamus fringilla lacus nec metus bibendum egestas. Iaculis massa nisl malesuada lacinia integer nunc posuere. Ut hendrerit semper vel class aptent taciti sociosqu. Ad litora torquent per conubia nostra inceptos himenaeos.\r\n\r\nLorem ipsum dolor sit amet consectetur adipiscing elit. Quisque faucibus ex sapien vitae pellentesque sem placerat. In id cursus mi pretium tellus duis convallis. Tempus leo eu aenean sed diam urna tempor. Pulvinar vivamus fringilla lacus nec metus bibendum egestas. Iaculis massa nisl malesuada lacinia integer nunc posuere. Ut hendrerit semper vel class aptent taciti sociosqu. Ad litora torquent per conubia nostra inceptos himenaeos.\r\n\r\nLorem ipsum dolor sit amet consectetur adipiscing elit. Quisque faucibus ex sapien vitae pellentesque sem placerat. In id cursus mi pretium tellus duis convallis. Tempus leo eu aenean sed diam urna tempor. Pulvinar vivamus fringilla lacus nec metus bibendum egestas. Iaculis massa nisl malesuada lacinia integer nunc posuere. Ut hendrerit semper vel class aptent taciti sociosqu. Ad litora torquent per conubia nostra inceptos himenaeos.\r\n\r\nLorem ipsum dolor sit amet consectetur adipiscing elit. Quisque faucibus ex sapien vitae pellentesque sem placerat. In id cursus mi pretium tellus duis convallis. Tempus leo eu aenean sed diam urna tempor. Pulvinar vivamus fringilla lacus nec metus bibendum egestas. Iaculis massa nisl malesuada lacinia integer nunc posuere. Ut hendrerit semper vel class aptent taciti sociosqu. Ad litora torquent per conubia nostra inceptos himenaeos.\r\n\r\nLorem ipsum dolor sit amet consectetur adipiscing elit. Quisque faucibus ex sapien vitae pellentesque sem placerat. In id cursus mi pretium tellus duis convallis. Tempus leo eu aenean sed diam urna tempor. Pulvinar vivamus fringilla lacus nec metus bibendum egestas. Iaculis massa nisl malesuada lacinia integer nunc posuere. Ut hendrerit semper vel class aptent taciti sociosqu. Ad litora torquent per conubia nostra inceptos himenaeos.\r\n\r\nLorem ipsum dolor sit amet consectetur adipiscing elit. Quisque faucibus ex sapien vitae pellentesque sem placerat. In id cursus mi pretium tellus duis convallis. Tempus leo eu aenean sed diam urna tempor. Pulvinar vivamus fringilla lacus nec metus bibendum egestas. Iaculis massa nisl malesuada lacinia integer nunc posuere. Ut hendrerit semper vel class aptent taciti sociosqu. Ad litora torquent per conubia nostra inceptos himenaeos.\r\n\r\nLorem ipsum dolor sit amet consectetur adipiscing elit. Quisque faucibus ex sapien vitae pellentesque sem placerat. In id cursus mi pretium tellus duis convallis. Tempus leo eu aenean sed diam urna tempor. Pulvinar vivamus fringilla lacus nec metus bibendum egestas. Iaculis massa nisl malesuada lacinia integer nunc posuere. Ut hendrerit semper vel class aptent taciti sociosqu. Ad litora torquent per conubia nostra inceptos himenaeos.\r\n\r\nLorem ipsum dolor sit amet consectetur adipiscing elit. Quisque faucibus ex sapien vitae pellentesque sem placerat. In id cursus mi pretium tellus duis convallis. Tempus leo eu aenean sed diam urna tempor. Pulvinar vivamus fringilla lacus nec metus bibendum egestas. Iaculis massa nisl malesuada lacinia integer nunc posuere. Ut hendrerit semper vel class aptent taciti sociosqu. Ad litora torquent per conubia nostra inceptos himenaeos.\r\n\r\nLorem ipsum dolor sit amet consectetur adipiscing elit. Quisque faucibus ex sapien vitae pellentesque sem placerat. In id cursus mi pretium tellus duis convallis. Tempus leo eu aenean sed diam urna tempor. Pulvinar vivamus fringilla lacus nec metus bibendum egestas. Iaculis massa nisl malesuada lacinia integer nunc posuere. Ut hendrerit semper vel class aptent taciti sociosqu. Ad litora torquent per conubia nostra inceptos himenaeos.\r\n\r\nLorem ipsum dolor sit amet consectetur adipiscing elit. Quisque faucibus ex sapien vitae pellentesque sem placerat. In id cursus mi pretium tellus duis convallis. Tempus leo eu aenean sed diam urna tempor. Pulvinar vivamus fringilla lacus nec metus bibendum egestas. Iaculis massa nisl malesuada lacinia integer nunc posuere. Ut hendrerit semper vel class aptent taciti sociosqu. Ad litora torquent per conubia nostra inceptos himenaeos.\r\n\r\nLorem ipsum dolor sit amet consectetur adipiscing elit. Quisque faucibus ex sapien vitae pellentesque sem placerat. In id cursus mi pretium tellus duis convallis. Tempus leo eu aenean sed diam urna tempor. Pulvinar vivamus fringilla lacus nec metus bibendum egestas. Iaculis massa nisl malesuada lacinia integer nunc posuere. Ut hendrerit semper vel class aptent taciti sociosqu. Ad litora torquent per conubia nostra inceptos himenaeos.\r\n\r\nLorem ipsum dolor sit amet consectetur adipiscing elit. Quisque faucibus ex sapien vitae pellentesque sem placerat. In id cursus mi pretium tellus duis convallis. Tempus leo eu aenean sed diam urna tempor. Pulvinar vivamus fringilla lacus nec metus bibendum egestas. Iaculis massa nisl malesuada lacinia integer nunc posuere. Ut hendrerit semper vel class aptent taciti sociosqu. Ad litora torquent per conubia nostra inceptos himenaeos.";

            var sam = new ScratchAllocationManager(backupScratchBufferMaxSize: backupBufferMaxSize);

            // Data of length 5 - SAM creates a buffer of size 64
            var as1 = sam.CreateArgSlice(string1);
            ClassicAssert.AreEqual(string1.Length, sam.ScratchBufferOffset);
            ClassicAssert.AreEqual(64, sam.TotalLength);

            // Data of length 63 - SAM creates an additional buffer of size 128
            var as2 = sam.CreateArgSlice(string2);
            ClassicAssert.AreEqual(string1.Length + string2.Length, sam.ScratchBufferOffset);
            ClassicAssert.AreEqual(64 + 128, sam.TotalLength);

            // Cannot re-wind slice that was not the last one created
            ClassicAssert.IsFalse(sam.RewindScratchBuffer(ref as1));

            // Re-wind last slice created - new offset is 64
            // Total length is either 64 or 64 + 128 (if we enabled a backup buffer with a max >= 128)
            ClassicAssert.IsTrue(sam.RewindScratchBuffer(ref as2));
            ClassicAssert.AreEqual(string1.Length, sam.ScratchBufferOffset);
            var expectedTotalSize = backupBufferMaxSize switch
            {
                < 128 => 64,
                _ => 64 + 128
            };
            ClassicAssert.AreEqual(expectedTotalSize, sam.TotalLength);

            // Re-wind last slice created - new offset is 0
            // Total length is unchanged
            ClassicAssert.IsTrue(sam.RewindScratchBuffer(ref as1));
            ClassicAssert.AreEqual(0, sam.ScratchBufferOffset);
            ClassicAssert.AreEqual(expectedTotalSize, sam.TotalLength);

            // Re-create slices for previous data
            // SAM should have either reused the backup buffer or allocated a new buffer of size 128
            // Either way, total length is 64 + 128
            _ = sam.CreateArgSlice(string1);
            _ = sam.CreateArgSlice(string2);
            ClassicAssert.AreEqual(64 + 128, sam.TotalLength);

            // Reset all buffers, offset should be 0
            // Total length is either 64 or 64 + 128 (if we enabled a backup buffer with a max >= 128)
            sam.Reset();
            ClassicAssert.AreEqual(0, sam.ScratchBufferOffset);
            ClassicAssert.AreEqual(expectedTotalSize, sam.TotalLength);

            // Data of length 6611 - SAM creates a buffer of size 8192 & removes the empty buffer of size 64
            var as3 = sam.CreateArgSlice(string3);
            // Data of length 5 - added to the existing buffer
            // Total length is either 8192, 64 + 8192 or 128 + 8192 (depending on the max backup buffer length)
            as1 = sam.CreateArgSlice(string1);
            ClassicAssert.AreEqual(string1.Length + string3.Length, sam.ScratchBufferOffset);
            expectedTotalSize = backupBufferMaxSize switch
            {
                <= 0 => 8192,
                < 128 => 64 + 8192,
                _ => 128 + 8192
            };
            ClassicAssert.AreEqual(expectedTotalSize, sam.TotalLength);

            // Re-wind last 2 slices created - new offset is 0
            // Total length is either 8192 or 128 + 8192 (if we enabled a backup buffer)
            ClassicAssert.IsTrue(sam.RewindScratchBuffer(ref as1));
            ClassicAssert.IsTrue(sam.RewindScratchBuffer(ref as3));
            ClassicAssert.AreEqual(0, sam.ScratchBufferOffset);
            ClassicAssert.AreEqual(expectedTotalSize, sam.TotalLength);
        }
    }
}