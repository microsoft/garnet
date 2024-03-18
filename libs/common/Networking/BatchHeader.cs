// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Garnet.networking
{
    /// <summary>
    /// Header for message batch (Little Endian server)
    /// [4 byte seqNo][1 byte protocol][3 byte numMessages]
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public struct BatchHeader
    {
        /// <summary>
        /// Size
        /// </summary>
        public const int Size = 8;

        /// <summary>
        /// Sequence number.
        /// </summary>
        [FieldOffset(0)]
        public int SeqNo;

        /// <summary>
        /// Lower-order 8 bits are wire protocol, higher-order 24 bits are num messages.
        /// </summary>
        [FieldOffset(4)]
        private int numMessagesAndProtocol;

        /// <summary>
        /// Number of messages packed in batch
        /// </summary>
        public int NumMessages
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (int)((uint)numMessagesAndProtocol >> 8);
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { numMessagesAndProtocol = (value << 8) | (numMessagesAndProtocol & 0xFF); }
        }

        /// <summary>
        /// Wire protocol this batch is written in
        /// </summary>
        public WireFormat Protocol
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (WireFormat)(numMessagesAndProtocol & 0xFF);
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { numMessagesAndProtocol = (numMessagesAndProtocol & ~0xFF) | ((int)value & 0xFF); }
        }

        /// <summary>
        /// Set num messages and wire protocol
        /// </summary>
        /// <param name="numMessages"></param>
        /// <param name="protocol"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetNumMessagesProtocol(int numMessages, WireFormat protocol)
        {
            numMessagesAndProtocol = (numMessages << 8) | (int)protocol;
        }
    }
}