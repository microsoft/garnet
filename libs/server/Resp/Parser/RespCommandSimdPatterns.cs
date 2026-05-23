// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.Intrinsics;

namespace Garnet.server
{
    /// <summary>
    /// SIMD Vector128 patterns and masks for <see cref="RespServerSession.FastParseCommand"/>.
    /// </summary>
    internal sealed unsafe partial class RespServerSession
    {
        // SIMD Vector128 patterns for FastParseCommand.
        // Each encodes the full RESP header + command: *N\r\n$L\r\nCMD\r\n
        // Masks zero out trailing bytes for patterns shorter than 16 bytes.
        private static readonly Vector128<byte> s_mask13 = Vector128.Create(
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00).AsByte();
        private static readonly Vector128<byte> s_mask14 = Vector128.Create(
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00).AsByte();
        private static readonly Vector128<byte> s_mask15 = Vector128.Create(
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00).AsByte();

        /// <summary>
        /// Builds a Vector128 RESP pattern for SIMD matching: *{argCount}\r\n${cmdLen}\r\n{cmd}\r\n
        /// Zero-padded to 16 bytes. Only called during static init.
        /// </summary>
        private static Vector128<byte> RespPattern(int argCount, string cmd)
        {
            // Total encoded length: 4 (*N\r\n) + 4 ($L\r\n) + cmd.Length + 2 (\r\n) = cmd.Length + 10
            var totalLen = cmd.Length + 10;
            if (totalLen > 16)
                throw new ArgumentException($"SIMD pattern overflow: command '{cmd}' with {argCount} args requires {totalLen} bytes, max 16");
            if (argCount < 1 || argCount > 9)
                throw new ArgumentException($"SIMD pattern requires single-digit arg count (1-9), got {argCount} for '{cmd}'");
            if (cmd.Length < 1 || cmd.Length > 9)
                throw new ArgumentException($"SIMD pattern requires single-digit command length (1-9), got {cmd.Length} for '{cmd}'");

            Span<byte> buf = stackalloc byte[16];
            buf.Clear();
            buf[0] = (byte)'*';
            buf[1] = (byte)('0' + argCount);
            buf[2] = (byte)'\r';
            buf[3] = (byte)'\n';
            buf[4] = (byte)'$';
            buf[5] = (byte)('0' + cmd.Length);
            buf[6] = (byte)'\r';
            buf[7] = (byte)'\n';
            for (int i = 0; i < cmd.Length; i++)
                buf[8 + i] = (byte)cmd[i];
            buf[8 + cmd.Length] = (byte)'\r';
            buf[9 + cmd.Length] = (byte)'\n';
            return Vector128.Create(buf);
        }

        // 13-byte: *N\r\n$3\r\nXXX\r\n (3-char commands)
        private static readonly Vector128<byte> s_GET = RespPattern(2, "GET");
        private static readonly Vector128<byte> s_SET = RespPattern(3, "SET");
        private static readonly Vector128<byte> s_DEL = RespPattern(2, "DEL");
        private static readonly Vector128<byte> s_TTL = RespPattern(2, "TTL");

        // 14-byte: *N\r\n$4\r\nXXXX\r\n (4-char commands)
        private static readonly Vector128<byte> s_PING = RespPattern(1, "PING");
        private static readonly Vector128<byte> s_INCR = RespPattern(2, "INCR");
        private static readonly Vector128<byte> s_DECR = RespPattern(2, "DECR");
        private static readonly Vector128<byte> s_EXEC = RespPattern(1, "EXEC");
        private static readonly Vector128<byte> s_PTTL = RespPattern(2, "PTTL");

        // 15-byte: *N\r\n$5\r\nXXXXX\r\n (5-char commands)
        private static readonly Vector128<byte> s_MULTI = RespPattern(1, "MULTI");
        private static readonly Vector128<byte> s_SETNX = RespPattern(3, "SETNX");
        private static readonly Vector128<byte> s_SETEX = RespPattern(4, "SETEX");

        // 16-byte: *N\r\n$6\r\nXXXXXX\r\n (6-char commands, no mask needed)
        private static readonly Vector128<byte> s_EXISTS = RespPattern(2, "EXISTS");
        private static readonly Vector128<byte> s_GETDEL = RespPattern(2, "GETDEL");
        private static readonly Vector128<byte> s_APPEND = RespPattern(3, "APPEND");
        private static readonly Vector128<byte> s_INCRBY = RespPattern(3, "INCRBY");
        private static readonly Vector128<byte> s_DECRBY = RespPattern(3, "DECRBY");
        private static readonly Vector128<byte> s_PSETEX = RespPattern(4, "PSETEX");
    }
}