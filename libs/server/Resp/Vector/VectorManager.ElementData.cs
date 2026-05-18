// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Garnet.server
{
    /// <summary>
    /// Methods for converting data as received by Garnet into formats (and alignment) that DiskANN expects.
    /// </summary>
    public sealed partial class VectorManager
    {
        /// <summary>
        /// A vector's element data that has been prepared to be passed to DiskANN.
        /// 
        /// This means the data is pinned, correctly aligned, and converted to the expected format for the target index.
        /// </summary>
        internal readonly ref struct PreparedVectorData : IDisposable
        {
            private readonly GCHandle pin;
            private readonly byte[] rentedArray;

            /// <summary>
            /// Vector data to pass to DiskANN.
            /// </summary>
            public readonly ReadOnlySpan<byte> ReadOnlySpan { get; }

            /// <summary>
            /// Count of elements in <see cref="ReadOnlySpan"/>.
            /// 
            /// This is not the same as <see cref="Span{T}.Length"/> as the data might represent
            /// something other than bytes.
            /// </summary>
            public readonly int ElementCount { get; }

            /// <summary>
            /// Create a <see cref="PreparedVectorData"/> with an already pinned span.
            /// </summary>
            internal PreparedVectorData(ReadOnlySpan<byte> data, int count)
            {
                pin = default;
                rentedArray = default;
                ReadOnlySpan = data;
                ElementCount = count;
            }

            /// <summary>
            /// Create a <see cref="PreparedVectorData"/> with an array and a gc pin handle.
            /// </summary>
            internal PreparedVectorData(GCHandle pin, byte[] data, int count)
            {
                this.pin = pin;
                rentedArray = data;
                ReadOnlySpan = data;
                ElementCount = count;
            }

            /// <inheritdoc/>
            public void Dispose()
            {
                if (rentedArray != null)
                {
                    pin.Free();
                    ArrayPool<byte>.Shared.Return(rentedArray);
                }
            }
        }

        /// <summary>
        /// Ensure the provided vector data is converted and aligned for passing to DiskANN.
        /// 
        /// Quantizers have an internal "native" format they expect all vectors to passed in.
        /// <list type="bullet">
        ///   <item><see cref="VectorQuantType.NoQuant"/> -&gt; <see cref="VectorValueType.FP32"/></item>
        ///   <item><see cref="VectorQuantType.Q8"/> -&gt; <see cref="VectorValueType.FP32"/></item>
        ///   <item><see cref="VectorQuantType.Bin"/> -&gt; <see cref="VectorValueType.FP32"/></item>
        ///   <item><see cref="VectorQuantType.XNoQuant_U8"/> -&gt; <see cref="VectorValueType.XU8"/></item>
        ///   <item><see cref="VectorQuantType.XBin_I8"/> -&gt; <see cref="VectorValueType.XI8"/></item>
        /// </list>
        /// 
        /// Even if the formats match, the data must also be aligned to the element's native alignment (i.e. for <see cref="VectorValueType.FP32"/> that's 4 bytes, for <see cref="VectorValueType.XU8"/> it's 1 byte).
        /// </summary>
        private static PreparedVectorData PrepareVectorData(VectorQuantType quantType, VectorValueType valueType, ReadOnlySpan<byte> providedData, out ReadOnlySpan<byte> error)
        {
            switch (quantType)
            {
                // All Redis compatible quantizers expect F32 vectors
                case VectorQuantType.NoQuant:
                case VectorQuantType.Q8:
                case VectorQuantType.Bin:
                    error = default;
                    switch (valueType)
                    {
                        case VectorValueType.FP32: return ConvertF32ForAlignment(providedData);
                        case VectorValueType.XI8: return ConvertI8ToF32(providedData);
                        case VectorValueType.XU8: return ConvertU8ToF32(providedData);

                        case VectorValueType.Invalid:
                        default: throw new InvalidOperationException($"Unexpected VectorValueType: {valueType}");
                    }
                // NoQuant_U8 expected U8 vectors
                case VectorQuantType.XNoQuant_U8:
                    switch (valueType)
                    {
                        case VectorValueType.FP32: return ConvertF32ToU8(providedData, out error);
                        case VectorValueType.XI8: return ConvertI8ToU8(providedData, out error);
                        case VectorValueType.XU8:
                            error = default;
                            return new(providedData, providedData.Length);

                        case VectorValueType.Invalid:
                        default: throw new InvalidOperationException($"Unexpected VectorValueType: {valueType}");
                    }
                // XBin_I8 expects I8 vectors
                case VectorQuantType.XBin_I8:
                    switch (valueType)
                    {
                        case VectorValueType.FP32: return ConvertF32ToI8(providedData, out error);
                        case VectorValueType.XI8:
                            error = default;
                            return new(providedData, providedData.Length);
                        case VectorValueType.XU8: return ConvertU8ToI8(providedData, out error);

                        case VectorValueType.Invalid:
                        default: throw new InvalidOperationException($"Unexpected VectorValueType: {valueType}");
                    }

                case VectorQuantType.Invalid:
                default: throw new InvalidOperationException($"Unexpected VectorQuantType: {quantType}");
            }

            // Copy provided data (which is assumed to have floats in it) to a pinned and aligned buffer if needed
            static PreparedVectorData ConvertF32ForAlignment(ReadOnlySpan<byte> providedData)
            {
                var numElements = providedData.Length / sizeof(float);

                unsafe
                {
                    // Already aligned, pass it on down
                    var isAligned = ((nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(providedData)) % sizeof(float)) == 0;
                    if (isAligned)
                    {
                        return new(providedData, numElements);
                    }
                }

                // Need to copy to an aligned buffer
                var toCopyTo = ArrayPool<byte>.Shared.Rent(providedData.Length);
                var pin = GCHandle.Alloc(toCopyTo, GCHandleType.Pinned);

                unsafe
                {
                    Debug.Assert(((nint)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(toCopyTo)) % sizeof(float)) == 0, "byte[] should be word aligned, or we're in trouble");
                }

                providedData.CopyTo(toCopyTo);

                return new(pin, toCopyTo, numElements);
            }

            // Convert provided data (which is assumed to have signed byte in it) to a pinned buffer full of floats
            static PreparedVectorData ConvertI8ToF32(ReadOnlySpan<byte> providedData)
            {
                var asI8 = MemoryMarshal.Cast<byte, sbyte>(providedData);

                var numElements = providedData.Length;
                var numBytes = numElements * sizeof(float);

                // Need to copy to an aligned buffer
                var toCopyTo = ArrayPool<byte>.Shared.Rent(numBytes);
                var pin = GCHandle.Alloc(toCopyTo, GCHandleType.Pinned);

                unsafe
                {
                    Debug.Assert(((nint)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(toCopyTo)) % sizeof(float)) == 0, "byte[] should be word aligned, or we're in trouble");
                }

                var asFloats = MemoryMarshal.Cast<byte, float>(toCopyTo.AsSpan());

                // Do the actual copy, which expands sbyte -> float
                for (var i = 0; i < numElements; i++)
                {
                    asFloats[i] = asI8[i];
                }

                return new(pin, toCopyTo, numElements);
            }

            // Convert provided data (which is assumed to have unsigned bytes in it) to a pinned buffer full of floats
            static PreparedVectorData ConvertU8ToF32(ReadOnlySpan<byte> providedData)
            {
                var numElements = providedData.Length;
                var numBytes = numElements * sizeof(float);

                // Need to copy to an aligned buffer
                var toCopyTo = ArrayPool<byte>.Shared.Rent(numBytes);
                var pin = GCHandle.Alloc(toCopyTo, GCHandleType.Pinned);

                unsafe
                {
                    Debug.Assert(((nint)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(toCopyTo)) % sizeof(float)) == 0, "byte[] should be word aligned, or we're in trouble");
                }

                var asFloats = MemoryMarshal.Cast<byte, float>(toCopyTo.AsSpan());

                // Do the actual copy, which expands byte -> float
                for (var i = 0; i < numElements; i++)
                {
                    asFloats[i] = providedData[i];
                }

                return new(pin, toCopyTo, numElements);
            }

            // Convert provided data (which is assumed to have floats in it) to a pinned buffer full of unsigned bytes
            static PreparedVectorData ConvertF32ToU8(ReadOnlySpan<byte> providedData, out ReadOnlySpan<byte> error)
            {
                var asFloats = MemoryMarshal.Cast<byte, float>(providedData);
                var numElements = asFloats.Length;

                // Validate vector can convert from f32 -> u8 without issue
                if (asFloats.ContainsAnyExceptInRange(byte.MinValue, byte.MaxValue))
                {
                    error = "Vector contains element that is < 0 or > 255, operation will lose precision"u8;
                    return default;
                }

                // Need to copy to an aligned buffer
                var toCopyTo = ArrayPool<byte>.Shared.Rent(numElements);
                var pin = GCHandle.Alloc(toCopyTo, GCHandleType.Pinned);

                // Do the actual copy, which truncates f32 -> u8
                for (var i = 0; i < numElements; i++)
                {
                    var f32 = asFloats[i];
                    toCopyTo[i] = (byte)f32;
                }

                error = default;
                return new(pin, toCopyTo, numElements);
            }

            // Convert provided data (which is assumed to have signed bytes in it) to a pinned buffer full of unsigned bytes
            static PreparedVectorData ConvertI8ToU8(ReadOnlySpan<byte> providedData, out ReadOnlySpan<byte> error)
            {
                var asI8 = MemoryMarshal.Cast<byte, sbyte>(providedData);
                var numElements = asI8.Length;

                // Validate vector can convert from f32 -> u8 without issue
                if (asI8.ContainsAnyInRange(sbyte.MinValue, (sbyte)-1))
                {
                    error = "Vector contains element that is < 0, operation will lose precision"u8;
                    return default;
                }

                // Need to copy to an aligned buffer
                var toCopyTo = ArrayPool<byte>.Shared.Rent(numElements);
                var pin = GCHandle.Alloc(toCopyTo, GCHandleType.Pinned);

                // Do the actual copy, which truncates i8 -> u8
                for (var i = 0; i < numElements; i++)
                {
                    var i8 = asI8[i];
                    toCopyTo[i] = (byte)i8;
                }

                error = default;
                return new(pin, toCopyTo, numElements);
            }

            // Convert provided data (which is assumed to have floats in it) to a pinned buffer full of signed bytes
            static PreparedVectorData ConvertF32ToI8(ReadOnlySpan<byte> providedData, out ReadOnlySpan<byte> error)
            {
                var asFloats = MemoryMarshal.Cast<byte, float>(providedData);
                var numElements = asFloats.Length;

                // Validate vector can convert from f32 -> u8 without issue
                if (asFloats.ContainsAnyExceptInRange(sbyte.MinValue, sbyte.MaxValue))
                {
                    error = "Vector contains element that is < -128 or > 127, operation will lose precision"u8;
                    return default;
                }

                // Need to copy to an aligned buffer
                var toCopyTo = ArrayPool<byte>.Shared.Rent(numElements);
                var pin = GCHandle.Alloc(toCopyTo, GCHandleType.Pinned);

                var asSBytes = MemoryMarshal.Cast<byte, sbyte>(toCopyTo.AsSpan());

                // Do the actual copy, which truncates f32 -> i8
                for (var i = 0; i < numElements; i++)
                {
                    var f32 = asFloats[i];
                    asSBytes[i] = (sbyte)f32;
                }

                error = default;
                return new(pin, toCopyTo, numElements);
            }

            // Convert provided data (which is assumed to have unsigned bytes in it) to a pinned buffer full of signed bytes
            static PreparedVectorData ConvertU8ToI8(ReadOnlySpan<byte> providedData, out ReadOnlySpan<byte> error)
            {
                var numElements = providedData.Length;

                // Validate vector can convert from u8 -> i8 without issue
                if (providedData.ContainsAnyInRange((byte)128, byte.MaxValue))
                {
                    error = "Vector contains element that is > 127, operation will lose precision"u8;
                    return default;
                }

                // Need to copy to an aligned buffer
                var toCopyTo = ArrayPool<byte>.Shared.Rent(numElements);
                var pin = GCHandle.Alloc(toCopyTo, GCHandleType.Pinned);

                var asI8 = MemoryMarshal.Cast<byte, sbyte>(toCopyTo.AsSpan());

                // Do the actual copy, which truncates i8 -> u8
                for (var i = 0; i < numElements; i++)
                {
                    var u8 = providedData[i];
                    asI8[i] = (sbyte)u8;
                }

                error = default;
                return new(pin, toCopyTo, numElements);
            }
        }
    }
}