// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//#define HLL_SINGLE_PFADD_ENABLED

using System;
#if DEBUG
using System.Collections.Generic;
#endif
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using Garnet.common;

#pragma warning disable IDE1006 // Naming Styles

namespace Garnet.server
{
    ///
    /// Hyperloglog implementation based on 
    /// "New cardinality estimation algorithms for HyperLogLog sketches " (https://arxiv.org/abs/1702.01284)
    ///    

    /// <summary>
    /// HLL Data structure types
    /// </summary>
    enum HLL_DTYPE : byte
    {
        HLL_SPARSE,
        HLL_DENSE
    }

    /// <summary>
    /// Garnet HypperLogLog implementation    
    /// </summary>
    public unsafe class HyperLogLog
    {
        private static readonly byte hbit = 64; // number of bits in hash value        
        private readonly byte pbit; //register offset bits
        private readonly byte qbit; //leading zero counting bits                           
        private readonly int mcnt; //register count

        private static readonly byte reg_bits = 6;
        private static readonly byte reg_bits_msk = (byte)((1 << reg_bits) - 1);
        /// Dense and Sparse representations have a 16 byte header (additional 2 bytes for Sparse to store length of RLE).
        /// [HYLL | E | N/U | Cardin]
        /// HYLL: 4 byte magic string
        /// E: 1 byte encoding for sparse/dense representation
        /// N/U: 3 bytes not used
        /// Cardin: 8 bytes (64 bit int) for previously computed cardinality if data has not changed
        private static readonly byte hll_header_bytes = 16;

        private static readonly double alpha = 0.721347520444481703680; /* constant for 0.5/ln(2) */

        /// <summary>
        /// How many bytes needed for dense representation
        /// </summary>
        public readonly int DenseBytes;

        /// <summary>
        /// How many bytes needed per new value added to the hll
        /// </summary>
        private readonly int SparseMaxBytesPerInsert = 2;

        /// <summary>
        /// Maximum number of bytes consumed by the sparse representation
        /// </summary>
        private readonly int SparseSizeMaxCap = (1 << 12);

        /// <summary>
        /// Sparse representation allocation increments
        /// </summary>
        public readonly int SparseMemorySectorSize = (1 << 7);

        /// <summary>
        /// Sparse representation initial zero ranges cnt
        /// </summary>        
        private readonly int SparseZeroRanges;

        /// <summary>
        /// Sparse header size = hll_header_bytes + RLE current size
        /// </summary>        
        private readonly int SparseHeaderSize;

        /// <summary>
        /// Sparse representation initial number of bytes
        /// header + sequence length in bytes + default zero sequence data + buffer to grow
        /// </summary>                
        public readonly int SparseBytes;

        /// <summary>
        /// Return bits used for indexing
        /// </summary>
        public byte PBit => pbit;

        /// <summary>
        /// Return bits used for clz
        /// </summary>
        public byte QBit => qbit;

        /// <summary>
        /// Default hyperloglog instance
        /// </summary>        
        public static readonly HyperLogLog DefaultHLL = new();

        /// <summary>
        /// Default Garnet HyperLogLog Constructor
        /// </summary>
        protected HyperLogLog() : this(14) //default bits for register offset
        { }

        /// <summary>
        /// Custom Garnet HyperLogLog Constructor
        /// </summary>
        /// <param name="pbit"></param>
        public HyperLogLog(byte pbit)
        {
            this.pbit = pbit;
            this.qbit = (byte)(hbit - pbit);
            this.mcnt = 1 << pbit;
            this.DenseBytes = hll_header_bytes + ((reg_bits * RegCnt) >> 3);
            this.SparseZeroRanges = this.mcnt >> 7;
            this.SparseHeaderSize = hll_header_bytes + 2;
            this.SparseBytes = SparseHeaderSize + SparseZeroRanges + SparseMemorySectorSize;
        }

        /// <summary>
        /// Get register count for dense HLL representation
        /// </summary>        
        public int RegCnt => mcnt;

        /// <summary>
        /// Extract register index
        /// </summary>
        /// <param name="hv"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ushort RegIdx(long hv) => (ushort)(hv & (this.mcnt - 1));

        /// <summary>
        /// Count leading zeros
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte clz(long hv)
        {
            ulong bits = (ulong)hv;
            byte lz = (byte)(BitOperations.LeadingZeroCount(bits));
            return lz >= qbit ? (byte)(qbit + 1) : (byte)(lz + 1);
        }

        /// <summary>
        /// Get register-idx value
        /// </summary>
        /// <param name="reg"></param>
        /// <param name="idx"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private byte _get_register(byte* reg, ushort idx)
        {
            int m = (int)idx * (int)reg_bits;
            ushort _b0 = (ushort)(m >> 3); // find byte zero location
            ushort _lsb = (ushort)(m & 0x7); // find bits in byte zero

            byte b0 = (byte)(reg[_b0] >> _lsb); // extract bits from byte zero
            byte b1 = (byte)(reg[_b0 + 1] << (8 - _lsb)); //extract bits from byte one
            //Console.WriteLine("{0},{1}", reg[_b0], reg[_b0 + 1]);
            return (byte)((b0 | b1) & reg_bits_msk); //combine bits in byte zero and one
        }

        /// <summary>
        /// Set register-idx in register array to val.
        /// </summary>
        /// <param name="reg"></param>
        /// <param name="idx"></param>
        /// <param name="val"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void _set_register(byte* reg, ushort idx, byte val)
        {
            int m = idx * reg_bits;
            ushort _b0 = (ushort)(m >> 3); // find byte zero location
            byte _lsb = (byte)(m & 0x7); // find bits in byte zero
            byte _msb = (byte)(8 - _lsb);

            Debug.Assert(_b0 < ((this.mcnt * reg_bits) / 8));

            reg[_b0] &= (byte)~(reg_bits_msk << _lsb);//clear bits for lsb
            reg[_b0] |= (byte)(val << _lsb);//set new value for lsb

            reg[_b0 + 1] &= (byte)(~(reg_bits_msk >> _msb));// clear bits for msb
            reg[_b0 + 1] |= (byte)((val >> _msb));//set new value for msb            
        }

        /// <summary>
        /// Check if header is correctly formatted.
        /// </summary>
        /// <param name="ptr"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsValidHYLL(byte* ptr) => (IsSparse(ptr) || IsDense(ptr)) && IsHYLL(ptr);

        /// <summary>
        /// Check if value is of type HLL
        /// </summary>
        /// <param name="ptr"></param>
        /// <param name="length"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsValidHYLL(byte* ptr, int length) => IsHYLL(ptr) && IsValidHLLLength(ptr, length);

        /// <summary>
        /// Check if tag is correctly set.
        /// </summary>
        /// <param name="ptr"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsHYLL(byte* ptr) => *(int*)(ptr + 4) == (int)0x48594C4C;

        private bool IsValidHLLLength(byte* ptr, int length)
        {
            return (IsSparse(ptr) && SparseInitialLength(1) <= length || length <= SparseSizeMaxCap) ||
                (IsDense(ptr) && length == this.DenseBytes);
        }

        /// <summary>
        /// Set prefix HYLL (HYperLogLog)
        /// </summary>     
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetPrefix(byte* ptr) => *(long*)ptr = (long)0x48594C4C00000000;

        /// <summary>
        /// Extract data structure type from header
        /// </summary>                
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte GetType(byte* ptr) => *(ptr + 3);

        /// <summary>
        /// Set representation type
        /// </summary>                
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetType(byte* ptr, byte dType) => *(ptr + 3) = dType;

        /// <summary>
        /// Check if data structure is sparse from header
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsSparse(byte* ptr) => GetType(ptr) == (byte)HLL_DTYPE.HLL_SPARSE;

        /// <summary>
        /// Check if data structure is sparse from dense
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsDense(byte* ptr) => GetType(ptr) == (byte)HLL_DTYPE.HLL_DENSE;

        /// <summary>
        /// Invalidate cached cardinality estimate
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetCard(byte* ptr, long card) => *((long*)(ptr + 8)) = card;

        /// <summary>
        /// Get cached cardinality estimate
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetCard(byte* ptr) => *((long*)(ptr + 8));

        /// <summary>
        /// Check if previously calculated cardinality has been invalidated by previous update
        /// </summary>
        /// <param name="ptr"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsValidCard(byte* ptr) => (GetCard(ptr) >= 0);

        /// <summary>
        /// Initialize HLL data structure
        /// </summary>
        /// <param name="input"></param>
        /// <param name="value"></param>
        /// <param name="vlen"></param>
        public void Init(byte* input, byte* value, int vlen)
        {
            int count = *(int*)(input);
            if (vlen != this.DenseBytes)//Sparse representation
            {
                InitSparse(value);
                IterateUpdateSparse(input, count, value);
            }
            else
            {
                InitDense(value);
                IterateUpdateDense(input, count, value);
            }
        }

        /// <summary>
        /// Initialize sparse blob
        /// </summary>
        public void InitSparse(byte* ptr)
        {
            SetPrefix(ptr);
            SetType(ptr, (byte)HLL_DTYPE.HLL_SPARSE);
            SetCard(ptr, long.MinValue);

            ushort ranges = (ushort)SparseZeroRanges;
            SetSparseRLESize(ptr, ranges); //Initial bytes for zero representation//
            byte* regs = ptr + hll_header_bytes + 2;

            //initialize to represent small zero
            for (int i = 0; i < ranges; i++) regs[i] = 0xFF;
        }

        /// <summary>
        /// Initialize dense blob
        /// </summary>  
        public void InitDense(byte* ptr)
        {
            for (int i = 0; i < DenseBytes; i++) ptr[i] = 0x0;

            SetPrefix(ptr);
            SetType(ptr, (byte)HLL_DTYPE.HLL_DENSE);
            SetCard(ptr, long.MinValue);
        }

        /// <summary>
        /// Initial length for HLL based on inserted value count from input
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public int SparseInitialLength(byte* input)
        {
            int count = *(int*)(input);//get count of elements in sequence
            return SparseInitialLength(count);
        }

        private int SparseInitialLength(int count)
        {
            int requiredBytes = SparseRequiredBytes(count);// get bytes for elements
            //if total bytes required greater than max cap switch to dense
            //else calculate additional spase neede apart from default.
            return (SparseHeaderSize + requiredBytes) > SparseSizeMaxCap ? this.DenseBytes :
                ((requiredBytes < SparseZeroRanges + SparseMemorySectorSize) ?
                    SparseBytes :
                    SparseHeaderSize + requiredBytes);
        }

        /// <summary>
        /// Required space allocation for given count of inserted values
        /// </summary>
        /// <param name="cnt"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int SparseRequiredBytes(int cnt)
        {
            int usedBytes = cnt * SparseMaxBytesPerInsert;
            int pageCount = ((usedBytes - 1) / SparseMemorySectorSize) + 1;
            return pageCount * SparseMemorySectorSize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int SparseCurrentSizeInBytes(byte* ptr) => SparseHeaderSize + GetSparseRLESize(ptr);

        /// <summary>
        /// Check if allocated space is enough for [count] elements.
        /// </summary>
        /// <param name="value"></param>
        /// <param name="valueLen"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool CanGrowInPlace(byte* value, int valueLen, int count) => SparseCurrentSizeInBytes(value) + (SparseMaxBytesPerInsert * count) < valueLen;

        /// <summary>
        /// Return length of new value
        /// </summary>        
        public int UpdateGrow(byte* input, byte* value)
        {
            int count = *(int*)(input);
            if (IsSparse(value))
            {
                //calculate additional sparse needed and check if we are allowed to grow to that size based of the max-cap-size
                int sparseBlobBytes = SparseCurrentSizeInBytes(value) + SparseRequiredBytes(count);
                return sparseBlobBytes < SparseSizeMaxCap ? sparseBlobBytes : this.DenseBytes;
            }

            if (IsDense(value))
                return this.DenseBytes;

            throw new GarnetException("HyperLogLog UpdateGrowV2 invalid data structure type");
        }

        /// <summary>
        /// Calculate growth for merge. 
        /// </summary>
        /// <param name="srcHLL"></param>
        /// <param name="dstHLL"></param>        
        /// <returns></returns>
        public int MergeGrow(byte* srcHLL, byte* dstHLL)
        {
            byte dstType = GetType(dstHLL);
            byte srcType = GetType(srcHLL);
            if (dstType == (byte)HLL_DTYPE.HLL_SPARSE && srcType == (byte)(HLL_DTYPE.HLL_SPARSE))
            {
                int srcNonZeroBytes = SparseCountNonZero(srcHLL) * SparseMaxBytesPerInsert;
                int pageCount = (((srcNonZeroBytes - 1) / SparseMemorySectorSize) + 1);//Get an extra allocation for future growth
                int sparseBlobBytes = SparseCurrentSizeInBytes(dstHLL) + pageCount * SparseMemorySectorSize;
                return sparseBlobBytes < SparseSizeMaxCap ? sparseBlobBytes : this.DenseBytes;
            }
            else
                return this.DenseBytes;
        }

        /// <summary>
        /// Merge operation triggered growth. First copy/transform oldHLL to newHLL and then merge srcHLL to newHLL.
        /// </summary>
        /// <param name="srcHLLPtr"></param>
        /// <param name="oldDstHLLPtr"></param>
        /// <param name="newDstHLLPtr"></param>
        /// <param name="oldValueLen"></param>
        /// <param name="newValueLen"></param>
        public void CopyUpdateMerge(byte* srcHLLPtr, byte* oldDstHLLPtr, byte* newDstHLLPtr, int oldValueLen, int newValueLen)
        {
            if (oldValueLen == newValueLen)
                Buffer.MemoryCopy(oldDstHLLPtr, newDstHLLPtr, oldValueLen, oldValueLen);
            else
            {
                if (newValueLen == this.DenseBytes)
                    InitDense(newDstHLLPtr);
                else
                    InitSparse(newDstHLLPtr);
                Merge(oldDstHLLPtr, newDstHLLPtr);
            }
            Merge(srcHLLPtr, newDstHLLPtr);
            SetCard(newDstHLLPtr, long.MinValue);
        }

        /// <summary>
        /// Main Copy update used for growing sparse to sparse or dense
        /// </summary>
        /// <param name="input"></param>
        /// <param name="oldValue"></param>
        /// <param name="newValue"></param>
        /// <param name="newValueLen"></param>
        /// <returns></returns>
        public bool CopyUpdate(byte* input, byte* oldValue, byte* newValue, int newValueLen)
        {
            bool fUpdated = false;
            int count = *(int*)(input);
            //Only reach this point if old-blob is of sparse type
            if (IsSparse(oldValue))
            {
                if (newValueLen == this.DenseBytes)//We are upgrading to dense representation here
                {
                    InitDense(newValue);
                    fUpdated |= SparseToDense(oldValue, newValue);
                    fUpdated |= IterateUpdateDense(input, count, newValue);
                    return fUpdated;
                }
                else//We are upgrading to a bigger size sparse representation
                {
                    InitSparse(newValue);
                    int sparseBlobBytes = SparseCurrentSizeInBytes(oldValue);
                    Buffer.MemoryCopy(oldValue, newValue, sparseBlobBytes, sparseBlobBytes);
                    fUpdated = IterateUpdateSparse(input, count, newValue);
                }
                return fUpdated;
            }
            throw new GarnetException("HyperLogLog Update invalid data structure type");
        }

        /// <summary>
        /// Copy oldValue (sparse) to newValue (dense) and insert new hash-value (hv)
        /// </summary>
        /// <param name="hv"></param>
        /// <param name="oldValue"></param>
        /// <param name="newValue"></param>
        /// <returns></returns>
        private bool SparseToDenseCopy(long hv, byte* oldValue, byte* newValue)
        {
            bool fUpdated = false;
            InitDense(newValue);
            fUpdated |= SparseToDense(oldValue, newValue);
            fUpdated |= UpdateDense(newValue, hv);
            return fUpdated;
        }

        /// <summary>
        /// Copy oldValue (sparse) to newValue (sparse) and insert new hash-value (hv)
        /// </summary>
        /// <param name="hv"></param>
        /// <param name="oldValue"></param>
        /// <param name="newValue"></param>
        /// <returns></returns>
        private bool SparseToSparseCopy(long hv, byte* oldValue, byte* newValue)
        {
            int sparseBlobBytes = SparseCurrentSizeInBytes(oldValue);
            Buffer.MemoryCopy(oldValue, newValue, sparseBlobBytes, sparseBlobBytes);
            return UpdateSparse(newValue, hv);
        }

        /// <summary>
        /// Merge denseBlobA to denseBlobB
        /// </summary>
        /// <param name="srcDenseBlob"></param>
        /// <param name="dstDenseBlob"></param>
        public bool DenseToDense(byte* srcDenseBlob, byte* dstDenseBlob)
        {
            bool fUpdated = false;
            byte* srcRegs = srcDenseBlob + hll_header_bytes;
            byte* dstRegs = dstDenseBlob + hll_header_bytes;
            for (ushort idx = 0; idx < this.mcnt; idx++)
            {
                byte srcLZ = _get_register(srcRegs, idx);
                byte dstLZ = _get_register(dstRegs, idx);
                if (srcLZ > dstLZ)
                {
                    _set_register(dstRegs, idx, srcLZ);
                    fUpdated = true;
                }
            }
            SetCard(dstDenseBlob, long.MinValue);
            return fUpdated;
        }

        /// <summary>
        /// Main multi value update method
        /// </summary>
        /// <param name="input"></param>
        /// <param name="value"></param>
        /// <param name="valueLen"></param>
        /// <param name="updated"></param>
        /// <returns></returns>           
        public bool Update(byte* input, byte* value, int valueLen, ref bool updated)
        {
            int count = *(int*)(input);

            if (IsDense(value))// if blob layout is dense
            {
                updated = IterateUpdateDense(input, count, value);
                return true;
            }

            if (IsSparse(value))// if blob layout is sparse
            {
                if (CanGrowInPlace(value, valueLen, count))//check if we can grow in place
                {
                    updated = IterateUpdateSparse(input, count, value);
                    return true;
                }
                else return false;// need to request for more space
            }
            throw new GarnetException("Update HyperLogLog Error!");
        }

        private bool IterateUpdateDense(byte* input, int count, byte* value)
        {
            bool updated = false;
            byte* hash_value_vector = input + sizeof(int); //4 byte count + hash values
            for (int i = 0; i < count; i++)
            {
                long hv = *(long*)hash_value_vector;
                updated |= UpdateDense(value, hv);
                hash_value_vector += 8;
            }
            return updated;
        }

        /// <summary>
        /// Update dense HypperLogLog structure
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool UpdateDense(byte* ptr, long hv)
        {
            ushort idx = RegIdx(hv);
            byte cntlz = clz(hv);

            Debug.Assert(idx < mcnt);
            Debug.Assert(cntlz < qbit + 1);

            return UpdateDenseRegister(ptr, idx, cntlz);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool UpdateDenseRegister(byte* ptr, ushort idx, byte cntlz)
        {
            byte* regs = ptr + hll_header_bytes;
            if (cntlz > _get_register(regs, idx))
            {
                SetCard(ptr, long.MinValue);//invalidate previously calculated cardinality                
                _set_register(regs, idx, cntlz);
                return true;
            }
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SetSparseRLESize(byte* ptr, ushort size) => *(ushort*)(ptr + hll_header_bytes) = size;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ushort GetSparseRLESize(byte* ptr) => *(ushort*)(ptr + hll_header_bytes);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsZeroRange(byte* p) => (((*p) & 0x80) != 0); // 1xxx xxxx

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int ZeroRangeLen(byte* p) => (((*p) & 0x7F) + 1); // 1xxx xxxx 

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ZeroRangeSet(byte* p, byte len) => *p = (byte)((len - 1) | 0x80); // 1xxx xxxx                

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private byte GetNonZero(byte* p) => (byte)(((*p) & 0x7F) + 1);// 0vvv vvvv

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SetNonZero(byte* p, byte cnt) => *p = (byte)(cnt - 1); // 0vvv vvvv        

        private bool IterateUpdateSparse(byte* input, int count, byte* value)
        {
            bool updated = false;
            byte* hash_value_vector = input + sizeof(int); // 4 byte count + hash values
            for (int i = 0; i < count; i++)
            {
                long hv = *(long*)hash_value_vector;
                updated |= UpdateSparse(value, hv);
                hash_value_vector += 8;
            }
            return updated;
        }

        /// <summary>
        /// Update sparse representation
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool UpdateSparse(byte* ptr, long hv)
        {
            ushort idx = RegIdx(hv);
            byte cntlz = clz(hv);
            bool fUpdated = UpdateSparseReg(ptr, idx, cntlz);
            if (fUpdated)
                SetCard(ptr, long.MinValue);//invalidate previously calculated cardinality                
            return fUpdated;
        }

        /// <summary>
        /// Update sparse representation
        /// </summary>
        private bool UpdateSparseReg(byte* ptr, ushort idx, byte cntlz)
        {
            ushort rleSize = GetSparseRLESize(ptr);

            byte* curr = ptr + SparseHeaderSize; // start of sparse representation
            byte* end = curr + rleSize;// end of sparse representation

            byte* prev = null;//prev opcode in sequence
            byte* next = null;//next opcode in sequence

            int clen = 0;//length covered by opcode
            int offset = 0;//offset within sequence            

            //1. find position of range that covers idx
            while (curr < end)
            {
                clen = IsZeroRange(curr) ? ZeroRangeLen(curr) : 1;
                if (idx <= offset + clen - 1) break;
                prev = curr++;
                offset += clen;
            }

            next = curr + 1;
            bool is_val = !IsZeroRange(curr);

            //2. If we enter here the curr pointers points to val opcode that can be updated in-place or return no update
            if (is_val)
            {
                byte lz = GetNonZero(curr);

                //No update
                if (cntlz <= lz) return false;

                //Update
                SetNonZero(curr, cntlz);
                SetCard(ptr, long.MinValue);//invalidate previously calculated cardinality                
                return true;
            }

            //3. Here split zero opcode
            byte* buf = stackalloc byte[3];
            byte* cbuf = buf;
            int roffset = offset + clen - 1;
            if (offset != idx)//build left opcode if idx not at beginning of range
            {
                int zrange = idx - offset;
                ZeroRangeSet(cbuf, (byte)zrange);
                cbuf++;
            }

            SetNonZero(cbuf, cntlz);
            cbuf++;

            if (roffset != idx)// build right opcode if idx not at ending of range
            {
                int zrange = roffset - idx;
                ZeroRangeSet(cbuf, (byte)zrange);
                cbuf++;
            }

            //4. Copy suffix
            int blen = (int)(cbuf - buf);
            int suffixlen = (int)(end - next) + 1;//            

            Buffer.MemoryCopy(next, next + blen - 1, suffixlen, suffixlen);
            Buffer.MemoryCopy(buf, curr, blen, blen);

            SetSparseRLESize(ptr, (ushort)(rleSize + blen - 1));

            SetCard(ptr, long.MinValue);//invalidate previously calculated cardinality                
            return true;
        }

        /// <summary>
        /// Main Count HLL method
        /// </summary>
        public long Count(byte* ptr)
        {
            byte dType = GetType(ptr);

            //Return cached cardinality if not invalidated
            if (IsValidCard(ptr))
            {
                return GetCard(ptr);
            }
            var E = (HLL_DTYPE)dType switch
            {
                HLL_DTYPE.HLL_SPARSE => CountSparseNCEstimator(ptr),
                HLL_DTYPE.HLL_DENSE => CountDenseNCEstimator(ptr),
                _ => throw new GarnetException("HyperLogLog Count invalid data structure type"),
            };
            SetCard(ptr, E);
            return E;
        }

        private static double cTau(double x)
        {
            if (x == 0.0 || x == 1.0) return 0.0;
            double _z;
            double y = 1.0;
            double z = 1 - x;
            do
            {
                x = Math.Sqrt(x);
                _z = z;
                y *= 0.5;
                z -= Math.Pow(1 - x, 2) * y;
            } while (_z != z);

            return z / (double)3;
        }

        private static double cSigma(double x)
        {
            if (x == 1.0) return double.PositiveInfinity;
            double _z;
            double y = 1;
            double z = x;

            do
            {
                x *= x;
                _z = z;
                z += x * y;
                y += y;
            } while (_z != z);

            return z;
        }

        /// <summary>
        /// No correction for large values sparse estimator
        /// </summary>
        /// <param name="ptr"></param>
        /// <returns></returns>
        private long CountSparseNCEstimator(byte* ptr)
        {
            int* rhisto = stackalloc int[64];
            ushort rleSize = GetSparseRLESize(ptr);

            byte* curr = ptr + SparseHeaderSize; // start of sparse representation
            byte* end = curr + rleSize;// end of sparse representation
            byte lz = 0;
            int clen = 0;//length covered by opcode
            int offset = 0;//offset within sequence

            for (int i = 0; i < 64; i++) rhisto[i] = 0;
            //rhisto[0] = 0; rhisto[1] = 0; rhisto[2] = 0; rhisto[3] = 0; rhisto[4] = 0; rhisto[5] = 0; rhisto[6] = 0; rhisto[7] = 0;
            //rhisto[8] = 0; rhisto[9] = 0; rhisto[10] = 0; rhisto[10] = 0; rhisto[11] = 0; rhisto[12] = 0; rhisto[13] = 0; rhisto[14] = 0;
            //rhisto[16] = 0; rhisto[17] = 0; rhisto[18] = 0; rhisto[19] = 0; rhisto[20] = 0; rhisto[21] = 0; rhisto[22] = 0; rhisto[23] = 0;
            //rhisto[24] = 0; rhisto[25] = 0; rhisto[26] = 0; rhisto[27] = 0; rhisto[28] = 0; rhisto[29] = 0; rhisto[30] = 0; rhisto[31] = 0;
            //rhisto[32] = 0; rhisto[33] = 0; rhisto[34] = 0; rhisto[35] = 0; rhisto[36] = 0; rhisto[37] = 0; rhisto[38] = 0; rhisto[38] = 0;
            //rhisto[40] = 0; rhisto[41] = 0; rhisto[42] = 0; rhisto[43] = 0; rhisto[44] = 0; rhisto[45] = 0; rhisto[46] = 0; rhisto[47] = 0;
            //rhisto[48] = 0; rhisto[49] = 0; rhisto[50] = 0; rhisto[51] = 0; rhisto[52] = 0; rhisto[53] = 0; rhisto[54] = 0; rhisto[55] = 0;
            //rhisto[56] = 0; rhisto[57] = 0; rhisto[58] = 0; rhisto[59] = 0; rhisto[60] = 0; rhisto[61] = 0; rhisto[62] = 0; rhisto[63] = 0;            

            while (curr != end)
            {
                bool iszero = IsZeroRange(curr);
                clen = iszero ? ZeroRangeLen(curr) : 1;

                //get lz count
                lz = iszero ? (byte)0 : GetNonZero(curr);

                //if nonzero increment at lz count pos by 1 else increment by clen
                rhisto[lz] += clen;

                offset += clen;
                curr++;
            }

            //for (int i = 0; i < 64; i++) Console.WriteLine("{0} = {1}", i, rhisto[i]);

            double E;//estimate            
            double z = mcnt * cTau((mcnt - rhisto[qbit + 1]) / (double)mcnt);

            for (int j = qbit; j >= 1; --j)
            {
                z += rhisto[j];
                z *= 0.5;
            }
            z += mcnt * cSigma(rhisto[0] / (double)mcnt);
            E = alpha * mcnt * mcnt / z;

            return (long)Math.Round(E);
        }

        /// <summary>
        /// No correction for large values dense estimator
        /// </summary>
        /// <param name="ptr"></param>
        /// <returns></returns>
        private long CountDenseNCEstimator(byte* ptr)
        {
            int* rhisto = stackalloc int[64];
            byte* regs = ptr + hll_header_bytes;

            for (int i = 0; i < 64; i++) rhisto[i] = 0;

            //if (this.mcnt == 16384)
            {
                byte r00, r01, r02, r03, r04, r05, r06, r07;
                byte r08, r09, r10, r11, r12, r13, r14, r15;

                int end = this.mcnt >> 4; // this.mcnt / 16
                //unpack 6-bit registers
                for (int j = 0; j < end; j++)
                {
                    r00 = (byte)(regs[0] & 63);
                    r01 = (byte)(((regs[0] >> 6) | (regs[1] << 2)) & 63);
                    r02 = (byte)(((regs[1] >> 4) | (regs[2] << 4)) & 63);
                    r03 = (byte)((regs[2] >> 2) & 63);

                    r04 = (byte)(regs[3] & 63);
                    r05 = (byte)(((regs[3] >> 6) | (regs[4] << 2)) & 63);
                    r06 = (byte)(((regs[4] >> 4) | (regs[5] << 4)) & 63);
                    r07 = (byte)((regs[5] >> 2) & 63);

                    r08 = (byte)(regs[6] & 63);
                    r09 = (byte)(((regs[6] >> 6) | (regs[7] << 2)) & 63);
                    r10 = (byte)(((regs[7] >> 4) | (regs[8] << 4)) & 63);
                    r11 = (byte)((regs[8] >> 2) & 63);

                    r12 = (byte)(regs[9] & 63);
                    r13 = (byte)(((regs[9] >> 6) | (regs[10] << 2)) & 63);
                    r14 = (byte)(((regs[10] >> 4) | (regs[11] << 4)) & 63);
                    r15 = (byte)((regs[11] >> 2) & 63);

                    rhisto[r00]++; rhisto[r01]++; rhisto[r02]++; rhisto[r03]++;
                    rhisto[r04]++; rhisto[r05]++; rhisto[r06]++; rhisto[r07]++;
                    rhisto[r08]++; rhisto[r09]++; rhisto[r10]++; rhisto[r11]++;
                    rhisto[r12]++; rhisto[r13]++; rhisto[r14]++; rhisto[r15]++;

                    regs += 12;
                }
            }

            //for (int i = 0; i < 64; i++) Console.WriteLine("{0} = {1}", i, rhisto[i]);

            double E;//estimate            
            double z = mcnt * cTau((mcnt - rhisto[qbit + 1]) / (double)mcnt);

            for (int j = qbit; j >= 1; --j)
            {
                z += rhisto[j];
                z *= 0.5;
            }
            z += mcnt * cSigma(rhisto[0] / (double)mcnt);
            E = alpha * mcnt * mcnt / z;

            return (long)Math.Round(E);
        }

        /// <summary>
        /// TryMerge - fails if cannot update in place
        /// </summary>
        /// <param name="srcBlob"></param>
        /// <param name="dstBlob"></param>
        /// <param name="dstLen"></param>
        /// <returns></returns>
        public bool TryMerge(byte* srcBlob, byte* dstBlob, int dstLen)
        {
            byte dTypeDst = GetType(dstBlob);
            if (dTypeDst == (byte)HLL_DTYPE.HLL_DENSE)//destination dense
            {
                Merge(srcBlob, dstBlob);
                DefaultHLL.SetCard(dstBlob, long.MinValue);
                return true;
            }
            else// destination sparse
            {
                byte dTypeSrc = GetType(srcBlob);
                if (dTypeSrc == (byte)HLL_DTYPE.HLL_SPARSE)// discover if you need to grow before merging
                {
                    //TODO: check if we can update in place or need to grow by counting srcBlob non-zero
                    int srcNonZeroBytes = SparseCountNonZero(srcBlob) * 2;

                    if (SparseCurrentSizeInBytes(dstBlob) + srcNonZeroBytes < dstLen)//can grow in-place
                    {
                        Merge(srcBlob, dstBlob);
                        DefaultHLL.SetCard(dstBlob, long.MinValue);
                        return true;
                    }
                    else
                        return false;
                }
                else return false; // always fail if merging from dense to sparse              
            }
            throw new GarnetException("TryMerge exception");
        }

        /// <summary>
        /// Merge hll srcBlob to hll dstBlob
        /// </summary>
        public bool Merge(byte* srcBlob, byte* dstBlob)
        {
            byte dTypeSrc = GetType(srcBlob);
            byte dTypeDst = GetType(dstBlob);

            //Copy destination dense HLL, do not need to grow memory
            if (dTypeDst == (byte)HLL_DTYPE.HLL_DENSE)
            {
                if (dTypeSrc == (byte)HLL_DTYPE.HLL_SPARSE)//Sparse
                {
                    return SparseToDense(srcBlob, dstBlob);
                }
                else//Dense
                {
                    return DenseToDense(srcBlob, dstBlob);
                }
            }

            if (dTypeDst == (byte)HLL_DTYPE.HLL_SPARSE)
            {
                //when dst of merge is sparse then src can only be from sparse, else we need to grow to dense.
                Debug.Assert(dTypeSrc == (byte)HLL_DTYPE.HLL_SPARSE);
                return SparseToSparse(srcBlob, dstBlob);
            }
            throw new GarnetException("Merge exception");
        }

        /// <summary>
        /// Transform sparse hll to dense hll
        /// </summary>
        public bool SparseToDense(byte* src, byte* dst)
        {
            bool fUpdated = false;

            ushort rleSize = GetSparseRLESize(src);
            byte* curr = src + SparseHeaderSize; // start of sparse representation
            byte* end = curr + rleSize;// end of sparse representation            
            int offset = 0;//offset within sequence

            while (curr != end)
            {
                bool iszero = IsZeroRange(curr);

                if (!iszero)
                {
                    byte lz = GetNonZero(curr);
                    fUpdated |= UpdateDenseRegister(dst, (ushort)offset, lz);
                }

                offset += iszero ? ZeroRangeLen(curr) : 1;
                curr++;
            }
            return fUpdated;
        }

        /// <summary>
        /// Merge sparseSrc to SparseDst
        /// </summary>
        /// <param name="sparseSrc"></param>
        /// <param name="sparceDst"></param>
        /// <returns></returns>
        public bool SparseToSparse(byte* sparseSrc, byte* sparceDst)
        {
            bool fUpdated = false;
            byte* currSrc = sparseSrc + SparseHeaderSize; // start of sparse representation
            byte* endSrc = currSrc + GetSparseRLESize(sparseSrc);// end of sparse representation            
            int offset = 0;//offset within sequence

            while (currSrc != endSrc)
            {
                bool iszero = IsZeroRange(currSrc);
                int clen = iszero ? ZeroRangeLen(currSrc) : 1;

                if (!iszero)
                {
                    byte lz = GetNonZero(currSrc);
                    fUpdated |= UpdateSparseReg(sparceDst, (ushort)offset, lz);
                }

                offset += clen;
                currSrc++;
            }

            return fUpdated;
        }

        /// <summary>
        /// Count zeros in dense
        /// </summary>
        /// <param name="ptr"></param>
        /// <returns></returns>
        private int DenseCountNonZero(byte* ptr)
        {
            int cnt = 0;
            byte* regs = ptr + hll_header_bytes;

            for (int idx = 0; idx < this.mcnt; idx++)
            {
                byte lz = _get_register(regs, (ushort)idx);
                cnt += lz == 0 ? 1 : 0;
            }

            return cnt;
        }

        /// <summary>
        /// Count zeros in sparse
        /// </summary>
        /// <param name="ptr"></param>
        /// <returns></returns>
        private int SparseCountNonZero(byte* ptr)
        {
            ushort rleSize = GetSparseRLESize(ptr);
            byte* curr = ptr + SparseHeaderSize; // start of sparse representation
            byte* end = curr + rleSize;// end of sparse representation            
            int offset = 0;//offset within sequence
            int cnt = 0;

            while (curr != end)
            {
                bool iszero = IsZeroRange(curr);

                cnt += !iszero ? 1 : 0;
                offset += iszero ? ZeroRangeLen(curr) : 1;
                curr++;
            }

            return cnt;
        }

#if DEBUG
        private void DumpSparseRawBytes(byte* ptr)
        {
            Console.WriteLine("[0x00000000x0]");
            int usedBytesInBlob = GetSparseRLESize(ptr) + SparseHeaderSize;
            int len = usedBytesInBlob >> 3;
            byte* t = ptr;
            int count = 1;

            for (int i = 0; i < len; i++)
            {
                byte v = *t;

                Console.Write(count.ToString("D3") + ": ");
                Console.Write("0x" + v.ToString("X2") + " "); t++; v = *t;
                Console.Write("0x" + v.ToString("X2") + " "); t++; v = *t;
                Console.Write("0x" + v.ToString("X2") + " "); t++; v = *t;
                Console.Write("0x" + v.ToString("X2") + " "); t++; v = *t;
                Console.Write("0x" + v.ToString("X2") + " "); t++; v = *t;
                Console.Write("0x" + v.ToString("X2") + " "); t++; v = *t;
                Console.Write("0x" + v.ToString("X2") + " "); t++; v = *t;
                Console.Write("0x" + v.ToString("X2") + " "); t++; v = *t;

                Console.WriteLine("");
                count++;
            }

            Console.Write(count.ToString("D3") + ": ");
            int tail = usedBytesInBlob & 7;
            for (int i = tail - 1; i >= 0; i--)
            {
                byte v = *t;
                Console.Write("0x" + v.ToString("X2") + " "); t++;
            }
            Console.WriteLine("");

        }

        /// <summary>
        /// Dump HLL structure raw bytes
        /// </summary>
        /// <param name="ptr"></param>
        public void DumpRawBytes(byte* ptr)
        {
            byte dType = GetType(ptr);
            switch ((HLL_DTYPE)dType)
            {
                case HLL_DTYPE.HLL_SPARSE:
                    DumpSparseRawBytes(ptr);
                    break;
                case HLL_DTYPE.HLL_DENSE:
                    throw new GarnetException("Unimplemented...");
                    //break;
            }
        }

        /// <summary>
        /// Used for debugging to compare dense and sparse HLL
        /// </summary>
        /// <param name="denseHLL"></param>
        /// <param name="sparseHLL"></param>
        public void CompareSparseToDense(byte* denseHLL, byte* sparseHLL)
        {
            Dictionary<int, int> denseRegs = new();
            byte* regs = denseHLL + hll_header_bytes;

            for (uint i = 0; i < this.mcnt; i++)
            {
                ushort idx = (ushort)i;
                byte lz = _get_register(regs, idx);
                if (lz != 0)
                    denseRegs.Add(idx, lz);
            }

            ushort rleSize = GetSparseRLESize(sparseHLL);
            byte* curr = sparseHLL + SparseHeaderSize; // start of sparse representation
            byte* end = curr + rleSize; // end of sparse representation
            int offset = 0; // offset within sequence

            while (curr != end)
            {
                bool iszero = IsZeroRange(curr);
                int clen = iszero ? ZeroRangeLen(curr) : 1; // length covered by opcode

                if (!iszero)
                {
                    int val = GetNonZero(curr);
                    if (!denseRegs.ContainsKey(offset))
                    {
                        Console.WriteLine("FAILED: Nonzero not contained: {0} = {1}", offset, val);
                        Environment.Exit(1);
                    }

                    if (denseRegs[offset] != val)
                    {
                        Console.WriteLine("FAILED: Nonzero wrong nonzero value: {0} = {1}", offset, val);
                        Environment.Exit(1);
                    }
                }
                offset += clen;
                curr++;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ptr"></param>
        public void DumpRegs(byte* ptr)
        {
            byte dType = GetType(ptr);
            switch ((HLL_DTYPE)dType)
            {
                case HLL_DTYPE.HLL_SPARSE:
                    DumpSparseRegs(ptr);
                    break;
                case HLL_DTYPE.HLL_DENSE:
                    DumpDenseRegs(ptr);
                    break;
            }
        }

        /// <summary>
        /// Dump dense reg contents to console
        /// </summary>
        public void DumpDenseRegs(byte* denseHLL)
        {
            Console.WriteLine("-------[Dumping Dense Reg Content]-------");
            byte* regs = denseHLL + hll_header_bytes;
            for (uint i = 0; i < this.mcnt; i++)
            {
                ushort idx = (ushort)i;
                byte lz = _get_register(regs, idx);
                if (lz != 0)
                    Console.WriteLine("{0} = {1}", idx, _get_register(regs, idx));
            }
        }

        /// <summary>
        /// Dump sparse reg contents to console
        /// </summary>
        public void DumpSparseRegs(byte* sparseHLL, Dictionary<int, int> data = null)
        {
            if (data == null) Console.WriteLine("-------[Dumping Sparse Reg Content]-------");
            ushort rleSize = GetSparseRLESize(sparseHLL);

            byte* curr = sparseHLL + SparseHeaderSize; // start of sparse representation
            byte* end = curr + rleSize; // end of sparse representation
            int offset = 0; // offset within sequence

            int nonzeroCount = 0;
            while (curr != end)
            {
                bool iszero = IsZeroRange(curr);
                int clen = iszero ? ZeroRangeLen(curr) : 1; // length covered by opcode

                if (data == null)
                {
                    if (iszero)
                    {
                        //Console.WriteLine("ZeroRange: {0}-{1}", offset, offset + clen);
                    }
                    else
                    {
                        Console.WriteLine("{0} = {1}", offset, GetNonZero(curr));
                    }
                }
                else
                {
                    if (!iszero)
                    {
                        int val = GetNonZero(curr);
                        if (!data.ContainsKey(offset))
                        {
                            Console.WriteLine("FAILED: Nonzero not contained: {0} = {1}", offset, val);
                            Environment.Exit(1);
                        }

                        if ((data[offset] + 1) != val)
                        {
                            Console.WriteLine("FAILED: Nonzero wrong nonzero value: {0} = {1}", offset, val);
                            Environment.Exit(1);
                        }

                        nonzeroCount++;
                    }
                }

                offset += clen;
                curr++;
            }

            if (data != null && data.Keys.Count != nonzeroCount)
            {
                Console.WriteLine("FAILED: nonzeroCount differs {0},{1}", data.Keys.Count, nonzeroCount);
                Environment.Exit(1);
            }
        }
#endif
    }
}