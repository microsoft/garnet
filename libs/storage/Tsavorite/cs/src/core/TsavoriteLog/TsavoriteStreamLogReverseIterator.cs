//namespace Tsavorite.core;

//using System.Diagnostics;
//using Microsoft.Extensions.Logging;

//using EmptyStoreFunctions = StoreFunctions<Empty, byte, EmptyKeyComparer, DefaultRecordDisposer<Empty, byte>>;

//// Currently works by having the a linked list for previous address scanning. This is adding 8 bytes per entry, and I need to get rid of this.

//// I don't believe this needs to be concurrency control aware since we only go backwards on entries with knowledge of their addresses.
//internal class TsavoriteStreamLogReverseIterator : TsavoriteLogScanIterator
//{
//  internal TsavoriteStreamLogReverseIterator(TsavoriteLog tsavoriteLog, BlittableAllocatorImpl<Empty, byte, EmptyStoreFunctions> hlog, long beginAddress, long endAddress,
//          GetMemory getMemory, ScanBufferingMode scanBufferingMode, LightEpoch epoch, int headerSize, bool scanUncommitted = false, ILogger logger = null)
//      : base(tsavoriteLog, hlog, beginAddress, endAddress, getMemory, scanBufferingMode, epoch, headerSize, scanUncommitted, logger)
//  { }

//  /// <summary>
//  /// Retrieve physical address of next iterator value. Since this is a reverse itertor this means that the next one is actuall the previous one in log order.
//  /// (under epoch protection if it is from main page buffer)
//  /// </summary>
//  /// <param name="physicalAddress"></param>
//  /// <param name="entryLength"></param>
//  /// <param name="currentAddress"></param>
//  /// <param name="outNextAddress"></param>
//  /// <param name="commitRecord"></param>
//  /// <param name="onFrame"></param>
//  /// <returns></returns>
//  protected override unsafe bool GetNextInternal(out long physicalAddress, out int entryLength, out long currentAddress, out long outNextAddress, out bool commitRecord, out bool onFrame)
//  {
//    Debug.Assert(!tsavoriteLog.readOnlyMode, "Reverse stream iterator not supported in read-only mode");
//    Debug.Assert(tsavoriteLog.logChecksum == LogChecksumType.None, "Reverse stream iterator does not support logs with checksums");

//    while (true)
//    {
//      physicalAddress = 0;
//      entryLength = 0;
//      currentAddress = nextAddress;
//      outNextAddress = currentAddress;
//      commitRecord = false; // reverse iterator never reads commit records so this is always false, but we keep it for signature compatibility
//      onFrame = false;

//      var headAddress = allocator.HeadAddress;
//      if (currentAddress < allocator.BeginAddress ||  // Check for boundary conditions. This is basically someone asking for something before the start of log. So we can say false
//        (allocator.IsNullDevice && currentAddress < headAddress) || // it also may be the case where someone is asking for something before the head address in null device mode.
//        currentAddress < endAddress || // we have gone past the end address we were supposed to scan back till
//        disposed)
//      {
//        return false;
//      }

//      // let's say you are trying to scan back from an address in the uncomitted range but you said false to scanUncommitted.
//      // then we need to jump back to the committed until address
//      if (!scanUncommitted && currentAddress >= tsavoriteLog.CommittedUntilAddress)
//      {
//        // This seems questionable at best. HK TODO: Review this logic.
//        currentAddress = tsavoriteLog.CommittedUntilAddress;
//      }

//      var currentPage = currentAddress >> allocator.LogPageSizeBits;
//      var currentFrame = currentPage % frameSize;
//      var currentOffset = currentAddress & allocator.PageSizeMask;

//      // are we below head address? We need to BufferAndLoad
//      if (currentAddress < headAddress)
//      {
//        var endAddr = endAddress;
//        // reverse iterator uses single page buffering only. So BufferAndLoad will always load just one page.
//        // We can later optimize this to load multiple pages if needed. Like we do double buffering in forward iterator.
//        if (BufferAndLoad(currentAddress, currentPage, currentFrame, headAddress, endAddr))
//          continue;

//        physicalAddress = frame.GetPhysicalAddress(currentFrame, currentOffset);
//        onFrame = true;
//      }
//      else
//      {
//        // in main log buffer in memory already, no paging needed
//        physicalAddress = allocator.GetPhysicalAddress(currentAddress);
//      }

//      // Get and check entry length
//      entryLength = tsavoriteLog.GetLength((byte*)physicalAddress);

//      // EntryLength should never be zero or negative in a reverse iterator. This is because unlike forward iterator we know which address to jump to directly.
//      Debug.Assert(entryLength > 0, "Reverse iterator should never commit records. Or zeroed out entries should have been handled above.");

//      // parse out the previous address present in stream entries right after the header.
//      outNextAddress = *(long*)(physicalAddress + headerSize);

//      // Update nextAddress to point to the previous entry for the next iteration
//      nextAddress = outNextAddress;

//      // Return true to indicate we found a valid entry. If the previous address is invalid (0), the next call will return false.
//      return true;
//    }
//  }
//}