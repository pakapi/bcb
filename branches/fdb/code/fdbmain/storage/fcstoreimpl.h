#ifndef STORAGE_CSTOREIMPL_H
#define STORAGE_CSTOREIMPL_H

#include "ffilesig.h"
#include "fcstore.h"
#include "searchcond.h"
#include <glog/logging.h>
#include <stdint.h>
#include <string.h>
#include <functional>
#include <map>
#include <stdexcept>
#include <vector>
#include <utility>
#include <boost/shared_array.hpp>

class StringHashSet;
namespace fdb {

// pimpl object for FMainMemoryCStore.
// TODO: this class is not implemented so far.
class FMainMemoryCStoreImpl {
public:
  FMainMemoryCStoreImpl (TableType type, int maxTuples);
  ~FMainMemoryCStoreImpl();

  TableType _type;
  int _maxTuples;
  std::vector<FCStoreColumn> _columns;
  KeyCompareFunc *_compfunc;
};


// context object for callback function in disk dump
class DirectFileOutputStream;
class FMainMemoryBTree;
struct CStoreDumpContext {
  CStoreDumpContext(int fileId_, DirectFileOutputStream *fd_, char *buffer_, const FCStoreColumn &column, const FMainMemoryBTree &btree);
  ~CStoreDumpContext();
  void updateFileSignature(FFileSignature &signature, TableType tableType, int columnIndex) const;

  void prepareForNewPageUniform (); // for uncompressed/dictionary encoded file

  bool flushBufferIfNeeded(); // returns true if flushed
  void flushBuffer();

  bool flipPageIfNeeded(); // returns true if flipped
  void flipPage();

  void writeLeafPageHeader(int countInThisPage, bool lastSibling, int64_t beginningPos);
  void writePageHeader(int countInThisPage, bool lastSibling, int64_t beginningPos, int level, bool root, int entrySize);

  void writeLeafEntry(const char *entryData);
  void writeLeafEntryRLE(int runLength, const char *entryData);
  void writeRootEntryRLE(int64_t beginningPos, int pageId);

  int fileId;
  DirectFileOutputStream *fd;
  char *buffer;
  int bufferedPages;
  int currentPageId;
  int currentPageOffset;
  int64_t currentTuple;
  int64_t tupleCount;
  FCStoreColumn column;
  int leafEntrySize; // this is not same as column.maxLength with RLE/Dictionary encoding
  int entryInCurrentPage;
  int entryPerLeafPage;

  // for RLE
  int64_t runTotal;
  int64_t currentRunBeginningPos;
  int currentRunCount;
  const char *currentRunValue;
  std::vector<int64_t> pageBeginningPositions;

  // for Dictionary Encoding
  int dictionarySize;
  int dictionaryBits;
  unsigned char currentPackedByte;
  int currentBitOffset; // for 1bit-4bit dictionary
  char *dictionary;
  StringHashSet *dictionaryHashSet; // used for >4bit dictionary.

  // used for 1bit-4bit dictionary.
  // linearly search in small dictionary (this dictionary is very small. faster than map or unordered_map)
  int searchInSmallDictionary (const char* value);
  // used for >4bit dictionary. uses hashtable
  int searchInLargeDictionary (const char* value); 

  // for RLE/Dic
  int rootPageStart;
  int rootPageCount;
  int rootPageLevel;
};

// base implementation of FColumnReader.
// derived classes implement compression-specific methods
class FColumnReaderImpl : virtual public FColumnReader {
public:
  FColumnReaderImpl(FBufferPool *bufferpool, const FCStoreColumn &column, const FFileSignature &signature);
  virtual ~FColumnReaderImpl(){}

  const FCStoreColumn& getColumn() const {
    return _column;
  }
  std::string normalize(const std::string &str) const;

  void setSearchRange (const PositionRange &range) {
    _searchRanges.clear();
    _searchRangeSet = true;
    _searchRanges.push_back (range);
  }
  void setSearchRanges (const std::vector<PositionRange> &ranges) {
    _searchRangeSet = true;
    _searchRanges = ranges;
  }
  void clearSearchRanges () {
    _searchRangeSet = false;
  }

protected:
  void logSearchCond (const SearchCond &cond) const;
  std::string toDebugStr (const void *key) const;

  FBufferPool *_bufferpool;
  FCStoreColumn _column;
  FFileSignature _signature;
  bool _searchRangeSet;
  std::vector<PositionRange> _searchRanges;
};

class FColumnReaderImplUncompressed : public FColumnReaderImpl {
public:
  FColumnReaderImplUncompressed(FBufferPool *bufferpool, const FCStoreColumn &column, const FFileSignature &signature);

  // getPositionRanges() is not implemented for uncompressed column.
  // this will be very inefficient even if implemented for uncompressed column.
  void getPositionRanges (const SearchCond &cond, std::vector<PositionRange> &positions) {
    assert (false);
    throw std::runtime_error ("not implemented yet!");
  }

  void getPositionBitmaps (const SearchCond &cond, std::vector<boost::shared_ptr<PositionBitmap> > &positions);

  void getDecompressedData (const PositionRange &range, void *buffer, size_t bufferSize);
private:
  int _entriesPerPage;

  int processPageString(const SearchCond &cond, const char *cursor, size_t tuplesToRead, PositionBitmap *bitmap, int64_t bitmapPageOffset);

  // manual partial specialization...sorta. ugly. but faster.

  // for string. less parameters
  template <typename COMPARE>
  int processPageStringBinary(COMPARE func, const SearchCond &cond, const char *cursor, size_t tuplesToRead, PositionBitmap *bitmap, int64_t bitmapPageOffset) {
    int matchCount = 0;
    const int length = _column.maxLength;
    for (size_t i = 0; i < tuplesToRead; ++i, cursor += length) {
      int ret = ::memcmp (cursor, cond.key, length);
      if (func(ret, 0)) {
        bitmap->setBit(i + bitmapPageOffset);
        ++matchCount;
      }
    }
    return matchCount;
  }
  int processPageStringBetween(const SearchCond &cond, const char *cursor, size_t tuplesToRead, PositionBitmap *bitmap, int64_t bitmapPageOffset);
  int processPageStringIn(const SearchCond &cond, const char *cursor, size_t tuplesToRead, PositionBitmap *bitmap, int64_t bitmapPageOffset);


  // for ints
  template <typename INT_TYPE>
  int processPageInts(const SearchCond &cond, const char *cursor, size_t tuplesToRead, PositionBitmap *bitmap, int64_t bitmapPageOffset) {
    switch (cond.type) {
    case SCT_EQUAL:
      return processPageIntsBinary<INT_TYPE> (std::equal_to<INT_TYPE>(), cond, cursor, tuplesToRead, bitmap, bitmapPageOffset);
    case SCT_LT:
      return processPageIntsBinary<INT_TYPE> (std::less<INT_TYPE>(), cond, cursor, tuplesToRead, bitmap, bitmapPageOffset);
    case SCT_GT:
      return processPageIntsBinary<INT_TYPE> (std::greater<INT_TYPE>(), cond, cursor, tuplesToRead, bitmap, bitmapPageOffset);
    case SCT_LTEQ:
      return processPageIntsBinary<INT_TYPE> (std::less_equal<INT_TYPE>(), cond, cursor, tuplesToRead, bitmap, bitmapPageOffset);
    case SCT_GTEQ:
      return processPageIntsBinary<INT_TYPE> (std::greater_equal<INT_TYPE>(), cond, cursor, tuplesToRead, bitmap, bitmapPageOffset);
    case SCT_BETWEEN: return processPageIntsBetween<INT_TYPE> (cond, cursor, tuplesToRead, bitmap, bitmapPageOffset);
    case SCT_IN: return processPageIntsIn<INT_TYPE> (cond, cursor, tuplesToRead, bitmap, bitmapPageOffset);
    default:
      assert (false);
      return 0;
    }
  }

  template <typename INT_TYPE, typename COMPARE>
  int processPageIntsBinary(COMPARE func, const SearchCond &cond, const char *cursor, size_t tuplesToRead, PositionBitmap *bitmap, int64_t bitmapPageOffset) {
    const INT_TYPE key = *reinterpret_cast<const INT_TYPE*>(cond.key);
    int matchCount = 0;
    for (size_t i = 0; i < tuplesToRead; ++i, cursor += sizeof (INT_TYPE)) {
      if (func(*reinterpret_cast<const INT_TYPE*>(cursor), key)) {
        bitmap->setBit(i + bitmapPageOffset);
        ++matchCount;
      }
    }
    return matchCount;
  }
  template <typename INT_TYPE>
  int processPageIntsBetween(const SearchCond &cond, const char *cursor, size_t tuplesToRead, PositionBitmap *bitmap, int64_t bitmapPageOffset) {
    const INT_TYPE key = *reinterpret_cast<const INT_TYPE*>(cond.key);
    const INT_TYPE key2 = *reinterpret_cast<const INT_TYPE*>(cond.key2);
    int matchCount = 0;
    for (size_t i = 0; i < tuplesToRead; ++i, cursor += sizeof (INT_TYPE)) {
      INT_TYPE  d = *reinterpret_cast<const INT_TYPE*>(cursor);
      if (key <= d && d <= key2) {
        bitmap->setBit(i + bitmapPageOffset);
        ++matchCount;
      }
    }
    return matchCount;
  }
  template <typename INT_TYPE>
  int processPageIntsIn(const SearchCond &cond, const char *cursor, size_t tuplesToRead, PositionBitmap *bitmap, int64_t bitmapPageOffset) {
    int matchCount = 0;
    for (size_t i = 0; i < tuplesToRead; ++i, cursor += sizeof (INT_TYPE)) {
      if (cond.matchIntsIn<INT_TYPE>(*reinterpret_cast<const INT_TYPE*>(cursor))) {
        bitmap->setBit(i + bitmapPageOffset);
        ++matchCount;
      }
    }
    return matchCount;
  }
};

class FColumnReaderImplDictionary : public FColumnReaderImpl, virtual public FColumnReaderDictionary {
public:
  FColumnReaderImplDictionary(FBufferPool *bufferpool, const FCStoreColumn &column, const FFileSignature &signature);

  // getPositionRanges() is not implemented for dictionary compressed column that is inefficient for RLE.
  // this will be very inefficient even if implemented for dictionary compressed column.
  // in other words, this method is just for RLE.
  void getPositionRanges (const SearchCond &cond, std::vector<PositionRange> &positions) {
    assert (false);
    throw std::runtime_error ("not implemented yet!");
  }

  void getPositionBitmaps (const SearchCond &cond, std::vector<boost::shared_ptr<PositionBitmap> > &positions);

  void getDecompressedData (const PositionRange &range, void *buffer, size_t bufferSize) {
    assert (false);
    throw std::runtime_error ("not implemented yet!");
  }

  void getDictionaryCompressedData (const PositionRange &range, void *buffer, size_t bufferSize, int &bitOffset);
  int getDictionaryEntryId (const void *value);
  int getDictionaryEntrySizeInBits ();
  int getDictionaryEntryCount ();
  void getAllDictionaryEntries (std::vector<std::string> &entries);
  std::vector<int> searchDictionary (const SearchCond &cond);

private:
  int _dictionaryBits;
  int _entriesPerPage;


  // for 1bit-4bits.
  int processPageBitOffset(const std::vector<int> &matchingIds, const uint8_t *cursor, int bitOffset, size_t tuplesToRead, PositionBitmap *bitmap, int64_t bitmapPageOffset);

  // for 8bits/16bits.
  template <typename T>
  int processPageNoBitOffset(const std::vector<int> &matchingIds, const T *cursor, size_t tuplesToRead, PositionBitmap *bitmap, int64_t bitmapPageOffset) {
    assert (matchingIds.size() > 0);
    int matchCount = 0;
    // branch to improve performance when matchingIds.size() == 1, which is often.
    if (matchingIds.size() == 1) {
      const T key = matchingIds[0];
      for (size_t i = 0; i < tuplesToRead; ++i, ++cursor) {
        if (*cursor == key) {
          bitmap->setBit(i + bitmapPageOffset);
          ++matchCount;
        }
      }
    } else {
      const size_t s = matchingIds.size();
      for (size_t i = 0; i < tuplesToRead; ++i, ++cursor) {
        for (size_t j = 0; j < s; ++j) {
          if (*cursor == matchingIds[j]) {
            bitmap->setBit(i + bitmapPageOffset);
            ++matchCount;
            break;
          }
        }
      }
    }
    return matchCount;
  }
};

class FColumnReaderImplRLE : public FColumnReaderImpl, virtual public FColumnReaderRLE {
public:
  FColumnReaderImplRLE (FBufferPool *bufferpool, const FCStoreColumn &column, const FFileSignature &signature);

  void getPositionRanges (const SearchCond &cond, std::vector<PositionRange> &positions);

  // likewise, getPositionBitmaps() is useless for RLE. this is not implemented.
  void getPositionBitmaps (const SearchCond &cond, std::vector<boost::shared_ptr<PositionBitmap> > &positions) {
    assert (false);
    throw std::runtime_error ("not implemented yet!");
  }

  void getDecompressedData (const PositionRange &range, void *buffer, size_t bufferSize) {
    assert (false);
    throw std::runtime_error ("not implemented yet!");
  }

  void getRLECompressedData (const PositionRange &range, std::vector<std::pair<PositionRange, int8_t> > &result) {
    assert (_column.type == COLUMN_INT8);
    getRLECompressedData (range, &result);
  }
  void getRLECompressedData (const PositionRange &range, std::vector<std::pair<PositionRange, int16_t> > &result) {
    assert (_column.type == COLUMN_INT16);
    getRLECompressedData (range, &result);
  }
  void getRLECompressedData (const PositionRange &range, std::vector<std::pair<PositionRange, int32_t> > &result) {
    assert (_column.type == COLUMN_INT32);
    getRLECompressedData (range, &result);
  }
  void getRLECompressedData (const PositionRange &range, std::vector<std::pair<PositionRange, int64_t> > &result) {
    assert (_column.type == COLUMN_INT64);
    getRLECompressedData (range, &result);
  }
  void getRLECompressedData (const PositionRange &range, std::vector<std::pair<PositionRange, std::string> > &result) {
    assert (_column.type == COLUMN_CHAR);
    getRLECompressedData (range, &result);
  }
private:
  void getPositionRangesFullscan (const SearchCond &cond, std::vector<PositionRange> &positions);
  void getPositionRangesPartialScan (const SearchCond &cond, std::vector<PositionRange> &positions, const PositionRange &scanRange);

  // return the range of pageid for given scan range
  // beginPageId: the first page which has some tuple in scanRange
  // endPageId: the first page after beginPage which has no tuple in scanRange
  std::pair<int, int> getPageRange (const PositionRange &scanRange);

  void getRLECompressedData (const PositionRange &range, void *result);
};

} // fdb
#endif // STORAGE_CSTOREIMPL_H
