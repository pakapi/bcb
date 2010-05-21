#ifndef STORAGE_FCSTORE_H
#define STORAGE_FCSTORE_H

#include "../configvalues.h"
#include "fkeycomp.h"
#include <string>
#include <vector>
#include <cassert>
#include <utility>
#include <boost/shared_ptr.hpp>

namespace fdb {

// Column-Store storage implementation for FDB.
// In Column store, every column is stored in a separated file
// and possibly compressed by RLE, Dictionary or LZO.

// Unlike BTree (row-store) implementation, the number of entries
// stored in a physical page is variable because of compression.

// Like BTree (row-store), however, we have two versions of
// implementation; one for main-memory dump-only, one for disk-based read-only.

// TODO : nonetheless, for now we don't implement FMainMemoryCStore for now.
// we just use BTree to keep on-memory stuff row-store.
// FMainMemoryCStore will be implemented later to compare the advantage/disadvantage
// to keep temporary fracture in C-store.

// represents a column in CStore.
// Could be compressed or uncompressed.
struct FCStoreColumn {
  FCStoreColumn () {}
  FCStoreColumn (const std::string &name_, ColumnType type_, int offset_, CompressionScheme compression_)
    : name(name_), type(type_), offset(offset_), compression(compression_) {
    switch (type) {
      case COLUMN_INT8: maxLength = 1; break;
      case COLUMN_INT16: maxLength = 2; break;
      case COLUMN_INT32: maxLength = 4; break;
      case COLUMN_INT64: maxLength = 8; break;
      default:
        assert (false);
    }
  }
  FCStoreColumn (const std::string &name_, ColumnType type_, int maxLength_, int offset_, CompressionScheme compression_)
    : name(name_), type(type_), maxLength(maxLength_), offset(offset_), compression(compression_) {}
  std::string name;
  ColumnType type;
  int maxLength;
  int offset; // byte offset of this column in tuple format. (NOTE: could be different from sum of maxLength of preceeding columns because of C++ memory alignment)
  CompressionScheme compression;
};

class FMainMemoryCStoreImpl;
// represents a main-memory table (projection) in column store for writing.
// this class allows only inserting new entries and dumping the entire table
// to column-oriented files. Compression will be done when the entire table is dumped.
// This object will be passed as a parameter when flushing to disk (see FSignatureSet::dumpToNewFiles()).
// Each column will be saved to a file named like <prefix>_columnname.db.
class FMainMemoryCStore {
public:
  FMainMemoryCStore (TableType type, int maxTuples);
  bool insert (const void *data);

  FMainMemoryCStoreImpl* getImpl () { return _impl; } // only used by testcases
private:
  FMainMemoryCStoreImpl *_impl;
};

class FFileSignature;
class FMainMemoryBTree;
typedef std::pair<size_t, bool> SortOrder; // pair<columnIndex, asc>
class FCStoreUtil {
public:
  // returns column designs for given TableType
  static std::vector<FCStoreColumn> getPhysicalDesignsOf(TableType table);
  static std::vector<SortOrder> getSortOrdersOf(TableType table);

  // dumps a main memory BTree to new files in c-store format.
  // properties of signatures will be set in this method.
  // only fileid/filepath should be set before calling this method.
  static void dumpToNewCStoreFile (std::vector<FFileSignature> &signatures, const FMainMemoryBTree &btree);
};

struct PositionRange {
  PositionRange() : begin(0), end(0) {};
  PositionRange(int64_t begin_, int64_t end_) : begin(begin_), end(end_) {}
  int64_t begin; // inclusive
  int64_t end; // exclusive
};
struct PositionBitmap {
public:
  PositionBitmap (int64_t beginPosition_, size_t bitLength_);
  ~PositionBitmap();
  int64_t beginPosition; // position in original relation(=tupleid)
  size_t bitLength;
  size_t byteLength;
  unsigned char *bitmap;
  int64_t matchedCount;
  inline void setBit(int64_t position) {
    bitmap[position / 8] |= (1 << (position % 8));
  }
  // might need unsetBit(), but not needed so far
  static boost::shared_ptr<PositionBitmap> newBitmap (int64_t beginPosition, size_t bitLength_);
private: // prohibit wrong copying
  PositionBitmap (const PositionBitmap &);
};

class FBufferPool;
class FReadOnlyCStoreImpl;
class FColumnReader;
class FSignatureSet;
// represents a disk-based read-only table (projection) in column store for reading.
// An instance of this class holds several file signatures, one for each column.
// Each column can have very different compression scheme although the sort order is same.
class FReadOnlyCStore {
public:
  FReadOnlyCStore (FBufferPool *bufferpool, TableType type, const FSignatureSet &signatureSet, const std::string &dataFolder, const std::string &filenamePrefix);

  FColumnReader* getColumnReader(const std::string &colname);
  FColumnReader* getColumnReader(size_t colIndex);

private:
  FBufferPool *_bufferpool;
  TableType _type;
  std::vector<FCStoreColumn> _columns;
  std::vector<boost::shared_ptr<FColumnReader> > _columnReaders;
};

// provides read accesses for a column
class SearchCond;
class FColumnReader {
public:
  virtual ~FColumnReader(){}

  // convert the given string to NULL-padded string for this column
  virtual std::string normalize(const std::string &str) const = 0;

  virtual const FCStoreColumn& getColumn() const = 0;

  // restrict all following searches (getPositionXXX) to the given position ranges.
  // this setting is cleared when clearSearchRanges() or is called.
  virtual void setSearchRange (const PositionRange &ranges) = 0;
  virtual void setSearchRanges (const std::vector<PositionRange> &ranges) = 0;
  virtual void clearSearchRanges () = 0;

  // search for given condition in this column and puts all found positions (=tupleid)
  // to the vector.
  virtual void getPositionRanges (const SearchCond &cond, std::vector<PositionRange> &positions) = 0;
  virtual void getPositionBitmaps (const SearchCond &cond, std::vector<boost::shared_ptr<PositionBitmap> > &positions) = 0;

  // put the column data of specified range to given buffer. decompress if needed so that
  // the returned data are original data.
  virtual void getDecompressedData (const PositionRange &range, void *buffer, size_t bufferSize) = 0;
};

class FColumnReaderRLE : virtual public FColumnReader {
public:

  // returns RLE compressed data without decompression as vector of <RLE-range, value>.
  virtual void getRLECompressedData (const PositionRange &range, std::vector<std::pair<PositionRange, int8_t> > &result) = 0;
  virtual void getRLECompressedData (const PositionRange &range, std::vector<std::pair<PositionRange, int16_t> > &result) = 0;
  virtual void getRLECompressedData (const PositionRange &range, std::vector<std::pair<PositionRange, int32_t> > &result) = 0;
  virtual void getRLECompressedData (const PositionRange &range, std::vector<std::pair<PositionRange, int64_t> > &result) = 0;
  virtual void getRLECompressedData (const PositionRange &range, std::vector<std::pair<PositionRange, std::string> > &result) = 0;
};

class FColumnReaderDictionary : virtual public FColumnReader {
public:

  // returns dictionary compressed data without decompression.
  // if dictionary bit size is not multiply of 8bits, bitOffset is set to
  // tell the bit offset of first byte (to avoid bit shifting in every entry).
  // NOTE: if the dictionary is not multiply of 8bits, reserve extra 2 bytes for buffer (for first and last byte).
  virtual void getDictionaryCompressedData (const PositionRange &range, void *buffer, size_t bufferSize, int &bitOffset) = 0;

  // returns the ID of given value. -1 if not found.
  virtual int getDictionaryEntryId (const void *value) = 0;
  // as the name tells
  virtual int getDictionaryEntrySizeInBits () = 0;
  virtual int getDictionaryEntryCount () = 0;
  // returns all entries. for CHAR columns. (INT version is not implemented yet. rarely used though)
  virtual void getAllDictionaryEntries (std::vector<std::string> &entries) = 0;

  // read dictionary pages (root pages) and returns the ID of
  // entries matching with the search condition.
  // performance of this function shouldn't matter as dictionary pages are small.
  // so, not much tuning is done.
  virtual std::vector<int> searchDictionary (const SearchCond &cond) = 0;
};
} // fdb
#endif // STORAGE_FCSTORE_H
