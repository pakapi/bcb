#ifndef STORAGE_FFILESIG_H
#define STORAGE_FFILESIG_H

#include "../configvalues.h"

namespace fdb {

#define FFILE_MAX_FILEPATH 128

// increase this number when you add a new property
#define FFILE_SIGNATURE_CUR_VER 3
// signature of one data file
struct FFileSignature {
  FFileSignature ()
    : signatureVersion(FFILE_SIGNATURE_CUR_VER), fileId(0), totalTupleCount(0),
    pageCount (0), rootPageStart(0), rootPageCount(0), rootPageLevel(0),
    keyEntrySize(0), keyCompareFuncType(KEY_CMP_INVALID), leafEntrySize(0), tableType(TABLE_TYPE_INVALID),
    columnFile (false), columnIndex(0), columnType(COLUMN_INVALID), columnMaxLength(0), columnOffset(0), columnCompression(COMPRESSION_INVALID), dictionaryBits(0), dictionaryEntryCount (0)
  {}

  int signatureVersion;
  char filepath[FFILE_MAX_FILEPATH];
  int fileId; // sequentially assigned ID for each file
  int64_t totalTupleCount; // total count of tuples in this file
  int pageCount; // total count of all pages in the file
  int rootPageStart, rootPageCount; // page index of the first root page and num of root pages
  int rootPageLevel; // the level of root pages. ex. if leaf+root, leaf=0, root=1.
  int keyEntrySize; // byte size of one key in BTree (r-store) or key table (compressed c-store)
  KeyCompareFuncType keyCompareFuncType; // specify the type of key comparison
  int leafEntrySize; // byte size of one tuple
  TableType tableType;

  // for column store files
  bool columnFile; // true if this is a column store file
  int columnIndex; // specifies the index of the column in the table
  ColumnType columnType; // type of the column
  int columnMaxLength; //length of the column
  int columnOffset; //offset of the column in tuple format
  CompressionScheme columnCompression; //the type of compression for this column
  int dictionaryBits; // for dictionary compression
  int dictionaryEntryCount; // for dictionary compression
};

} // fdb
#endif // STORAGE_FFILESIG_H
