#ifndef CONFIGVALUES_H
#define CONFIGVALUES_H

// defines hard-coded configuration values

#include <stdint.h>

// byte size of one page
#define FDB_PAGE_SIZE 65536

// number of pages to write to disk at once. in other words, output buffer size.
#define FDB_DISK_WRITE_BUFFER_PAGES 128

// a btree will adds more level if the highest level has more than this number of pages.
// note that our btree has more than one root pages ('root' in usual sense isn't needed).
#define FDB_MAX_ROOT_PAGES 10

// property file name for log4cxx
#define FDB_LOG4CXX_FILE "log4cxx.properties"

// lists every possible comparison type supported.
// a quick dirty way of specifying function pointers in data file.
enum KeyCompareFuncType {
  KEY_CMP_INVALID = 0,
  INT32_ASC_NODUP = 1,
  INT64_ASC_NODUP = 2,

  LINEORDER_ORDERDATEPK_COMP = 3,

  MV_PROJECTION_COMP = 4,
};

// lists every possible table type supported.
enum TableType {
  TABLE_TYPE_INVALID = 0,
  LINEORDER_PK_SORT = 1,
  CUSTOMER_PK_SORT = 2,
  SUPPLIER_PK_SORT = 3,
  PART_PK_SORT = 4,
  DATE_PK_SORT = 5,

  MV_PROJECTION = 7,
};

enum CompressionScheme {
  COMPRESSION_INVALID = 0,
  UNCOMPRESSED = 1,
  RLE_COMPRESSED = 2,

  DICTIONARY_COMPRESSED_1BIT = 10, // upto 2 values
  DICTIONARY_COMPRESSED_2BIT = 11, // upto 4 values
  DICTIONARY_COMPRESSED_4BIT = 12, // upto 16 values
  DICTIONARY_COMPRESSED_8BIT = 13, // upto 256 values
  DICTIONARY_COMPRESSED_16BIT = 14,  // upto 65536 values
};
inline const char *toCompressionSchemeName (CompressionScheme compression) {
  switch (compression) {
  case COMPRESSION_INVALID: return "COMPRESSION_INVALID";
  case UNCOMPRESSED: return "UNCOMPRESSED";
  case RLE_COMPRESSED: return "RLE_COMPRESSED";
  case DICTIONARY_COMPRESSED_1BIT: return "DICTIONARY_COMPRESSED_1BIT";
  case DICTIONARY_COMPRESSED_2BIT: return "DICTIONARY_COMPRESSED_2BIT";
  case DICTIONARY_COMPRESSED_4BIT: return "DICTIONARY_COMPRESSED_4BIT";
  case DICTIONARY_COMPRESSED_8BIT: return "DICTIONARY_COMPRESSED_8BIT";
  case DICTIONARY_COMPRESSED_16BIT: return "DICTIONARY_COMPRESSED_16BIT";
  default: return "UNKNOWN";
  }
}
inline int toDictionaryCompressionBits (CompressionScheme compression) {
  switch (compression) {
  case DICTIONARY_COMPRESSED_1BIT: return 1;
  case DICTIONARY_COMPRESSED_2BIT: return 2;
  case DICTIONARY_COMPRESSED_4BIT: return 4;
  case DICTIONARY_COMPRESSED_8BIT: return 8;
  case DICTIONARY_COMPRESSED_16BIT: return 16;
  default: return -1;
  }
}
inline bool isDictionaryCompression(CompressionScheme compression) {
  return compression >= DICTIONARY_COMPRESSED_1BIT && compression <= DICTIONARY_COMPRESSED_16BIT;
}

enum ColumnType {
  COLUMN_INVALID = 0,
  COLUMN_INT8 = 1,
  COLUMN_INT16 = 2,
  COLUMN_INT32 = 3,
  COLUMN_INT64 = 4,
//   COLUMN_BOOL = 5, not supported so far
  COLUMN_CHAR = 6,
};
inline const char *toColumnTypeName (ColumnType type) {
  switch (type) {
  case COLUMN_INVALID: return "COLUMN_INVALID";
  case COLUMN_INT8: return "COLUMN_INT8";
  case COLUMN_INT16: return "COLUMN_INT16";
  case COLUMN_INT32: return "COLUMN_INT32";
  case COLUMN_INT64: return "COLUMN_INT64";
  case COLUMN_CHAR: return "COLUMN_CHAR";
  default: return "UNKNOWN";
  }
}

// uses linux's O_DIRECT or not to read/write data files
#define FDB_USE_DIRECT_IO true
// if O_DIRECT is used, the size of memory page (Google open() and posix_memalign() for more details)
#define FDB_DIRECT_IO_ALIGNMENT 4096

// Like the original dbgen, we use Minimal Standard random generator.
#define MSRAND_MOD 0x7FFFFFFF
#define MSRAND_MUL 16807
#define MSRAND_MQ (MSRAND_MOD / MSRAND_MUL)
#define MSRAND_MR (MSRAND_MOD % MSRAND_MUL)
inline int ms_rand (int current) {
  int nU = current / MSRAND_MQ;
  int nV = current - MSRAND_MQ * nU;       /* i.e., nV = nSeed % nQ */
  current = MSRAND_MUL * nV - nU * MSRAND_MR;
  if (current < 0)
      current += MSRAND_MOD;
  return (current);
}

#endif // CONFIGVALUES_H
