#ifndef STORAGE_FKEYCOMP_H
#define STORAGE_FKEYCOMP_H

#include "../configvalues.h"

namespace fdb {

// key comparison functions used both in BTree and CStore.

// returns appropriate key comparison type for given table type
KeyCompareFuncType toKeyCompareFuncType(TableType type);

// key comparison function should return a negative, zero, positive values
// when key1<key2, key1=key2, key1>key2 respectively.
typedef int (*KeyCompareFunc) (const void *key1, const void *key2);

// same as KeyCompareFunc, but the second param is tuple data.
// used for key comparison in leaf pages
typedef int (*KeyDataCompareFunc) (const void *key1, const void *data2);

typedef int (*DataDataCompareFunc) (const void *data1, const void *data2);

typedef void (*ExtractKeyFromTupleFunc) (const void *tuple, void *key);

// returns appropriate key comparison function for given type
KeyCompareFunc toKeyCompareFunc(KeyCompareFuncType type);
KeyDataCompareFunc toKeyDataCompareFunc(TableType type);
DataDataCompareFunc toDataDataCompareFunc(TableType type);
ExtractKeyFromTupleFunc toExtractKeyFromTupleFunc(TableType type);
int toKeySize(TableType type);

// pre-defined comparison functions
int int32AscCompareFunc (const void *key1, const void *key2);
int int64AscCompareFunc (const void *key1, const void *key2);

// just for LINEORDER_PK_SORT
int int64LineorderPKAscCompareFunc(const void *key1, const void *data2);

// data2's first 4 byte is the key.
int int32FirstAscCompareFunc (const void *key1, const void *data2);

} // fdb
#endif // STORAGE_FKEYCOMP_H
