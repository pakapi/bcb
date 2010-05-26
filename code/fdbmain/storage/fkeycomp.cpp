
#include <cassert>
#include <exception>
#include <stdint.h>
#include "fkeycomp.h"
#include "../configvalues.h"
#include "../ssb/ssb.h"

namespace fdb {

// ==========================================================================
//  Key Comparison Functions and Utilities
// ==========================================================================
int toKeySize(TableType type) {
  switch (type) {
    case  LINEORDER_PK_SORT:
      return sizeof(int64_t);
    case  CUSTOMER_PK_SORT:
    case  SUPPLIER_PK_SORT:
    case  PART_PK_SORT:
    case  DATE_PK_SORT:
      return sizeof(int32_t);
    case  MV_PROJECTION:
      return sizeof(MVProjection::PKType);
    default:
      // unsupported type
      assert (false);
      throw std::exception();
  }
}

int toDataSize(TableType type) {
  switch (type) {
    case  LINEORDER_PK_SORT:
      return sizeof(Lineorder);
    case  CUSTOMER_PK_SORT:
      return sizeof(Customer);
    case  SUPPLIER_PK_SORT:
      return sizeof(Supplier);
    case  PART_PK_SORT:
      return sizeof(Part);
    case  DATE_PK_SORT:
      return sizeof(Date);
    case  MV_PROJECTION:
      return sizeof(MVProjection);
    default:
      // unsupported type
      assert (false);
      throw std::exception();
  }
}

KeyCompareFuncType toKeyCompareFuncType(TableType type) {
  switch (type) {
    case  LINEORDER_PK_SORT:
      return INT64_ASC_NODUP;
    case  CUSTOMER_PK_SORT:
    case  SUPPLIER_PK_SORT:
    case  PART_PK_SORT:
    case  DATE_PK_SORT:
      return INT32_ASC_NODUP;
    case  MV_PROJECTION:
      return MV_PROJECTION_COMP;
    default:
      // unsupported type
      assert (false);
      throw std::exception();
  }
}

ExtractKeyFromTupleFunc toExtractKeyFromTupleFunc(TableType type) {
  switch (type) {
    case  LINEORDER_PK_SORT:
      return Lineorder::extractPK;
    case  CUSTOMER_PK_SORT:
      return Customer::extractPK;
    case  SUPPLIER_PK_SORT:
      return Supplier::extractPK;
    case  PART_PK_SORT:
      return Part::extractPK;
    case  DATE_PK_SORT:
      return Date::extractPK;
    case  MV_PROJECTION:
      return MVProjection::extractPK;
    default:
      // unsupported type
      assert (false);
      throw std::exception();
  }
}

KeyCompareFunc toKeyCompareFunc(KeyCompareFuncType type) {
  switch (type) {
    case  INT32_ASC_NODUP:
      return int32AscCompareFunc;
    case  INT64_ASC_NODUP:
      return int64AscCompareFunc;
    case  MV_PROJECTION_COMP:
      return MVProjection::PKType::keyCompareFunc;
    default:
      // unsupported type
      assert (false);
      throw std::exception();
  }
}

int int32AscCompareFunc (const void *key1, const void *key2) {
  int32_t k1 = *(reinterpret_cast<const int32_t*> (key1));
  int32_t k2 = *(reinterpret_cast<const int32_t*> (key2));
  return k1 - k2; // no overflow possible. pretty simple
}
int int64AscCompareFunc (const void *key1, const void *key2) {
  int64_t k1 = *(reinterpret_cast<const int64_t*> (key1));
  int64_t k2 = *(reinterpret_cast<const int64_t*> (key2));
  if (k1 < k2) return -1;
  if (k1 == k2) return 0;
  return 1;
}

KeyDataCompareFunc toKeyDataCompareFunc(TableType type) {
  switch (type) {
    case  LINEORDER_PK_SORT:
      return int64LineorderPKAscCompareFunc;
    case  CUSTOMER_PK_SORT:
    case  SUPPLIER_PK_SORT:
    case  PART_PK_SORT:
    case  DATE_PK_SORT:
      return int32FirstAscCompareFunc;
    case  MV_PROJECTION:
      return MVProjection::PKType::keyDataCompareFunc;
    default:
      // unsupported type
      assert (false);
      throw std::exception();
  }
}
DataDataCompareFunc toDataDataCompareFunc(TableType type) {
  switch (type) {
    case  LINEORDER_PK_SORT:
      return Lineorder::compareTuplePK;
    case  CUSTOMER_PK_SORT:
    case  SUPPLIER_PK_SORT:
    case  PART_PK_SORT:
    case  DATE_PK_SORT:
      return int32FirstAscCompareFunc;
    case  MV_PROJECTION:
      return MVProjection::compareTuple;
    default:
      // unsupported type
      assert (false);
      throw std::exception();
  }
}

int int64LineorderPKAscCompareFunc(const void *key1, const void *data2) {
  int64_t k1 = *(reinterpret_cast<const int64_t*> (key1));
  int64_t k2 = reinterpret_cast<const Lineorder*> (data2)->getPK();
  if (k1 < k2) return -1;
  if (k1 == k2) return 0;
  return 1;
}

int int32FirstAscCompareFunc (const void *key1, const void *data2) {
  int32_t k1 = *(reinterpret_cast<const int32_t*> (key1));
  int32_t k2 = *(reinterpret_cast<const int32_t*> (data2));
  return k1 - k2;
}

} // fdb
