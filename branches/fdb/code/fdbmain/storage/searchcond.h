#ifndef STORAGE_SEARCHCOND_H
#define STORAGE_SEARCHCOND_H

#include <vector>
#include <string.h>

namespace fdb {

enum SearchCondType {
  EQUAL,
  BETWEEN,
  IN,
  LT,
  GT,
  LTEQ,
  GTEQ,
};
struct SearchCond {
  SearchCond (SearchCondType type_, const void *key_) : type(type_), key(key_), key2(NULL) {} // for EQUAL, LT, GT, LTEQ, GTEQ
  SearchCond (const void *from, const void *to) : type(BETWEEN), key(from), key2(to) {} // for BETWEEN
  SearchCond (const std::vector<const void*> &keys_) : type(IN), key(NULL), key2(NULL), keys(keys_) {} // for IN
  SearchCondType type;
  const void *key, *key2;
  std::vector<const void*> keys;

  // TODO : these matchings are executed quite often when scanning files.
  // switch/if, etc should be avoided.
  // so, better to parameterize everything (parameter type and search type) by template.
  // NOTE : above parameterization is now done in CStore classes, not in this class

  inline bool matchStringIn(const void *data, size_t length) const {
    for (size_t i = 0; i < keys.size(); ++i) {
      if (::memcmp (data, keys[i], length) == 0) return true;
    }
    return false;
  }
  inline bool matchString(const void *data, size_t length) const {
    switch (type) {
    case EQUAL: return ::memcmp (data, key, length) == 0;
    case LT: return ::memcmp (data, key, length) < 0;
    case GT: return ::memcmp (data, key, length) > 0;
    case LTEQ: return ::memcmp (data, key, length) <= 0;
    case GTEQ: return ::memcmp (data, key, length) >= 0;
    case BETWEEN: return ::memcmp (data, key, length) >= 0 && ::memcmp (data, key2, length) <= 0;
    case IN: return matchStringIn(data, length);
    default:
      assert (false);
      return false;
    }
  }

  // non parameterized version. do not use this in performance sensitive place.
  inline bool matchInts(const void *data, size_t length) const {
    switch (length) {
    case 1: return matchInts<int8_t>(*reinterpret_cast<const int8_t*>(data));
    case 2: return matchInts<int16_t>(*reinterpret_cast<const int16_t*>(data));
    case 4: return matchInts<int32_t>(*reinterpret_cast<const int32_t*>(data));
    case 8: return matchInts<int64_t>(*reinterpret_cast<const int64_t*>(data));
    default:
      assert (false);
      return false;
    }
  }

  // only INT_TYPE is parameterized so far. still switch for search type...
  template<typename INT_TYPE>
  inline bool matchIntsIn(INT_TYPE d) const {
    for (size_t i = 0; i < keys.size(); ++i) {
      if (d == *reinterpret_cast<const INT_TYPE*>(keys[i])) return true;
    }
    return false;
  }
  template<typename INT_TYPE>
  inline bool matchInts(INT_TYPE d) const {
    switch (type) {
    case EQUAL: return d == *reinterpret_cast<const INT_TYPE*>(key);
    case LT: return d < *reinterpret_cast<const INT_TYPE*>(key);
    case GT: return d > *reinterpret_cast<const INT_TYPE*>(key);
    case LTEQ: return d <= *reinterpret_cast<const INT_TYPE*>(key);
    case GTEQ: return d >= *reinterpret_cast<const INT_TYPE*>(key);
    case BETWEEN: return d >= *reinterpret_cast<const INT_TYPE*>(key) && d <= *reinterpret_cast<const INT_TYPE*>(key2);
    case IN: return matchIntsIn<INT_TYPE>(d);
    default:
      assert (false);
      return false;
    }
  }
};

inline const char* toSearchCondOp (SearchCondType type) {
  switch (type) {
  case EQUAL:
    return "=";
  case LT:
    return "<";
  case GT:
    return ">";
  case LTEQ:
    return "<=";
  case GTEQ:
    return ">=";
  case IN:
    return "IN";
  case BETWEEN:
    return "BETWEEN";
  default:
    return "UNKNOWN";
  }
}

} //fdb
#endif // STORAGE_SEARCHCOND_H
