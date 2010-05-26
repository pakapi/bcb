#ifndef UTIL_HASHMAP_H
#define UTIL_HASHMAP_H

// boost/tr1 or tr1 really depends on options and versions of
// compiler/boost (as of 2010). Our crappy old departmental machine doesn't have it.
// rather than struglling just for unordered_map, here we implement
// a simpler (and probably faster) version of unordered_map.

// Implememtation of a simple hashtable for String.

// This class basically emulates what boost/tr1/unordered_map does,
// but relies on many assumptions to simplify and improve performance.
// 1. the key is passed as char* with a uniform length.
// 2. the passed char* must be a valid pointer while this class is used,
// so you need to hold the real data somewhere and just pass the pointer to it.
// key copying is skipped thanks to this assumption.
// 3. the data is zero padded, so that memcmp (length) can correctly tell the difference.
// 4. no dynamic rehash/re-bucket.
// 5. no erase(). just find() and insert().

#include <string.h>
#include <cassert>

const size_t BIT_MASKS[] = {
  0x0000, 0x0001, 0x0003, 0x0007,
  0x000f, 0x001f, 0x003f, 0x007f,
  0x00ff, 0x01ff, 0x03ff, 0x07ff,
  0x0fff, 0x1fff, 0x3fff, 0x7fff,
  0xffff, 0x1ffff, 0x3ffff, 0x7ffff,
};

class StringHashSet {
public:
  StringHashSet (size_t stringLength, size_t hashbits) : _stringLength(stringLength), _hashbits (hashbits) {
    assert (hashbits <= 20);
    _hashtable = new std::vector<const char*>*[(1 << _hashbits)];
    ::memset (_hashtable, 0, sizeof (std::vector<const char*>*) << _hashbits);
  }
  ~StringHashSet () {
    for (size_t i = 0; i < (size_t) (1 << _hashbits); ++i) {
      if (_hashtable[i] != NULL) {
        delete _hashtable[i];
      }
    }
    delete[] _hashtable;
  }
  const char* find (const char *data) const {
    size_t h = hash (data);
    if (_hashtable[h] == NULL) return NULL;
    for (size_t i = 0; i < _hashtable[h]->size(); ++i) {
      const char *data2 = (*_hashtable[h])[i];
      if (equal (data, data2)) return data2;
    }
    return NULL;
  }
  void insert (const char *data) {
    size_t h = hash (data);
    if (_hashtable[h] == NULL) _hashtable[h] = new std::vector<const char*>();
    _hashtable[h]->push_back (data);
  }
  size_t hash (const char *data) const {
    size_t ret = 0;
    for (size_t i = 0; i < _stringLength; ++i) {
      ret = ret * 0x74821fe1 + *(data + i);
    }
    return ret & BIT_MASKS[_hashbits];
  }
  bool equal(const char *data1, const char *data2) const {
    return ::memcmp (data1, data2, _stringLength) == 0;
  }
private:
  std::vector<const char*> **_hashtable;
  size_t _stringLength;
  size_t _hashbits; // the length of hash bits used, and the size of the table is 2^_hashbits
};

template <typename T>
class StringHashMap {
public:
  StringHashMap (size_t stringLength, size_t hashbits) : _stringLength(stringLength), _hashbits (hashbits) {
    assert (hashbits <= 20);
    _hashtable = new std::vector<Entry>*[(1 << _hashbits)];
    ::memset (_hashtable, 0, sizeof (std::vector<Entry>*) << _hashbits);
  }
  ~StringHashMap () {
    for (size_t i = 0; i < (size_t) (1 << _hashbits); ++i) {
      if (_hashtable[i] != NULL) {
        delete _hashtable[i];
      }
    }
    delete[] _hashtable;
  }
  T find (const char *key) const {
    size_t h = hash (key);
    if (_hashtable[h] == NULL) return NULL;
    for (size_t i = 0; i < _hashtable[h]->size(); ++i) {
      const Entry &entry = (*_hashtable[h])[i];
      if (equal (key, entry.key)) return entry.data;
    }
    assert (false);
    return NULL;
  }
  void insert (const char *key, T data) {
    size_t h = hash (key);
    if (_hashtable[h] == NULL) _hashtable[h] = new std::vector<Entry>();
    _hashtable[h]->push_back (Entry(key, data));
  }
  size_t hash (const char *key) const {
    size_t ret = 0;
    for (size_t i = 0; i < _stringLength; ++i) {
      ret = ret * 0x74821fe1 + *(key + i);
    }
    return ret & BIT_MASKS[_hashbits];
  }
  bool equal(const char *key1, const char *key2) const {
    return ::memcmp (key1, key2, _stringLength) == 0;
  }
private:
  struct Entry {
    Entry (const char *key_, T data_) : key (key_), data(data_){}
    const char *key;
    T data;
  };

  std::vector<Entry> **_hashtable;
  size_t _stringLength;
  size_t _hashbits; // the length of hash bits used, and the size of the table is 2^_hashbits
};
#endif// UTIL_HASHMAP_H
