#ifndef STORAGE_FBTREEIMPL_H
#define STORAGE_FBTREEIMPL_H

#include "ffilesig.h"
#include "fbtree.h"
#include <algorithm>
#include <string.h>
#include <stdint.h>
#include <vector>
#include <stx/btree_map.h>
#include <glog/logging.h>

namespace fdb {

// pimpl class of FMainMemoryBTree.
// so far, this class uses the STX Btree as an internal data structure.
// STX BTree does not support variable length key/data, thus
// we parameterize key/data types in the derived class.
class FMainMemoryBTreeImpl {
public:
  FMainMemoryBTreeImpl(int keySize, int dataSize, TableType tableType, int64_t maxSize);
  virtual ~FMainMemoryBTreeImpl();

  virtual bool insert (const void *key, const void *data) = 0;
  virtual void finishInserts () = 0;
  virtual const void* getSingleTupleByKey (const void *key) const = 0;
  virtual void traverse(TraversalCallback callback, void *context) const = 0;
  void dumpToNewRowStoreFile (FFileSignature &signature) const;
  int64_t size () const { return _tuples; }
  int getKeySize() const { return _keySize; }
  int getDataSize() const { return _dataSize; }
  TableType getTableType() const {return _tableType;}

  virtual bool isSortedBuffer() const = 0;
  virtual void scanTuplesGreaterEqual (TupleCallback callback, void *context, const char *key) const = 0;
  const void* getUnsortedBuffer() const {
    return _array;
  }

  int _keySize, _dataSize;
  TableType _tableType;
  int64_t _maxSize;
  bool _finishedInserts;
  KeyDataCompareFunc _keydataFunc;
  DataDataCompareFunc _datadataFunc;

protected:
  void insertTupleToArray (const void *key, const void *data);

  char *_array;
  int64_t _tuples;
};

// always-sorted version
template <typename Key, typename Compare=std::less<Key> >
class FMainMemoryBTreeImplSorted : public FMainMemoryBTreeImpl {
public:
  FMainMemoryBTreeImplSorted (TableType tableType, int dataSize, int64_t maxSize) : FMainMemoryBTreeImpl(sizeof(Key), dataSize, tableType, maxSize) {
  }
  virtual ~FMainMemoryBTreeImplSorted() {}

  typedef typename stx::btree_map<Key, void*, Compare> MapType;
  typedef typename MapType::const_iterator MapConstIter;
  MapType _map;

  bool insert (const void *key, const void *data) {
    _map.insert(*(reinterpret_cast<const Key*>(key)), _array + (_dataSize * _tuples));
    insertTupleToArray(key, data);
    return true;
  }
  void finishInserts () {
    // does nothing. data is always sorted
    _finishedInserts = true;
  }
  const void* getSingleTupleByKey (const void *key) const {
    MapConstIter iter = _map.find (*(reinterpret_cast<const Key*>(key)));
    if (iter == _map.end()) return NULL;
    return iter.data();
  }
  void traverse(TraversalCallback callback, void *context) const {
    for (MapConstIter iter = _map.begin(); iter != _map.end(); ++iter) {
      callback(context, &(iter.key()), iter.data());
    }
  }
  bool isSortedBuffer() const { return true; }
  void scanTuplesGreaterEqual (TupleCallback callback, void *context, const char *key) const {
    for (MapConstIter iter = _map.lower_bound(*(reinterpret_cast<const Key*>(key))); iter != _map.end(); ++iter) {
      TupleCallbackRet ret = callback(context, iter.data());
      if (ret != TUPLE_CALLBACK_OK) break;
    }
  }
};

// unlike FMainMemoryBTreeImplUnsortedArray,
// this class just keep appending the data without maintaining BTree until finishInserts() is called.
template <typename Key, typename Compare=std::less<Key> >
class FMainMemoryBTreeImplUnsorted : public FMainMemoryBTreeImpl {
public:
  FMainMemoryBTreeImplUnsorted (TableType tableType, int dataSize, int64_t maxSize) : FMainMemoryBTreeImpl(sizeof(Key), dataSize, tableType, maxSize) {
    _sortedKeys = new KeyAndPtr[maxSize];
    ::memset (_sortedKeys, 0, sizeof(KeyAndPtr) * maxSize);
  }
  virtual ~FMainMemoryBTreeImplUnsorted(){}

  struct KeyAndPtr {
    KeyAndPtr() {}
    KeyAndPtr(const Key &key, const void *data) : _key(key), _data(data) {}
    Key _key;
    const void *_data;
    inline bool operator==(const KeyAndPtr &other) const {
      return _key == other._key;
    }
    inline bool operator!=(const KeyAndPtr &other) const {
      return _key != other._key;
    }
    inline bool operator<(const KeyAndPtr &other) const {
      return _key < other._key;
    }
  };
  KeyAndPtr *_sortedKeys; // sorted only when finishInserts() is called

  bool insert (const void *key, const void *data) {
    _sortedKeys[_tuples]._key = *reinterpret_cast<const Key*>(key);
    _sortedKeys[_tuples]._data = _array + (_dataSize * _tuples);
    insertTupleToArray(key, data);
    return true;
  }
  void finishInserts () {
    if (!_finishedInserts) {
      std::sort (_sortedKeys, _sortedKeys + _tuples);
      _finishedInserts = true;
    }
  }
  const void* getSingleTupleByKey (const void *key) const {
    assert (_finishedInserts);
    KeyAndPtr *found = std::lower_bound (_sortedKeys, _sortedKeys + _tuples, KeyAndPtr(*reinterpret_cast<const Key*>(key), NULL));
    if (found == NULL) return NULL;
    return found->_data;
  }
  void traverse(TraversalCallback callback, void *context) const {
    assert (_finishedInserts);
    for (size_t i = 0; i < _tuples; ++i) {
      callback(context, &(_sortedKeys[i]._key), _sortedKeys[i]._data);
    }
  }
  bool isSortedBuffer() const { return false; }
  void scanTuplesGreaterEqual (TupleCallback callback, void *context, const char *key) const {
    assert (false);
    throw std::exception();
  }
};

struct FPageHeader;
// pimple class for FReadOnlyDiskBTree
class FReadOnlyDiskBTreeImpl {
public:
  FReadOnlyDiskBTreeImpl (FBufferPool *bufferpool, const FFileSignature &signature);

  const FFileSignature& getFileSignature () const { return _signature; }
  const char* getSingleTupleByKey (const char *key);

  // return the id of first leaf page that *might* contain the matching tuple
  // to the given search key. first-key of such a page is less or eqaul to key
  // , so the following leaf scan starts from this page.
  // returns a negative number if there is no such a page.
  // @param equalitySearch specifies the search requests equality or not.
  //    Suppose (page-keys 1,2,4,5). this method returns -1 for search(3) if true, returns 4 if false.
  int getFirstMatchingLeafPageId(int currentLevel, int fromPageId, const char *key, bool equalitySearch);

  void scanAllTuples (TupleCallback callback, void *context);
  void scanTuplesGreaterEqual (TupleCallback callback, void *context, const char *key);

  void checkNonLeafPageHeader(const FPageHeader *header, int pageId, int currentLevel);
  void checkLeafPageHeader(const FPageHeader *header, int pageId);

  FBufferPool *_bufferpool;
  FFileSignature _signature;
  bool _empty; // true if this btree has no data
  KeyCompareFunc _compfunc;
  KeyDataCompareFunc _compfuncForLeaf;
};


} // fdb
#endif // STORAGE_FBTREEIMPL_H
