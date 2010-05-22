#ifndef STORAGE_FBTREE_H
#define STORAGE_FBTREE_H

#include "../configvalues.h"
#include "fkeycomp.h"
#include <stdint.h>

namespace fdb {

// BTree classes for FDB.
// BTrees in FDB are quite different from those in conventional DBs.
// Every BTree is written to disk at once; it's just a dump.
// Once written, every BTree is strictly read-only, so these classes are
// substantially simpler than usual BTrees.


enum TupleCallbackRet {
  TUPLE_CALLBACK_OK = 1, // requests to keep providing tuple
  TUPLE_CALLBACK_QUIT = 2, // requests to stop providing tuple
  TUPLE_CALLBACK_ERROR = 3, // tells that something bad happens..
  // more specific error codes?
};
typedef TupleCallbackRet (*TupleCallback) (void *context, const void *tuple);

class FMainMemoryBTreeImpl;
struct FFileSignature;
// BTree for writing.
// This BTree is just for maintaining a temporary on-memory BTree structure
// and will be passed as aparameter when flushing to disk (see FSignatureSet::dumpToNewFile()).
typedef void (*TraversalCallback) (void *context, const void *key, const void *data);
class FMainMemoryBTree {
public:
  // @maxSize maximum number of tuples
  // @sortedBuffer whether this buffer is kept sorted
  FMainMemoryBTree(TableType type, int64_t maxSize, bool sortedBuffer);
  ~FMainMemoryBTree();

  // inserts a new entry into the btree. returns true when succeeded.
  bool insert (const void *key, const void *data);

  // call this method when you are done with INSERTs.
  // this method 'might' trigger reorganization (sorting) of the on-memory data
  // , leading to better query performance.
  // once this method is called, insert () can't be called for this object.
  void finishInserts ();

  // returns the tuple for given key. NULL if not found.
  const void* getSingleTupleByKey (const void *key) const;

  // returns the total count of entries
  int64_t size () const;

  // traverses all entries in this btree, calls back the function for each entry with given context object.
  void traverse(TraversalCallback callback, void *context) const;

  // dumps this BTree to a new file in row-store format.
  // properties of signature will be set in this method.
  // only fileid/filepath should be set before calling this method.
  void dumpToNewRowStoreFile (FFileSignature &signature) const;

  // returns the byte sizes of one key
  int getKeySize() const;
  // returns the byte sizes of one data
  int getDataSize() const;

  TableType getTableType() const;

  // methods to directly access the buffer
  // returns true if the buffer is sorted
  bool isSortedBuffer() const;
  // returns the unsorted (append only) buffer
  const void* getUnsortedBuffer() const;
  // same as FReadOnlyDiskBTree's method, assuming it's a sorted buffer.
  // if this method is called for unsorted buffer, it throws exception.
  void scanTuplesGreaterEqual (TupleCallback callback, void *context, const char *key) const;

  FMainMemoryBTreeImpl* getImpl () { return _impl; } // only used by testcases
private:
  FMainMemoryBTreeImpl *_impl; //pimpl object
};

class FBufferPool;
struct FFileSignature;
class FReadOnlyDiskBTreeImpl;

// BTree for reading.
// This BTree is disk-based, but only for reading.
// Constructed with a file signature and a bufferpool instance.
class FReadOnlyDiskBTree {
public:
  FReadOnlyDiskBTree (FBufferPool *bufferpool, const FFileSignature &signature);
  ~FReadOnlyDiskBTree ();

  const FFileSignature& getFileSignature () const;

  // returns tuple data of given key of this BTree (returns first tuple if not unique).
  // returns NULL if the key wasn't found.
  // to get more than one tuples, use scanAllTuples() or scanTuplesGreaterEqual().
  const char* getSingleTupleByKey (const char *key);

  // Calls back the provided function for all tuples starting from the first tuple.
  // This method is for full table scan (or less-than search).
  void scanAllTuples (TupleCallback callback, void *context);

  // Calls back the provided function for tuples starting from the first
  // tuple that has a key equal to OR greater than the key.
  // This method can be used for exact search, inequality search, and range search.
  void scanTuplesGreaterEqual (TupleCallback callback, void *context, const char *key);

  FReadOnlyDiskBTreeImpl* getImpl () { return _impl; } // only used by testcases
private:
  FReadOnlyDiskBTreeImpl *_impl; //pimpl object
};

} // fdb
#endif // STORAGE_FBTREE_H
