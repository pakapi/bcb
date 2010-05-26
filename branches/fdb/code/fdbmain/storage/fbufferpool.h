#ifndef STORAGE_FBUFFERPOOL_H
#define STORAGE_FBUFFERPOOL_H

#include "../configvalues.h"
#include <vector>

namespace fdb {

class FBufferPoolImpl;
struct FFileSignature;

// represents a buffer pool to keep disk pages in main memory.
// note that this buffer pool is *just for reading*, thus all pages
// in it cannot be 'dirty' thanks to the simple fractured
// database architecture where every disk write is a 'dump'.
class FBufferPool {
public:
  FBufferPool(int _maxPageCount);
  ~FBufferPool();

  // releases all buffered pages, opened file descriptors, etc (but the pool is still usable unlike calling the destructor)
  void clear();

  // returns the content of specified page from this buffer pool. returns NULL if not found.
  char* findPage (int fileId, int pageId);

  // adds the given page to this buffer pool, possibly evicting some page.
  // this method grants the ownership of the 'data' pointer to this buffer pool,
  // so it will be deleted by the buffer pool.
  void addPage (int fileId, int pageId, char *data);

  // returns the content of specified page, accessing the file if not found in the pool.
  // this method automatically open/close the file and adds the newly read page to the pool.
  // usually, a user just uses this method rather than findPage()/addPage() explicitly.
  const char* readPage (const FFileSignature &signature, int pageId);

  // same as readPage(), except that this reads multiple contiguous pages at once.
  // This could be efficient if 100 or 1000- contiguous pages are read at once.
  // the returned vector always contains pageCount elements, but could contain NULL char*
  // if the file doesn't have that much pages from beginningPageId.
  std::vector<const char*> readPages (const FFileSignature &signature, int beginningPageId, int pageCount);

  // Minor TODO:
  // all methods above assume that the returned pages will not be evicted while
  // the caller is touching them, which is most likely true.
  // if there is some long-running transaction where this might not be true,
  // we have to add pinning/unpinning options to each method, or
  // the pages must be copied before using.

  // this method is special. it's similar to readPages(), but the result is not stored in the bufferpool
  // but instead copied to the given buffer pointer.
  // this method is useful if the caller knows the pages will not be read again (ex. when merging fractures).
  // note that the buffer pointer has to be allocated by DirectFileStream::allocateMemoryForIO.
  void readPages (const FFileSignature &signature, int beginningPageId, int pageCount, char *buffer);


  // should be only used from testcases..
  FBufferPoolImpl* getImpl () { return _impl; };
private:
  FBufferPoolImpl *_impl;//pimpl object
};


} // fdb

#endif // STORAGE_FBUFFERPOOL_H
