#include "fbufferpool.h"
#include "fbtree.h"
#include "fbtreeimpl.h"
#include "ffile.h"
#include "../ssb/ssb.h"
#include "../io/fis.h"
#include "../util/stopwatch.h"
#include <boost/scoped_ptr.hpp>
#include <string.h>
#include <fstream>
#include <glog/logging.h>

using namespace std;
using namespace boost;

namespace fdb {

// ==========================================================================
//  Main Memory BTree Implementation
// ==========================================================================
FMainMemoryBTree::FMainMemoryBTree(TableType type, int64_t maxSize, bool sortedBuffer) {
  switch (type) {
    case  LINEORDER_PK_SORT:
      if (sortedBuffer) {
        _impl = new FMainMemoryBTreeImplSorted<Lineorder::PKType>(type, sizeof(Lineorder), maxSize);
      } else {
        _impl = new FMainMemoryBTreeImplUnsorted<Lineorder::PKType>(type, sizeof(Lineorder), maxSize);
      }
      break;
    case  CUSTOMER_PK_SORT:
      if (sortedBuffer) {
        _impl = new FMainMemoryBTreeImplSorted<Customer::PKType>(type, sizeof(Customer), maxSize);
      } else {
        _impl = new FMainMemoryBTreeImplUnsorted<Customer::PKType>(type, sizeof(Customer), maxSize);
      }
      break;
    case  SUPPLIER_PK_SORT:
      if (sortedBuffer) {
        _impl = new FMainMemoryBTreeImplSorted<Supplier::PKType>(type, sizeof(Supplier), maxSize);
      } else {
        _impl = new FMainMemoryBTreeImplUnsorted<Supplier::PKType>(type, sizeof(Supplier), maxSize);
      }
      break;
    case  PART_PK_SORT:
      if (sortedBuffer) {
        _impl = new FMainMemoryBTreeImplSorted<Part::PKType>(type, sizeof(Part), maxSize);
      } else {
        _impl = new FMainMemoryBTreeImplUnsorted<Part::PKType>(type, sizeof(Part), maxSize);
      }
      break;
    case  DATE_PK_SORT:
      if (sortedBuffer) {
        _impl = new FMainMemoryBTreeImplSorted<Date::PKType>(type, sizeof(Date), maxSize);
      } else {
        _impl = new FMainMemoryBTreeImplUnsorted<Date::PKType>(type, sizeof(Date), maxSize);
      }
      break;
    case MV_PROJECTION:
      if (sortedBuffer) {
        _impl = new FMainMemoryBTreeImplSorted<MVProjection::PKType>(type, sizeof(MVProjection), maxSize);
      } else {
        _impl = new FMainMemoryBTreeImplUnsorted<MVProjection::PKType>(type, sizeof(MVProjection), maxSize);
      }
      break;
    default:
      // unsupported type
      assert (false);
      throw std::exception();
  }
}

FMainMemoryBTree::~FMainMemoryBTree() {
  delete _impl;
}

bool FMainMemoryBTree::insert (const void *key, const void *data) {
  return _impl->insert(key, data);
}

long long FMainMemoryBTree::size () const {
  return _impl->size();
}

void FMainMemoryBTree::traverse(TraversalCallback callback, void *context) const {
  _impl->traverse(callback, context);
}
const void* FMainMemoryBTree::getSingleTupleByKey (const void *key) const {
  return _impl->getSingleTupleByKey(key);
}

void FMainMemoryBTree::finishInserts () {
  _impl->finishInserts();
}

void FMainMemoryBTree::dumpToNewRowStoreFile (FFileSignature &signature) const {
  _impl->dumpToNewRowStoreFile(signature);
}

int FMainMemoryBTree::getKeySize() const {
  return _impl->getKeySize();
}
int FMainMemoryBTree::getDataSize() const {
  return _impl->getDataSize();
}
TableType FMainMemoryBTree::getTableType() const {
  return _impl->getTableType();
}

bool FMainMemoryBTree::isSortedBuffer() const {
  return _impl->isSortedBuffer();
}
const void* FMainMemoryBTree::getUnsortedBuffer() const {
  return _impl->getUnsortedBuffer();
}
void FMainMemoryBTree::scanTuplesGreaterEqual (TupleCallback callback, void *context, const char *key) const {
  _impl->scanTuplesGreaterEqual(callback, context, key);
}


FMainMemoryBTreeImpl::FMainMemoryBTreeImpl(int keySize, int dataSize, TableType tableType, int64_t maxSize)
  : _keySize(keySize), _dataSize(dataSize), _tableType(tableType), _maxSize(maxSize), _finishedInserts(false)
{
  _keydataFunc = toKeyDataCompareFunc(tableType);
  _datadataFunc = toDataDataCompareFunc(tableType);
  _array = new char[dataSize * maxSize];
  ::memset (_array, 0, dataSize * maxSize);
  _tuples = 0;
}
FMainMemoryBTreeImpl::~FMainMemoryBTreeImpl() {
  delete[] _array;
}

void FMainMemoryBTreeImpl::insertTupleToArray (const void *key, const void *data) {
  assert (_tuples < _maxSize);
  assert (_finishedInserts == false);
  ::memcpy (_array + (_dataSize * _tuples), data, _dataSize);
  ++_tuples;
}

struct PageSignature {
  int pageId;
  int64_t beginningPos;
  string firstKey;
};

// context object for callback function in disk dump
struct DumpContext {
  int fileId;
  DirectFileOutputStream *fd;
  char *buffer;
  int bufferedPages;
  int currentPageId;
  int currentPageOffset;
  long long currentEntry;
  long long entryCount;
  int keySize;
  int dataSize;
  int entryPerLeafPage;
  vector<PageSignature> pageSignatures;
};

void dumpLeafPagesCallback (void *context, const void *key, const void *data) {
  DumpContext *dumpContext = reinterpret_cast<DumpContext*>(context);
  assert (dumpContext->currentEntry < dumpContext->entryCount);

  // do we need a new page?
  if (FDB_PAGE_SIZE - dumpContext->currentPageOffset < dumpContext->dataSize) {
    ++(dumpContext->currentPageId);
    dumpContext->currentPageOffset = 0;
    ++(dumpContext->bufferedPages);
  }

  if (dumpContext->currentPageOffset == 0) {
    VLOG(2) << "new page!";

    // check if we need to flush buffered pages
    assert (dumpContext->bufferedPages <= FDB_DISK_WRITE_BUFFER_PAGES);
    if (dumpContext->bufferedPages == FDB_DISK_WRITE_BUFFER_PAGES) {
      VLOG(2) << "flush!";
      dumpContext->fd->write (dumpContext->buffer, FDB_PAGE_SIZE * dumpContext->bufferedPages);
      dumpContext->bufferedPages = 0;
      ::memset (dumpContext->buffer, 0, FDB_DISK_WRITE_BUFFER_PAGES * FDB_PAGE_SIZE);
    }

    // write a page header.
    FPageHeader *header = reinterpret_cast<FPageHeader*> (
      dumpContext->buffer + (FDB_PAGE_SIZE * dumpContext->bufferedPages));
    header->magicNumber = MAGIC_NUMBER;
    header->fileId = dumpContext->fileId;
    header->pageId = dumpContext->currentPageId;
    header->level = 0;
    header->root = false;
    header->entrySize = dumpContext->dataSize;
    header->beginningPos = ((int64_t) dumpContext->currentPageId) * ((int64_t) dumpContext->entryPerLeafPage);
    int remainingCount = dumpContext->entryCount - dumpContext->currentEntry;
    if (remainingCount >= dumpContext->entryPerLeafPage) {
      header->count = dumpContext->entryPerLeafPage;
      header->lastSibling = false;
      VLOG(2) << "more page!";
    } else {
      // last leaf page!
      header->count = remainingCount;
      header->lastSibling = true;
      VLOG(2) << "last page!";
    }
    dumpContext->currentPageOffset = sizeof (FPageHeader);

    // adds this new page to the signature list.
    PageSignature sig;
    sig.beginningPos = header->beginningPos;
    sig.firstKey.assign(reinterpret_cast<const char*>(key), dumpContext->keySize);
    sig.pageId = dumpContext->currentPageId;
    dumpContext->pageSignatures.push_back (sig);
  }

  // write tuple data
  ::memcpy(dumpContext->buffer + (FDB_PAGE_SIZE * dumpContext->bufferedPages) + dumpContext->currentPageOffset, data, dumpContext->dataSize);
  (dumpContext->currentPageOffset) += dumpContext->dataSize;
  ++(dumpContext->currentEntry);
}

void dumpNonLeafPages (int currentLevel, DumpContext *dumpContext,
  int &rootPageStart, int &rootPageCount, int &rootPageLevel) {
  int entryPerNonLeafPage = (FDB_PAGE_SIZE - sizeof (FPageHeader)) / (dumpContext->keySize + sizeof(int));
  vector<PageSignature> higherPageSignatures;
  dumpContext->bufferedPages = 0;
  dumpContext->currentPageOffset = 0;
  dumpContext->currentEntry = 0;
  ::memset (dumpContext->buffer, 0, FDB_DISK_WRITE_BUFFER_PAGES * FDB_PAGE_SIZE);
  int entryCount = dumpContext->pageSignatures.size();
  dumpContext->entryCount = entryCount;

  int pageCount = entryCount / entryPerNonLeafPage + (entryCount % entryPerNonLeafPage != 0 ? 1 : 0);
  bool root = (pageCount <= FDB_MAX_ROOT_PAGES);
  if (root) {
    rootPageStart = dumpContext->currentPageId;
    rootPageCount = pageCount;
  }

  for (int i = 0; i < entryCount; ++i) {
    const PageSignature &cursig = dumpContext->pageSignatures[i];

    // do we need a new page?
    if (FDB_PAGE_SIZE - dumpContext->currentPageOffset < (int) (dumpContext->keySize + sizeof(int))) {
      ++(dumpContext->currentPageId);
      dumpContext->currentPageOffset = 0;
      ++(dumpContext->bufferedPages);
    }

    if (dumpContext->currentPageOffset == 0) {
      VLOG(2) << "new page!";

      // check if we need to flush buffered pages
      assert (dumpContext->bufferedPages <= FDB_DISK_WRITE_BUFFER_PAGES);
      if (dumpContext->bufferedPages == FDB_DISK_WRITE_BUFFER_PAGES) {
        VLOG(2) << "flush!";
        dumpContext->fd->write (dumpContext->buffer, FDB_PAGE_SIZE * dumpContext->bufferedPages);
        dumpContext->bufferedPages = 0;
        ::memset (dumpContext->buffer, 0, FDB_DISK_WRITE_BUFFER_PAGES * FDB_PAGE_SIZE);
      }

      // write a page header.
      FPageHeader *header = reinterpret_cast<FPageHeader*> (
        dumpContext->buffer + (FDB_PAGE_SIZE * dumpContext->bufferedPages));
      header->magicNumber = MAGIC_NUMBER;
      header->fileId = dumpContext->fileId;
      header->pageId = dumpContext->currentPageId;
      header->level = currentLevel;
      header->root = root;
      header->entrySize = dumpContext->keySize;
      header->beginningPos = cursig.beginningPos;
      int remainingCount = dumpContext->entryCount - dumpContext->currentEntry;
      if (remainingCount >= entryPerNonLeafPage) {
        header->count = entryPerNonLeafPage;
        header->lastSibling = false;
        VLOG(2) << "more page!";
      } else {
        header->count = remainingCount;
        header->lastSibling = true;
        VLOG(2) << "last page!";
      }
      dumpContext->currentPageOffset = sizeof (FPageHeader);

      // adds this new page to the signature list.
      PageSignature sig;
      sig.beginningPos = header->beginningPos;
      sig.firstKey = cursig.firstKey;
      sig.pageId = dumpContext->currentPageId;
      higherPageSignatures.push_back (sig);
    }

    // write page entries
    ::memcpy(dumpContext->buffer + (FDB_PAGE_SIZE * dumpContext->bufferedPages) + dumpContext->currentPageOffset, cursig.firstKey.data(), dumpContext->keySize);
    (dumpContext->currentPageOffset) += dumpContext->keySize;
    ::memcpy(dumpContext->buffer + (FDB_PAGE_SIZE * dumpContext->bufferedPages) + dumpContext->currentPageOffset, &(cursig.pageId), sizeof(int));
    (dumpContext->currentPageOffset) += sizeof(int);
    ++(dumpContext->currentEntry);
  }

  VLOG(2) << "flushing last pages...";
  if (dumpContext->currentPageOffset > 0) {
    ++(dumpContext->bufferedPages);
    dumpContext->currentPageOffset = 0;
    ++(dumpContext->currentPageId);
  }
  if (dumpContext->bufferedPages > 0) {
    dumpContext->fd->write(dumpContext->buffer, FDB_PAGE_SIZE * dumpContext->bufferedPages);
  }
  if (root) {
    VLOG(2) << "last non-leaf level";
    assert (dumpContext->currentPageId == rootPageStart + rootPageCount);
    rootPageLevel = currentLevel;
  } else {
    VLOG(2) << "recurse for higher level";
    dumpContext->pageSignatures = higherPageSignatures;
    dumpNonLeafPages (currentLevel + 1, dumpContext, rootPageStart, rootPageCount, rootPageLevel);
  }
}

void FMainMemoryBTreeImpl::dumpToNewRowStoreFile (FFileSignature &signature) const {
  assert (signature.fileId > 0);
  assert (signature.filepath != NULL);
  std::string filepath (signature.filepath);
  LOG(INFO) << "dumping an on-memory btree to a new file " << filepath << "...";

  StopWatch watch;
  watch.init();
  if (std::remove((filepath).c_str()) == 0) {
    LOG(INFO) << "deleted existing file " << (filepath) << ".";
  }

  // dump the btree. start from leaf pages
  scoped_ptr<DirectFileOutputStream> fd(new DirectFileOutputStream(filepath, FDB_USE_DIRECT_IO));

  // use the traverse method of BTree to write down leaf pages
  assert (FDB_PAGE_SIZE % FDB_DIRECT_IO_ALIGNMENT == 0);
  ScopedMemoryForIO bufferPtr(FDB_DISK_WRITE_BUFFER_PAGES * FDB_PAGE_SIZE, FDB_DIRECT_IO_ALIGNMENT, FDB_USE_DIRECT_IO);
  void *buffer = bufferPtr.get();
  ::memset (buffer, 0, FDB_DISK_WRITE_BUFFER_PAGES * FDB_PAGE_SIZE);
  DumpContext context;
  context.fileId = signature.fileId;
  context.fd = fd.get();
  context.buffer = (char*) buffer;
  context.bufferedPages = 0;
  context.currentPageId = 0;
  context.currentPageOffset = 0;
  context.currentEntry = 0;
  context.entryCount = size();
  context.keySize = getKeySize();
  context.dataSize = getDataSize();
  context.entryPerLeafPage = (FDB_PAGE_SIZE - sizeof (FPageHeader)) / context.dataSize;
  context.pageSignatures.reserve ((context.entryCount / context.entryPerLeafPage) + 10);
  traverse(dumpLeafPagesCallback, &context);

  // flush last leaf pages
  if (context.currentPageOffset > 0) {
    ++(context.bufferedPages);
    context.currentPageOffset = 0;
    ++(context.currentPageId);
  }
  if (context.bufferedPages > 0) {
    fd->write (buffer, FDB_PAGE_SIZE * context.bufferedPages);
  }
  int leafPageCount = context.pageSignatures.size();
  assert (leafPageCount == context.currentPageId);
  LOG(INFO) << "finished writing " << leafPageCount << " leaf pages.";

  // then, construct non-leaf pages from context.pageSignatures
  int rootPageStart = 0, rootPageCount = 0, rootPageLevel = 0;
  dumpNonLeafPages(1, &context, rootPageStart, rootPageCount, rootPageLevel);

  int totalPageCount = context.currentPageId;
  LOG(INFO) << "finished writing " << totalPageCount << " pages in total(" << (totalPageCount - leafPageCount) << " non-leaf pages).";

  // done. flush and close
  fd->sync();
  fd->close();

  signature.keyEntrySize = getKeySize();
  signature.leafEntrySize = getDataSize();
  signature.keyCompareFuncType = toKeyCompareFuncType(getTableType());
  signature.pageCount = totalPageCount;
  signature.rootPageStart = rootPageStart;
  signature.rootPageCount = rootPageCount;
  signature.rootPageLevel = rootPageLevel;
  signature.tableType = getTableType();

  watch.stop();
  LOG(INFO) << "completed writing. " << watch.getElapsed() << " micsosec";
}


// ==========================================================================
//  Disk-based Read-only BTree Implementation
// ==========================================================================
FReadOnlyDiskBTree::FReadOnlyDiskBTree(FBufferPool *bufferpool, const FFileSignature &signature) {
  _impl = new FReadOnlyDiskBTreeImpl(bufferpool, signature);
}
FReadOnlyDiskBTree::~FReadOnlyDiskBTree() {
  delete _impl;
}
const FFileSignature& FReadOnlyDiskBTree::getFileSignature() const {
  return _impl->getFileSignature();
}

const char* FReadOnlyDiskBTree::getSingleTupleByKey (const char *key) {
  return _impl->getSingleTupleByKey(key);
}

void FReadOnlyDiskBTree::scanAllTuples (TupleCallback callback, void *context) {
  _impl->scanAllTuples(callback, context);
}
void FReadOnlyDiskBTree::scanTuplesGreaterEqual (TupleCallback callback, void *context, const char *key) {
  _impl->scanTuplesGreaterEqual(callback, context, key);
}

FReadOnlyDiskBTreeImpl::FReadOnlyDiskBTreeImpl (FBufferPool *bufferpool, const FFileSignature &signature)
  : _bufferpool (bufferpool),
    _signature(signature),
    _empty (signature.pageCount == 0),
    _compfunc (toKeyCompareFunc(signature.keyCompareFuncType)),
    _compfuncForLeaf (toKeyDataCompareFunc(signature.tableType))
{
  if (_empty) {
    LOG(INFO) << "this btree is emptry";
  }
}

int FReadOnlyDiskBTreeImpl::getFirstMatchingLeafPageId(
  int currentLevel, int fromPageId, const char *key, bool equalitySearch) {
  assert (currentLevel <= _signature.rootPageLevel);
  assert (currentLevel > 0);

  // keys are "first"-keys, so the last page that
  // has less key than the searched key might have some tuple matching the search.
  int lastLessKeyPointsTo = -1;
  int nextSearchFrom = -1;
  bool needsToReadNextPage = true;

  for (int pageId = fromPageId; needsToReadNextPage; ++pageId) {
    assert (pageId < _signature.pageCount);
    const char* data = _bufferpool->readPage(_signature, pageId);
    const FPageHeader *header = reinterpret_cast<const FPageHeader*>(data);
    checkNonLeafPageHeader (header, pageId, currentLevel);
    for (int j = 0; j < header->count; ++j) {
      int offset = sizeof(FPageHeader) + j * (header->entrySize + sizeof(int));
      const char *curKey = data + offset;
      int pointedPageId = *(reinterpret_cast<const int*>(data + offset + header->entrySize));
      assert (pointedPageId >= 0);
      int compResult = _compfunc (key, curKey);
      if (compResult > 0) {
        lastLessKeyPointsTo = pointedPageId;
        // keep reading
      } else if (compResult == 0) {
        // found an equal key. so, the first matching tuples should be in this page or
        // one earlier page (lastLessKeyPointsTo).
        if (lastLessKeyPointsTo >= 0) {
          nextSearchFrom = lastLessKeyPointsTo;
        } else {
          // this page was the first page.
          nextSearchFrom = pointedPageId;
        }
        needsToReadNextPage = false;
        break; 
      } else {
        // now we found a key that is equal or greater than the searched key.
        if (lastLessKeyPointsTo >= 0) {
          // then, the last page might have matching tuples
          nextSearchFrom = lastLessKeyPointsTo;
        } else {
          if (equalitySearch) {
            // this page was the first page, but it has larger first-key.
            // so, there is no hope to find matching tuples
            nextSearchFrom = -1;
          } else {
            // if equalitySearch is false, this returns the first page that has
            // equal OR greater key, 
            nextSearchFrom = pointedPageId;
          }
        }
        needsToReadNextPage = false;
        break;
      }
    }
    // we have read all pages in this level
    if (header->lastSibling) {
      needsToReadNextPage = false;
    }
  }

  if (lastLessKeyPointsTo >= 0 && nextSearchFrom < 0) {
    // if we found a key less than curkey, but not a key greater than,
    // this means the less key was the last key. as page-key is a first-key,
    // we should start searching from the page.
    nextSearchFrom = lastLessKeyPointsTo;
  }
  if (nextSearchFrom < 0) {
    return -1; // no possible matching tuples
  }

  if (currentLevel > 1) {
    // there is an intermediate level down below this level. recurse!
    return getFirstMatchingLeafPageId(currentLevel - 1, nextSearchFrom, key, equalitySearch);
  } else {
    // next level is a leaf page, so the searching is done
    return nextSearchFrom;
  }
}

const char* FReadOnlyDiskBTreeImpl::getSingleTupleByKey (const char *key) {
  if (_empty) return NULL;

  int leafPageId = getFirstMatchingLeafPageId (_signature.rootPageLevel, _signature.rootPageStart, key, true);
  if (leafPageId < 0) return NULL;

  for (int pageId = leafPageId;; ++pageId) {
    assert (pageId < _signature.pageCount);
    const char* data = _bufferpool->readPage(_signature, pageId);
    const FPageHeader *header = reinterpret_cast<const FPageHeader*>(data);
    checkLeafPageHeader(header, pageId);
    for (int j = 0; j < header->count; ++j) {
      int offset = sizeof(FPageHeader) + j * header->entrySize;
      const char *curData = data + offset;
      int compResult = _compfuncForLeaf (key, curData);
      if (compResult > 0) {
        // keep reading
      } else if (compResult == 0) {
        return curData;
      } else {
        return NULL;
      }
    }
    if (header->lastSibling) {
      break;
    }
  }

  // not found
  return NULL;
}

void FReadOnlyDiskBTreeImpl::scanAllTuples (TupleCallback callback, void *context) {
  if (_empty) return;

  VLOG(2) << "start reading";
  bool needsToReadNext = true;
  for (int pageId = 0; needsToReadNext; ++pageId) {
    assert (pageId < _signature.pageCount);
    // Minor TODO: we should use readPage's' here to improve performance
    const char* data = _bufferpool->readPage(_signature, pageId);
    const FPageHeader *header = reinterpret_cast<const FPageHeader*>(data);
    checkLeafPageHeader(header, pageId);
    for (int j = 0; j < header->count; ++j) {
      int offset = sizeof(FPageHeader) + j * header->entrySize;
      const char* tuple = data + offset;
      TupleCallbackRet ret = callback(context, tuple);
      if (ret == TUPLE_CALLBACK_OK) {
        // keep going
      } else if (ret == TUPLE_CALLBACK_QUIT) {
        VLOG(2) << "callback quit";
        needsToReadNext = false;
        break;
      } else {
        LOG(ERROR) << "some error happened. ret=" << ret;
        needsToReadNext = false;
        break;
      }
    }
    if (header->lastSibling) {
      break;
    }
  }
  VLOG(2) << "reading ended";
}

void FReadOnlyDiskBTreeImpl::scanTuplesGreaterEqual (TupleCallback callback, void *context, const char *key) {
  if (_empty) return;

  int leafPageId = getFirstMatchingLeafPageId (_signature.rootPageLevel, _signature.rootPageStart, key, false);
  if (leafPageId < 0) return;

  VLOG(2) << "start reading";
  bool needsToReadNext = true;
  bool canSkipLessThanCheck = false;
  for (int pageId = leafPageId; needsToReadNext; ++pageId) {
    assert (pageId < _signature.pageCount);
    // Minor TODO: we should use readPage's' here to improve performance
    const char* data = _bufferpool->readPage(_signature, pageId);
    const FPageHeader *header = reinterpret_cast<const FPageHeader*>(data);
    checkLeafPageHeader(header, pageId);
    for (int j = 0; j < header->count; ++j) {
      int offset = sizeof(FPageHeader) + j * header->entrySize;
      const char* tuple = data + offset;
      // skip tuples whose key is less than the searched key
      if (canSkipLessThanCheck || _compfuncForLeaf (key, tuple) <= 0) {
        canSkipLessThanCheck = true; // once reached here, all tuples should be equal or greater than the key
        assert (_compfuncForLeaf (key, tuple) <= 0); // but check it again in DEBUG mode
        TupleCallbackRet ret = callback(context, tuple);
        if (ret == TUPLE_CALLBACK_OK) {
          // keep going
        } else if (ret == TUPLE_CALLBACK_QUIT) {
          VLOG(2) << "callback quit";
          needsToReadNext = false;
          break;
        } else {
          LOG(ERROR) << "some error happened. ret=" << ret;
          needsToReadNext = false;
          break;
        }
      }
    }
    if (header->lastSibling) {
      break;
    }
  }
  VLOG(2) << "read ended";
}


void FReadOnlyDiskBTreeImpl::checkNonLeafPageHeader(const FPageHeader *header, int pageId, int currentLevel) {
  assert (header->magicNumber == MAGIC_NUMBER);
  assert (header->pageId == pageId);
  assert (header->root == (currentLevel == _signature.rootPageLevel));
  assert (header->level == currentLevel);
  assert (header->fileId == _signature.fileId);
  assert (header->entrySize == _signature.keyEntrySize);
  assert (header->count > 0);
}

void FReadOnlyDiskBTreeImpl::checkLeafPageHeader (const FPageHeader *header, int pageId) {
  assert (header->magicNumber == MAGIC_NUMBER);
  assert (header->pageId == pageId);
  assert (header->root == false);
  assert (header->level == 0);
  assert (header->fileId == _signature.fileId);
  assert (header->entrySize == _signature.leafEntrySize);
  assert (header->count > 0);
}

} //fdb
