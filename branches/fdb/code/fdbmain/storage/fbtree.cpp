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
void FMainMemoryBTree::addAllToWriter(FBTreeWriter &writer) const {
  _impl->addAllToWriter(writer);
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

// ==========================================================================
//  Dump to disk
// ==========================================================================

FBTreeWriter::FBTreeWriter(FFileSignature &signature_, TableType type_, char *buffer_, int bufferSize_, int64_t tupleCount_, int keySize_, int dataSize_)
  : signature(signature_), fileId (signature.fileId), type(type_), extractFunc(toExtractKeyFromTupleFunc(type)),
    buffer(buffer_), bufferSize(bufferSize_), bufferedPages (0),
    currentPageId (0), currentPageOffset (0), currentTuple (0), tupleCount(tupleCount_),
    keySize(keySize_), dataSize(dataSize_),
    entryPerLeafPage ((FDB_PAGE_SIZE - sizeof (FPageHeader)) / dataSize),
    entryPerNonLeafPage (FDB_PAGE_SIZE - sizeof (FPageHeader) / (keySize + sizeof(int))),
    leafPageCount (0), rootPageStart (0), rootPageCount (0), rootPageLevel (0) {
  assert (signature.fileId > 0);
  assert (signature.getFilepath().size() > 0);
  if (std::remove(signature.getFilepath().c_str()) == 0) {
    LOG(INFO) << "deleted existing file " << signature.getFilepath() << ".";
  }
  fd = new DirectFileOutputStream(signature.getFilepath(), FDB_USE_DIRECT_IO);
  ::memset (buffer, 0, bufferSize * FDB_PAGE_SIZE);
  keyBuffer = new char[keySize];
  ::memset (keyBuffer, 0, keySize);
  pageSignatures.reserve ((tupleCount / entryPerLeafPage) + 10);
}
FBTreeWriter::~FBTreeWriter() {
  delete fd;
  delete[] keyBuffer;
}

// write a page header.
void FBTreeWriter::writePageHeader (int level, bool root, int entrySize, int64_t beginningPos, int64_t remainingCount, int entryPerPage) {
  FPageHeader *header = reinterpret_cast<FPageHeader*> (buffer + (FDB_PAGE_SIZE * bufferedPages));
  header->magicNumber = MAGIC_NUMBER;
  header->fileId = fileId;
  header->pageId = currentPageId;
  header->level = level;
  header->root = root;
  header->entrySize = entrySize;
  header->beginningPos = beginningPos;
  if (remainingCount >= entryPerPage) {
    header->count = entryPerPage;
    header->lastSibling = false;
    VLOG(2) << "more page!";
  } else {
    // last leaf page!
    header->count = remainingCount;
    header->lastSibling = true;
    VLOG(2) << "last page!";
  }
  currentPageOffset = sizeof (FPageHeader);
}
void FBTreeWriter::addTuple (const char *data) {
  assert (currentTuple < tupleCount);

  // do we need a new page?
  if (FDB_PAGE_SIZE - currentPageOffset < dataSize) {
    flipPage();
  }

  if (currentPageOffset == 0) {
    VLOG(2) << "new page!";
    flushIfFull ();

    int64_t beginningPos = ((int64_t) currentPageId) * ((int64_t) entryPerLeafPage);
    writePageHeader (0, false, dataSize, beginningPos, tupleCount - currentTuple, entryPerLeafPage);

    // adds this new page to the signature list.
    BTreePageSignature sig;
    sig.beginningPos = beginningPos;
    extractFunc(data, keyBuffer);
    sig.firstKey.assign(keyBuffer, keySize);
    sig.pageId = currentPageId;
    pageSignatures.push_back (sig);
  }

  // write tuple data
  ::memcpy(buffer + (FDB_PAGE_SIZE * bufferedPages) + currentPageOffset, data, dataSize);
  currentPageOffset += dataSize;
  ++currentTuple;
}

// check if we need to flush buffered pages
void FBTreeWriter::flushIfFull () {
  assert (bufferedPages <= bufferSize);
  if (bufferedPages == bufferSize) {
    flush();
  }
}
void FBTreeWriter::flush() {
  if (bufferedPages > 0) {
    VLOG(2) << "flush!";
    fd->write (buffer, FDB_PAGE_SIZE * bufferedPages);
    bufferedPages = 0;
    ::memset (buffer, 0, bufferSize * FDB_PAGE_SIZE);
  }
}
void FBTreeWriter::flipPage() {
  if (currentPageOffset > 0) {
    ++currentPageId;
    currentPageOffset = 0;
    ++bufferedPages;
  }
}

void FBTreeWriter::dumpNonLeafPages (int currentLevel) {
  vector<BTreePageSignature> oldPageSignatures (pageSignatures);
  pageSignatures.clear();
  bufferedPages = 0;
  currentPageOffset = 0;
  ::memset (buffer, 0, bufferSize * FDB_PAGE_SIZE);
  int currentEntry = 0;
  int entryCount = oldPageSignatures.size();

  int pageCount = entryCount / entryPerNonLeafPage + (entryCount % entryPerNonLeafPage != 0 ? 1 : 0);
  bool root = (pageCount <= FDB_MAX_ROOT_PAGES);
  if (root) {
    rootPageStart = currentPageId;
    rootPageCount = pageCount;
  }

  for (int i = 0; i < entryCount; ++i) {
    const BTreePageSignature &cursig = oldPageSignatures[i];

    // do we need a new page?
    if (FDB_PAGE_SIZE - currentPageOffset < (int) (keySize + sizeof(int))) {
      flipPage();
    }

    if (currentPageOffset == 0) {
      VLOG(2) << "new page!";
      flushIfFull ();

      writePageHeader (currentLevel, root, keySize + sizeof(int), cursig.beginningPos, entryCount - currentEntry, entryPerNonLeafPage);

      // adds this new page to the signature list.
      BTreePageSignature sig;
      sig.beginningPos = cursig.beginningPos;
      sig.firstKey = cursig.firstKey;
      sig.pageId = currentPageId;
      pageSignatures.push_back (sig);
    }

    // write page entries
    ::memcpy(buffer + (FDB_PAGE_SIZE * bufferedPages) + currentPageOffset, cursig.firstKey.data(), keySize);
    currentPageOffset += keySize;
    ::memcpy(buffer + (FDB_PAGE_SIZE * bufferedPages) + currentPageOffset, &(cursig.pageId), sizeof(int));
    currentPageOffset += sizeof(int);
    ++currentEntry;
  }

  VLOG(2) << "flushing last pages...";
  flipPage();
  flush ();
  if (root) {
    VLOG(2) << "last non-leaf level";
    assert (currentPageId == rootPageStart + rootPageCount);
    rootPageLevel = currentLevel;
  } else {
    VLOG(2) << "recurse for higher level";
    dumpNonLeafPages (currentLevel + 1);
  }
}

void FBTreeWriter::finishWriting () {
  // flush last leaf pages
  flipPage();
  flush();
  leafPageCount = pageSignatures.size();
  assert (leafPageCount == currentPageId);
  LOG(INFO) << "finished writing " << leafPageCount << " leaf pages.";

  // then, construct non-leaf pages from context.pageSignatures
  dumpNonLeafPages(1);

  totalPageCount = currentPageId;
  LOG(INFO) << "finished writing " << totalPageCount << " pages in total(" << (totalPageCount - leafPageCount) << " non-leaf pages).";

  // done. flush and close
  fd->sync();
  fd->close();
  updateFileSignature ();
}

void FBTreeWriter::updateFileSignature() {
  signature.totalTupleCount = tupleCount;
  signature.keyEntrySize = keySize;
  signature.leafEntrySize = dataSize;
  signature.keyCompareFuncType = toKeyCompareFuncType(type);
  signature.pageCount = totalPageCount;
  signature.leafPageCount = leafPageCount;
  signature.rootPageStart = rootPageStart;
  signature.rootPageCount = rootPageCount;
  signature.rootPageLevel = rootPageLevel;
  signature.tableType = type;
}


void FMainMemoryBTreeImpl::dumpToNewRowStoreFile (FFileSignature &signature) const {
  assert (signature.fileId > 0);
  assert (signature.filepathlen > 0);
  std::string filepath (signature.getFilepath());
  LOG(INFO) << "dumping an on-memory btree to a new file " << filepath << "...";

  StopWatch watch;
  watch.init();
  assert (FDB_PAGE_SIZE % FDB_DIRECT_IO_ALIGNMENT == 0);
  ScopedMemoryForIO bufferPtr(FDB_DISK_WRITE_BUFFER_PAGES * FDB_PAGE_SIZE, FDB_DIRECT_IO_ALIGNMENT, FDB_USE_DIRECT_IO);
  FBTreeWriter context (signature, _tableType, reinterpret_cast<char*>(bufferPtr.get()), FDB_DISK_WRITE_BUFFER_PAGES, size(), getKeySize(), getDataSize());
  addAllToWriter(context);
  context.finishWriting();
  signature = context.signature;

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

FReadOnlyDiskBTree::LeafPageIterator FReadOnlyDiskBTree::scanLeafPages () {
  return FReadOnlyDiskBTree::LeafPageIterator (_impl);
}

const char* FReadOnlyDiskBTree::getLeafPage (int pageId) {
  return _impl->getLeafPage(pageId);
}
int FReadOnlyDiskBTree::getLeafPageCount () const {
  return _impl->getLeafPageCount();
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
      const char *curKey = data + sizeof(FPageHeader) + j * (header->entrySize);
      int pointedPageId = *(reinterpret_cast<const int*>(curKey + _signature.keyEntrySize));
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
    const char* data = getLeafPage(pageId);
    const FPageHeader *header = reinterpret_cast<const FPageHeader*>(data);
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
    const char* data = getLeafPage(pageId);
    const FPageHeader *header = reinterpret_cast<const FPageHeader*>(data);
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
    const char* data = getLeafPage(pageId);
    const FPageHeader *header = reinterpret_cast<const FPageHeader*>(data);
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
const char* FReadOnlyDiskBTreeImpl::getLeafPage (int pageId) {
  const char* data = _bufferpool->readPage(_signature, pageId);
  const FPageHeader *header = reinterpret_cast<const FPageHeader*>(data);
  checkLeafPageHeader(header, pageId);
  return data;
}

int FReadOnlyDiskBTreeImpl::getLeafPageCount () const {
  return _signature.leafPageCount;
}

void FReadOnlyDiskBTreeImpl::checkNonLeafPageHeader(const FPageHeader *header, int pageId, int currentLevel) {
  assert (header->magicNumber == MAGIC_NUMBER);
  assert (header->pageId == pageId);
  assert (header->root == (currentLevel == _signature.rootPageLevel));
  assert (header->level == currentLevel);
  assert (header->fileId == _signature.fileId);
  assert (header->entrySize == (int) (_signature.keyEntrySize + sizeof(int)));
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

// ==========================================================================
//  Disk-based Read-only BTree Iterator
// ==========================================================================
FReadOnlyDiskBTree::LeafPageIterator::LeafPageIterator(FReadOnlyDiskBTreeImpl *impl_)
: currentPageId (0), currentTuple(0), impl (impl_) {
  const char* data = impl->getLeafPage(0);
  const FPageHeader *header = reinterpret_cast<const FPageHeader*>(data);
  tupleSize = header->entrySize;
  currentPage = data + sizeof (FPageHeader);
  currentPageTupleCount = header->count;
  leafPageCount = impl->getLeafPageCount();
}
void FReadOnlyDiskBTree::LeafPageIterator::readPage(bool forward) {
  if (forward) {
    ++currentPageId;
  } else {
    --currentPageId;
  }
  if (currentPageId < 0 || currentPageId >= leafPageCount) {
    currentPageTupleCount = 0;
    currentPage = NULL;
    currentTuple = 0;
    return;
  }
  const char* data = impl->getLeafPage(currentPageId);
  const FPageHeader *header = reinterpret_cast<const FPageHeader*>(data);
  tupleSize = header->entrySize;
  currentPage = data + sizeof (FPageHeader);
  currentPageTupleCount = header->count;
  if (forward) {
    currentTuple = 0;
  } else {
    currentTuple = currentPageTupleCount - 1;
  }
}

} //fdb
