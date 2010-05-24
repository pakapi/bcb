#include "fbufferpool.h"
#include "fbufferpoolimpl.h"
#include "fpage.h"
#include <cassert>

using namespace boost;
using namespace std;

namespace fdb {

void PoolEntry::clear () {
  if (data != NULL) {
    DirectFileStream::deallocateMemoryForIO(FDB_USE_DIRECT_IO, data); // this pointer might be allocated for O_DIRECT
    data = NULL;
  }
}


FBufferPoolImpl::FBufferPoolImpl (int maxPageCount) :
  _maxPageCount (maxPageCount),
  _entriesAutoPtr (new PoolEntry[maxPageCount]),
  _entries (_entriesAutoPtr.get()),
  _clockHand (0) {
  assert (maxPageCount > 0);
  LOG(INFO) << "created buffer pool: page count=" << _maxPageCount;
}

FBufferPoolImpl::~FBufferPoolImpl() {
  clear();
  LOG(INFO) << "destroyed buffer pool";
}

void FBufferPoolImpl::clear() {
  for (int i = 0; i < _maxPageCount; ++i) {
    _entries[i].clear();
  }
  _clockHand = 0;
  _idMap.clear();
  for (FileMapIter iter = _fileMap.begin(); iter != _fileMap.end(); ++iter) {
    iter->second.stream->close();
    delete iter->second.stream;
    iter->second.stream = NULL;
  }
  _fileMap.clear();
  LOG(INFO) << "cleared buffer pool";
}

DirectFileInputStream* FBufferPoolImpl::getOrOpenFile (const FFileSignature &signature) {
  FileMapIter iter = _fileMap.find (signature.fileId);
  if (iter != _fileMap.end()) {
    assert (iter->second.stream != NULL);
    return iter->second.stream;
  } else {
    // not opened yet -> open it
    FBufferedFileStatus newFile;
    newFile.signature = signature;
    newFile.stream = new DirectFileInputStream (signature.filepath, FDB_USE_DIRECT_IO);
    _fileMap [signature.fileId] = newFile;
    return newFile.stream;
  }
}

PoolEntry* FBufferPoolImpl::findEntry (int fileId, int pageId) const {
  FilePageId filePageId = toFilePageId(fileId, pageId);
  map<FilePageId, int>::const_iterator iter = _idMap.find(filePageId);
  if (iter == _idMap.end()) return NULL;
  int i = iter->second;
  assert (_entries[i].data != NULL);
  assert (_entries[i].fileId == fileId);
  assert (_entries[i].pageId == pageId);
  return &_entries[i];
}
void FBufferPoolImpl::addPage (int fileId, int pageId, char *data) {
  int location = -1;
  // find the place to put this page
  while (true) {
    if (_clockHand >= _maxPageCount) {
      _clockHand = 0;
    }
    if (_entries[_clockHand].data == NULL) {
      // found unused page. fine.
      location = _clockHand;
      ++_clockHand;
      break;
    } else {
      // hey, this page is in use, but...
      if (_entries[_clockHand].read) {
        // this page is recently read. go over this page, though the read flag is turned off.
        _entries[_clockHand].read = false;
        ++_clockHand;
        continue;
      } else {
        // this page is not recently read. evict this!
        FilePageId oldFilePageId = toFilePageId(_entries[_clockHand].fileId, _entries[_clockHand].pageId);
#ifndef NDEBUG
        size_t erased =
#endif// NDEBUG
          _idMap.erase (oldFilePageId);
        assert (erased > 0);
        _entries[_clockHand].clear();
        location = _clockHand;
        ++_clockHand;
        break;
      }
    }
  }
  assert (location != -1);
  PoolEntry &entry = _entries[location];
  assert (entry.data == NULL);
  entry.fileId = fileId;
  entry.pageId = pageId;
  entry.data = data;
  entry.read = true;
  FilePageId filePageId = toFilePageId(fileId, pageId);
  _idMap[filePageId] = location;
}

const char* FBufferPoolImpl::readPage (const FFileSignature &signature, int pageId) {
  // first, check whether the page is in the pool
  PoolEntry *entry = findEntry(signature.fileId, pageId);
  if (entry != NULL) {
    entry->read = true;
    return entry->data;
  }

  // the page was not in the pool yet, so read it from file.
  DirectFileInputStream *stream = getOrOpenFile (signature);
  stream->setNextLocation(((int64_t) pageId) * FDB_PAGE_SIZE);
  char *content = (char*) DirectFileStream::allocateMemoryForIO(FDB_PAGE_SIZE, FDB_DIRECT_IO_ALIGNMENT, FDB_USE_DIRECT_IO);
  stream->read(content, FDB_PAGE_SIZE);
  assert (reinterpret_cast<FPageHeader*>(content)->pageId == pageId);
  assert (reinterpret_cast<FPageHeader*>(content)->fileId == signature.fileId);
  addPage(signature.fileId, pageId, content);
  return content;
}
std::vector<const char*> FBufferPoolImpl::readPages (const FFileSignature &signature, int beginningPageId, int pageCount) {
  // so far does nothing special.
  // will do a huge sequential scan later.
  std::vector<const char*> ret;
  for (int i = 0; i < pageCount; ++i) {
    ret.push_back (readPage(signature, beginningPageId + i));
  }
  return ret;
}


FBufferPool::FBufferPool(int maxPageCount) {
  _impl = new FBufferPoolImpl(maxPageCount);
}
FBufferPool::~FBufferPool() {
  delete _impl;
  _impl = NULL;
}

void FBufferPool::clear () {
  _impl->clear();
}

char* FBufferPool::findPage (int fileId, int pageId) {
  PoolEntry *entry = _impl->findEntry(fileId, pageId);
  if (entry == NULL) return NULL;
  entry->read = true;
  return entry->data;
}
void FBufferPool::addPage (int fileId, int pageId, char *data) {
  _impl->addPage(fileId, pageId, data);
}

const char* FBufferPool::readPage (const FFileSignature &signature, int pageId) {
  return _impl->readPage(signature, pageId);
}

std::vector<const char*> FBufferPool::readPages (const FFileSignature &signature, int beginningPageId, int pageCount) {
  return _impl->readPages(signature, beginningPageId, pageCount);
}


} // fdb
