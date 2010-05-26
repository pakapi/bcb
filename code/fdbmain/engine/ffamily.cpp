#include "fengine.h"
#include "ffamilyimpl.h"
#include "../io/fis.h"
#include "../storage/fbtree.h"
#include "../storage/fbufferpool.h"
#include "../storage/ffile.h"
#include "../storage/ffilesig.h"
#include "../storage/fkeycomp.h"
#include "../storage/fpage.h"
#include "../util/stopwatch.h"
#include <algorithm>
#include <cassert>
#include <sstream>

#include <glog/logging.h>
#include <boost/shared_ptr.hpp>
#include <boost/scoped_ptr.hpp>

namespace fdb {
// ==========================================================================
//  Proxies
// ==========================================================================
FFamily::FFamily (const std::string &name, TableType type, bool cstore) : _impl (new FFamilyImpl(name, type, cstore)) {
}

FFamily::~FFamily () {
  delete _impl;
}

const std::vector<std::string>& FFamily::getOnDiskFractures () const {
  return _impl->getOnDiskFractures();
}
void FFamily::addOnDiskFracture (const std::string &name) {
  _impl->addOnDiskFracture(name);
}
bool FFamily::eraseOnDiskFracture(const std::string &name) {
  return _impl->eraseOnDiskFracture(name);
}
FMainMemoryBTree* FFamily::getCurrentFracture() {
  return _impl->getCurrentFracture();
}
void FFamily::setCurrentFracture(FMainMemoryBTree *fracture) {
  _impl->setCurrentFracture(fracture);
}
const std::string& FFamily::getName() const {
  return _impl->getName();
}
TableType FFamily::getTableType () const {
  return _impl->getTableType ();
}
bool FFamily::isCStore () const {
  return _impl->isCStore ();
}
std::string FFamily::mergeFractures (FEngine *engine, const std::vector<std::string> &fractureNames, bool deleteOldFractures, long long mergeBufferSize) {
  return _impl->mergeFractures (engine, fractureNames, deleteOldFractures, mergeBufferSize);
}


// ==========================================================================
//  Implementation General
// ==========================================================================
const std::vector<std::string>& FFamilyImpl::getOnDiskFractures () const {
  return _fractures;
}
void FFamilyImpl::addOnDiskFracture (const std::string &name) {
  assert (std::find(_fractures.begin(), _fractures.end(), name) == _fractures.end());
  _fractures.push_back(name);
  ++_nextFractureId;
}
bool FFamilyImpl::eraseOnDiskFracture(const std::string &name) {
  std::vector<std::string>::iterator iter = std::find(_fractures.begin(), _fractures.end(), name);
  if (iter == _fractures.end()) {
    return false;
  } else {
    _fractures.erase (iter);
    return true;
  }
}

FMainMemoryBTree* FFamilyImpl::getCurrentFracture() {
  return _current;
}
void FFamilyImpl::setCurrentFracture(FMainMemoryBTree *fracture) {
  _current = fracture;
}
const std::string& FFamilyImpl::getName() const {
  return _name;
}
TableType FFamilyImpl::getTableType () const {
  return _type;
}
bool FFamilyImpl::isCStore () const {
  return _cstore;
}

std::string FFamilyImpl::mergeFractures (FEngine *engine, const std::vector<std::string> &fractureNames, bool deleteOldFractures, long long mergeBufferSize) {
  if (_cstore) {
    return mergeCStoreFractures (engine, fractureNames, deleteOldFractures, mergeBufferSize);
  } else {
    return mergeBTreeFractures (engine, fractureNames, deleteOldFractures, mergeBufferSize);
  }
}

// ==========================================================================
//  BTree Merging Fractures
// ==========================================================================
struct FractureReadBuffer {
public:
  FractureReadBuffer (FBufferPool *bufferpool, const FFileSignature &signature, int bufferSize, int tupleSize)
    : _bufferpool(bufferpool), _signature (signature), _bufferSize (bufferSize), _tupleSize(tupleSize) {
    _buffer = reinterpret_cast<char*>(DirectFileStream::allocateMemoryForIO (bufferSize * FDB_PAGE_SIZE, FDB_DIRECT_IO_ALIGNMENT, FDB_USE_DIRECT_IO));
    _nextPageId = 0;
    _currentPageInBuffer = 0;
    _currentPageCountInBuffer = 0;
    _currentTupleInPage = 0;
    _currentTupleCountInPage = 0;
    readBulk ();
  }
  ~FractureReadBuffer () {
    DirectFileStream::deallocateMemoryForIO(FDB_USE_DIRECT_IO, _buffer);
  }
  bool hasCurrent () {
    return _currentTupleInPage < _currentTupleCountInPage && _currentPageInBuffer < _currentPageCountInBuffer;
  }
  inline const char* getCurrent () {
    return _buffer + FDB_PAGE_SIZE * _currentPageInBuffer + sizeof (FPageHeader) + _tupleSize * _currentTupleInPage;
  }
  bool next () {
    if (_currentTupleInPage < _currentTupleCountInPage - 1) {
      ++_currentTupleInPage;
      return true;
    }
    ++_currentPageInBuffer;
    _currentTupleInPage = 0;
    if (_currentPageInBuffer < _currentPageCountInBuffer) {
      _currentTupleCountInPage = reinterpret_cast<FPageHeader*> (_buffer + FDB_PAGE_SIZE * _currentPageInBuffer)->count;
    } else {
      // no more page in buffer. has to read from disk
      readBulk ();
      if (_currentPageInBuffer >= _currentPageCountInBuffer) {
        LOG (INFO) << "finished reading " << _signature.filepath;
        return false; // no more page in disk
      }
    }
    return true;
  }
  void readBulk () {
    int pagesToRead = _bufferSize;
    if (_nextPageId + pagesToRead > _signature.rootPageStart) {
      pagesToRead = _signature.rootPageStart - _nextPageId;
    }
    if (pagesToRead > 0) {
      _bufferpool->readPages(_signature, _nextPageId, pagesToRead, _buffer);
      _nextPageId += pagesToRead;
      _currentPageCountInBuffer = pagesToRead;
      _currentPageInBuffer = 0;
      _currentTupleInPage = 0;
      _currentTupleCountInPage = reinterpret_cast<FPageHeader*> (_buffer)->count;
    }
  }
  char *_buffer;
  FBufferPool *_bufferpool;
  FFileSignature _signature;
  int _bufferSize;
  int _tupleSize;

  int _nextPageId;
  int _currentPageInBuffer;
  int _currentPageCountInBuffer;
  int _currentTupleInPage;
  int _currentTupleCountInPage;
private:
  FractureReadBuffer (const FractureReadBuffer&);
};
struct PageSignature {
  int pageId;
  int64_t beginningPos;
  std::string firstKey;
};
struct FractureWriteBuffer {
public:
  FractureWriteBuffer (int fileId, TableType type, DirectFileOutputStream *fd, int bufferSize, int tupleSize)
    : _fileId(fileId), _type(type), _fd (fd), _bufferSize (bufferSize), _tupleSize(tupleSize), _totalTuplesWritten(0) {
    _buffer = reinterpret_cast<char*>(DirectFileStream::allocateMemoryForIO (bufferSize * FDB_PAGE_SIZE, FDB_DIRECT_IO_ALIGNMENT, FDB_USE_DIRECT_IO));
    ::memset (_buffer, 0, bufferSize * FDB_PAGE_SIZE);
    _keySize = toKeySize (type);
    _keyBuffer = new char[_keySize];
    ::memset (_keyBuffer, 0, _keySize);
    _nextPageId = 0;
    _currentPageInBuffer = 0;
    _currentPageOffset = 0;
    _extractFunc = toExtractKeyFromTupleFunc(type);
  }
  ~FractureWriteBuffer () {
    DirectFileStream::deallocateMemoryForIO(FDB_USE_DIRECT_IO, _buffer);
    delete[] _keyBuffer;
  }
  void writePageHeader (int level, bool root, int entrySize, bool lastSibling) {
    FPageHeader *header = reinterpret_cast<FPageHeader*> (_buffer + (FDB_PAGE_SIZE * _currentPageInBuffer));
    header->magicNumber = MAGIC_NUMBER;
    header->fileId = _fileId;
    header->pageId = _currentPageSignature.pageId;
    assert (header->pageId >= 0);
    header->level = level;
    header->root = root;
    header->entrySize = entrySize;
    header->beginningPos = _currentPageSignature.beginningPos;
    assert ((_currentPageOffset - sizeof (FPageHeader)) % entrySize == 0);
    header->count = (_currentPageOffset - sizeof (FPageHeader)) / entrySize;
  }
  void flush () {
    VLOG(1) << "flush!";
    _fd->write (_buffer, _currentPageInBuffer * FDB_PAGE_SIZE);
    _currentPageInBuffer = 0;
    _nextPageId += _currentPageInBuffer;
    ::memset (_buffer, 0, _bufferSize * FDB_PAGE_SIZE);
  }
  // check if we need to flush buffered pages
  void flushIfFull () {
    assert (_currentPageInBuffer <= _bufferSize);
    if (_currentPageInBuffer == _bufferSize) {
      flush ();
    }
  }
  void addPageSignature (int64_t beginningPos, const void *key) {
    _currentPageSignature.beginningPos = beginningPos;
    _currentPageSignature.firstKey.assign(reinterpret_cast<const char*>(key), _keySize);
    _currentPageSignature.pageId = _nextPageId + _currentPageInBuffer;
    assert (_currentPageSignature.pageId >= 0);
    _pageSignatures.push_back (_currentPageSignature);
  }
  void write (const char *tuple) {
    // do we need a new page?
    if (FDB_PAGE_SIZE - _currentPageOffset < _tupleSize) {
      writePageHeader(0, false, _tupleSize, false);
      ++_currentPageInBuffer;
      _currentPageOffset = 0;
    }

    if (_currentPageOffset == 0) {
      VLOG(2) << "new leaf page!";
      flushIfFull ();
      _currentPageOffset = sizeof(FPageHeader);
      _extractFunc (tuple, _keyBuffer);
      addPageSignature(_totalTuplesWritten, _keyBuffer);
    }
    ::memcpy(_buffer + (FDB_PAGE_SIZE * _currentPageInBuffer) + _currentPageOffset, tuple, _tupleSize);
    _currentPageOffset += _tupleSize;
    ++_totalTuplesWritten;
  }
  void flushLastPages (int level, bool root, int entrySize) {
    VLOG(2) << "flushing last pages...";
    if (_currentPageOffset > 0) {
      writePageHeader(level, root, entrySize, true);
      ++_currentPageInBuffer;
    }
    if (_currentPageInBuffer > 0) {
      _fd->write (_buffer, FDB_PAGE_SIZE * _currentPageInBuffer);
      _nextPageId += _currentPageInBuffer;
    }
  }
  void dumpNonLeafPages (int currentLevel, int &rootPageStart, int &rootPageCount, int &rootPageLevel) {
    int entryPerNonLeafPage = (FDB_PAGE_SIZE - sizeof (FPageHeader)) / (_keySize + sizeof(int));
    std::vector<PageSignature> currentPageSignatures (_pageSignatures);
    _currentPageInBuffer = 0;
    _currentPageOffset = 0;
    ::memset (_buffer, 0, _bufferSize * FDB_PAGE_SIZE);
    int entryCount = currentPageSignatures.size();
    _pageSignatures.clear ();

    int pageCount = entryCount / entryPerNonLeafPage + (entryCount % entryPerNonLeafPage != 0 ? 1 : 0);
    bool root = (pageCount <= FDB_MAX_ROOT_PAGES);
    if (root) {
      rootPageStart = _nextPageId;
      rootPageCount = pageCount;
    }

    for (int i = 0; i < entryCount; ++i) {
      const PageSignature &cursig = currentPageSignatures[i];
      // do we need a new page?
      if (FDB_PAGE_SIZE - _currentPageOffset < (int) (_keySize + sizeof(int))) {
        writePageHeader(currentLevel, root, _keySize + sizeof(int), false);
        ++_currentPageInBuffer;
        _currentPageOffset = 0;
      }

      if (_currentPageOffset == 0) {
        VLOG(2) << "new non-leaf page!";
        flushIfFull ();
        _currentPageOffset = sizeof(FPageHeader);
        addPageSignature(cursig.beginningPos, cursig.firstKey.data());
      }

      // write page entries
      ::memcpy(_buffer + (FDB_PAGE_SIZE * _currentPageInBuffer) + _currentPageOffset, cursig.firstKey.data(), _keySize);
      _currentPageOffset += _keySize;
      ::memcpy(_buffer + (FDB_PAGE_SIZE * _currentPageInBuffer) + _currentPageOffset, &(cursig.pageId), sizeof(int));
      _currentPageOffset += sizeof(int);
    }

    flushLastPages (currentLevel, root, _keySize + sizeof(int));

    if (root) {
      VLOG(2) << "last non-leaf level";
      rootPageLevel = currentLevel;
    } else {
      VLOG(2) << "recurse for higher level";
      dumpNonLeafPages (currentLevel + 1, rootPageStart, rootPageCount, rootPageLevel);
    }
  }

  char *_buffer;
  char *_keyBuffer;
  int _fileId;
  TableType _type;
  DirectFileOutputStream *_fd;
  int _bufferSize;
  int _keySize;
  int _tupleSize;
  ExtractKeyFromTupleFunc _extractFunc;

  int _nextPageId;
  int _currentPageInBuffer;
  int _currentPageOffset;
  int64_t _totalTuplesWritten;
  PageSignature _currentPageSignature;
  std::vector<PageSignature> _pageSignatures;
private:
  FractureWriteBuffer (const FractureWriteBuffer &);
};

std::string FFamilyImpl::mergeBTreeFractures (FEngine *engine, const std::vector<std::string> &fractureNames, bool deleteOldFractures, long long mergeBufferSize) {
  StopWatch watch;
  watch.init();

  // this method implements a simple sort-merge
  const size_t fractureCount = fractureNames.size();
  LOG (INFO) << "mergeBTreeFractures() start. merging " << fractureCount << " fractures into one...";

  // if we read tuples one by one, and if keys are not so skewed across fractures,
  // we have to pay disk seek costs for every page. it'd be too slow.
  // instead, we buffer a large number of pages to read sequentially.
  int bufferedPages = (mergeBufferSize / (fractureCount + 1)) / FDB_PAGE_SIZE; // +1 for output buffer
  LOG (INFO) << "buffered pages per fracture=" << bufferedPages;
  int tupleSize = toDataSize(_type);

  FSignatureSet &signatures = engine->getSignatureSet();
  std::vector<boost::shared_ptr<FractureReadBuffer> > bufferPtrs; // to keep the objects
  std::vector<FractureReadBuffer*> buffers;
  std::vector<bool> finished;
  size_t finishedFractures = 0;
  for (size_t i = 0; i < fractureCount; ++i) {
    // check the given fractureNames exists in the family
    bool found = std::find (_fractures.begin(), _fractures.end(), fractureNames[i]) != _fractures.end();
    if (!found) {
      LOG (ERROR) << "The fracture '" << fractureNames[i] << "'doesn't exist in the family " << _name;
    }
    const FFileSignature &signature = signatures.getFileSignature (fractureNames[i]);
    boost::shared_ptr<FractureReadBuffer> buffer (new FractureReadBuffer(engine->getBufferPool(), signature, bufferedPages, tupleSize));
    bufferPtrs.push_back (buffer);
    buffers.push_back (buffer.get());
    if (buffer->hasCurrent()) {
      finished.push_back (false);
    } else {
      finished.push_back (true);
      ++finishedFractures;
    }
  }

  // okay, start merging leaf pages
  const std::string &folder = engine->getDataFolder();
  std::stringstream str;
  str << folder << _name << "." << _nextFractureId;
  std::string filepath = str.str();

  if (std::remove((filepath).c_str()) == 0) {
    LOG(INFO) << "deleted existing file " << (filepath) << ".";
  }
  boost::scoped_ptr<DirectFileOutputStream> fd(new DirectFileOutputStream(filepath, FDB_USE_DIRECT_IO));
  int newFileId = signatures.issueNextFileId();
  FractureWriteBuffer writeBuffer(newFileId, _type, fd.get(), bufferedPages, tupleSize);

  DataDataCompareFunc func = toDataDataCompareFunc(_type);
  while (finishedFractures < fractureCount) {
    const char *minTuple = NULL;
    int minTupleIndex = -1;
    for (size_t i = 0; i < fractureCount; ++i) {
      if (finished[i]) continue;
      const char *tuple = buffers[i]->getCurrent();
      if (minTuple == NULL || func(tuple, minTuple) < 0) {
        minTuple = tuple;
        minTupleIndex = i;
      }
    }
    assert (minTuple);
    assert (minTupleIndex >= 0);
    writeBuffer.write(minTuple);
    if (writeBuffer._totalTuplesWritten % 1000000 == 0) {
      VLOG (1) << "writing " << writeBuffer._totalTuplesWritten << "...";
    }
    bool hasNext = buffers[minTupleIndex]->next ();
    if (!hasNext) {
      finished[minTupleIndex] = true;
      ++finishedFractures;
    }
  }

  // flush last leaf pages
  writeBuffer.flushLastPages (0, false, writeBuffer._tupleSize);

  int leafPageCount = writeBuffer._pageSignatures.size();
  LOG(INFO) << "finished writing " << leafPageCount << " leaf pages, " << writeBuffer._totalTuplesWritten << "tuples.";

  // then, construct non-leaf pages from context.pageSignatures
  int rootPageStart = 0, rootPageCount = 0, rootPageLevel = 0;
  writeBuffer.dumpNonLeafPages(1, rootPageStart, rootPageCount, rootPageLevel);

  int totalPageCount = writeBuffer._nextPageId;
  LOG(INFO) << "finished writing " << totalPageCount << " pages in total(" << (totalPageCount - leafPageCount) << " non-leaf pages).";

  // done. flush and close
  fd->sync();
  fd->close();

  FFileSignature signature;
  signature.fileId = newFileId;
  signature.totalTupleCount = writeBuffer._totalTuplesWritten;
  ::memcpy(signature.filepath, filepath.data(), filepath.size());
  signature.filepath[filepath.size()] = '\0';
  signature.keyEntrySize = writeBuffer._keySize;
  signature.leafEntrySize = tupleSize;
  signature.keyCompareFuncType = toKeyCompareFuncType(_type);
  signature.pageCount = totalPageCount;
  signature.rootPageStart = rootPageStart;
  signature.rootPageCount = rootPageCount;
  signature.rootPageLevel = rootPageLevel;
  signature.tableType = _type;

  signatures.addFileSignature(signature);
  addOnDiskFracture(signature.filepath);
  for (size_t i = 0; i < fractureCount; ++i) {
    signatures.removeFileSignature(fractureNames[i]);
    std::vector<std::string>::iterator iter = std::find (_fractures.begin(), _fractures.end(), fractureNames[i]);
    _fractures.erase (iter);
    if (deleteOldFractures) {
      if (std::remove((fractureNames[i]).c_str()) == 0) {
        LOG(INFO) << "deleted existing file " << (fractureNames[i]) << ".";
      }
    }
  }

  watch.stop();
  LOG (INFO) << "mergeBTreeFractures() end. " << watch.getElapsed() << " micsosec";
  return filepath;
}

std::string FFamilyImpl::mergeCStoreFractures (FEngine *engine, const std::vector<std::string> &fractureNames, bool deleteOldFractures, long long mergeBufferSize) {
}

} // fdb