#include "fengine.h"
#include "ffamilyimpl.h"
#include "../io/fis.h"
#include "../storage/fbtree.h"
#include "../storage/fbufferpool.h"
#include "../storage/fcstore.h"
#include "../storage/ffile.h"
#include "../storage/ffilesig.h"
#include "../storage/fkeycomp.h"
#include "../storage/fpage.h"
#include "../util/stopwatch.h"
#include "../util/hashmap.h"
#include <algorithm>
#include <cstdio>
#include <cassert>
#include <sstream>
#include <set>

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
    assert (bufferSize > 0);
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
    if (_nextPageId + pagesToRead > _signature.leafPageCount) {
      pagesToRead = _signature.leafPageCount - _nextPageId;
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
  int totalBufferedPages = mergeBufferSize / FDB_PAGE_SIZE;
  LOG (INFO) << "mergeBTreeFractures() start. merging " << fractureCount << " fractures into one with " << totalBufferedPages << " buffer pages...";

  // if we read tuples one by one, and if keys are not so skewed across fractures,
  // we have to pay disk seek costs for every page. it'd be too slow.
  // instead, we buffer a large number of pages to read sequentially.
  int tupleSize = toDataSize(_type);

  FSignatureSet &signatures = engine->getSignatureSet();
  std::vector<boost::shared_ptr<FractureReadBuffer> > bufferPtrs; // to keep the objects
  std::vector<FractureReadBuffer*> buffers;
  std::vector<bool> finished;
  size_t finishedFractures = 0;
  int64_t grandTotalTupleCount = 0; // divide buffers based on size of fracture
  for (size_t i = 0; i < fractureCount; ++i) {
    // check the given fractureNames exists in the family
    bool found = std::find (_fractures.begin(), _fractures.end(), fractureNames[i]) != _fractures.end();
    if (!found) {
      LOG (ERROR) << "The fracture '" << fractureNames[i] << "'doesn't exist in the family " << _name;
    }
    const FFileSignature &signature = signatures.getFileSignature (fractureNames[i]);
    grandTotalTupleCount += signature.totalTupleCount;
  }
  grandTotalTupleCount *= 2; // for output buffer
  assert (grandTotalTupleCount > 0);
  for (size_t i = 0; i < fractureCount; ++i) {
    const FFileSignature &signature = signatures.getFileSignature (fractureNames[i]);
    int bufferedPages = (int) ((double) totalBufferedPages * signature.totalTupleCount / grandTotalTupleCount);
    if (bufferedPages == 0) bufferedPages = 1;
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
  int outputBufferPages = totalBufferedPages / 2;
  if (outputBufferPages == 0) outputBufferPages = 1;
  FractureWriteBuffer writeBuffer(newFileId, _type, fd.get(), outputBufferPages, tupleSize);

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
  signature.leafPageCount = leafPageCount;
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
// ==========================================================================
//  CStore Merging Fractures
// ==========================================================================
// FractureReadBuffer wrapper for column-specific reading
struct FractureReadBufferColumn {
public:
  FractureReadBufferColumn (const FCStoreColumn &column, FBufferPool *bufferpool, const FFileSignature &signature, int bufferSize)
    : _column(column), _impl(new FractureReadBuffer (bufferpool, signature, bufferSize, 0)), _bufferSize(bufferSize) {
    _dictionary = NULL;
    _impl->readBulk();
    _currentCursor = (_impl->_buffer) + sizeof(FPageHeader);

    if (column.compression == DICTIONARY_COMPRESSED) {
      _dictionary = new std::vector<std::string>();
      int columnLength = column.maxLength;
      for (int j = 0; j < signature.rootPageCount; ++j) {
        const int pageId = j + signature.rootPageStart;
        const char *page = bufferpool->readPage(signature, pageId);
        const FPageHeader *header = reinterpret_cast<const FPageHeader*> (page);
        assert (header->root);
        for (int k = 0; k < header->count; ++k) {
          const char *cursor = page + sizeof (FPageHeader) + k * columnLength;
          _dictionary->push_back (std::string (cursor, columnLength));
        }
      }
      _dictionaryBits = signature.dictionaryBits;
      if (_dictionaryBits < 8) {
        if (_dictionaryBits == 1) {
          _mask = 0x1;
        } else if (_dictionaryBits == 2) {
          _mask = 0x3;
        } else {
          assert (_dictionaryBits == 4);
          _mask = 0xf;
        }
      }
      _currentBitOffset = 0;

      retrieveCurrentDictionaryValue ();
    } else if (column.compression == RLE_COMPRESSED) {
      retrieveRLEValue ();
    } else {
      assert (column.compression == UNCOMPRESSED);
      retrieveUncompressedValue();
    }
  }
  ~FractureReadBufferColumn () {
    delete _impl;
    if (_dictionary != NULL) delete _dictionary;
  }
  void retrieveCurrentDictionaryValue () {
    uint16_t index;
    if (_dictionaryBits == 16) {
      index = *reinterpret_cast<const uint16_t*>(_currentCursor);
    } else if (_dictionaryBits == 8) {
      index = *reinterpret_cast<const uint8_t*>(_currentCursor);
    } else {
      assert (_dictionaryBits < 8);
      unsigned char packedByte = *reinterpret_cast<const unsigned char*>(_currentCursor);
      index = (packedByte >> _currentBitOffset) & _mask;
    }
    assert (index < _dictionary->size());
    _currentValue = (*_dictionary)[index].data();
  }
  void retrieveRLEValue () {
    _currentValue = _currentCursor;
    _currentRemainingRunCount = *reinterpret_cast<const int*>(_currentCursor + _column.maxLength);
  }
  void retrieveUncompressedValue () {
    _currentValue = _currentCursor;
  }
  void retrieveNextValueInPage() {
    if (_column.compression == DICTIONARY_COMPRESSED) {
      _currentBitOffset += _dictionaryBits;
      if (_currentBitOffset >= 8) {
        _currentCursor += _currentBitOffset / 8;
        _currentBitOffset = _currentBitOffset % 8;
      }
      retrieveCurrentDictionaryValue ();
    } else if (_column.compression == RLE_COMPRESSED) {
      --_currentRemainingRunCount;
      if (_currentRemainingRunCount <= 0) {
        _currentCursor += _column.maxLength + sizeof(int);
        retrieveRLEValue ();
      }
    } else {
      assert (_column.compression == UNCOMPRESSED);
      _currentCursor += _column.maxLength;
      retrieveUncompressedValue();
    }
  }
  // this method assumes there is at least one tuple to be read.
  void next () {
    if (_impl->_currentTupleInPage < _impl->_currentTupleCountInPage - 1) {
      ++(_impl->_currentTupleInPage);
      retrieveNextValueInPage ();
      return;
    }
    ++(_impl->_currentPageInBuffer);
    _impl->_currentTupleInPage = 0;
    if (_impl->_currentPageInBuffer < _impl->_currentPageCountInBuffer) {
      _currentCursor = _impl->_buffer + FDB_PAGE_SIZE * _impl->_currentPageInBuffer + sizeof (FPageHeader);
    } else {
      _impl->readBulk ();
      assert (_impl->_currentPageInBuffer < _impl->_currentPageCountInBuffer);
    }
    if (_column.compression == DICTIONARY_COMPRESSED) {
      _currentBitOffset = 0;
      retrieveCurrentDictionaryValue ();
    } else if (_column.compression == RLE_COMPRESSED) {
      retrieveRLEValue ();
    } else {
      retrieveUncompressedValue();
    }
  }

  FCStoreColumn _column;
  FractureReadBuffer *_impl;
  int _bufferSize;

  const char *_currentValue;
  const char *_currentCursor;

  // for RLE
  int _currentRemainingRunCount;

  // for Dictionary Encoding
  std::vector<std::string> *_dictionary;
  int _dictionaryBits;
  unsigned char _mask;
  int _currentBitOffset; // for 1bit-4bit dictionary

private:
  FractureReadBufferColumn (const FractureReadBufferColumn&);
};
struct CStoreReadBuffer {
public:
  CStoreReadBuffer (FEngine *engine, const std::vector<FCStoreColumn> &columns, const std::string &fracture, int tupleSize)
    : _engine(engine), _bufferpool(engine->getBufferPool()), _columnCount(columns.size()), _columns(columns), _fracture (fracture), _tupleSize(tupleSize), _current (0) {
    _signatures = engine->getSignatureSet().getCStoreFileSignatures(engine->getDataFolder(), columns, fracture);
    _totalTupleCount = _signatures[0].totalTupleCount;
    _currentTuple = new char[tupleSize];
  }
  ~CStoreReadBuffer () {
    delete[] _currentTuple;
    for (size_t i = 0; i < _readBuffers.size(); ++i) {
      delete _readBuffers[i];
    }
    _readBuffers.clear();
  }
  int getSumPageCount () {
    int sumPageCount = 0;
    for (size_t i = 0; i < _signatures.size(); ++i) {
      sumPageCount += _signatures[i].pageCount;
    }
    return sumPageCount;
  }
  void assignBuffers (int bufferSize) {
    //further divide up the buffer for each column based on their file size
    int sumPageCount = getSumPageCount();
    for (size_t i = 0; i < _signatures.size(); ++i) {
      int columnBufferSize = bufferSize * _signatures[i].pageCount / sumPageCount;
      if (columnBufferSize == 0) columnBufferSize = 1;
      _readBuffers.push_back (new FractureReadBufferColumn(_columns[i], _bufferpool, _signatures[i], columnBufferSize));
    }

    for (size_t i = 0; i < _columnCount; ++i) {
      ::memcpy (_currentTuple + _columns[i].offset, _readBuffers[i]->_currentValue, _columns[i].maxLength);
    }
  }
  bool next () {
    ++_current;
    if (_current >= _totalTupleCount) return false;
    for (size_t i = 0; i < _columnCount; ++i) {
      _readBuffers[i]->next();
      ::memcpy (_currentTuple + _columns[i].offset, _readBuffers[i]->_currentValue, _columns[i].maxLength);
    }
    return true;
  }

  FEngine *_engine;
  FBufferPool *_bufferpool;
  size_t _columnCount;
  std::vector<FCStoreColumn> _columns;
  std::string _fracture;
  int _tupleSize;
  std::vector<FFileSignature> _signatures;
  int64_t _current;
  int64_t _totalTupleCount;
  char *_currentTuple;
  std::vector<FractureReadBufferColumn*> _readBuffers;
private:
  CStoreReadBuffer (const CStoreReadBuffer&);
};
#if 0
// dumping class for CStore. unlike read buffer, this doesn't reuse FractureWriteBuffer. too different.
struct FractureWriteBufferColumn {
public:
  FractureWriteBufferColumn (int fileId_, const FCStoreColumn &column_, DirectFileOutputStream *fd_, int bufferSize_, const std::vector<CStoreReadBuffer*> &readers, int columnIndex_)
    : fileId(fileId_), column(column_), fd (fd_), columnIndex(columnIndex_), bufferSize (bufferSize_) {
    buffer = reinterpretcast<char*>(DirectFileStream::allocateMemoryForIO (bufferSize * FDB_PAGE_SIZE, FDB_DIRECT_IO_ALIGNMENT, FDB_USE_DIRECT_IO));
    ::memset (buffer, 0, bufferSize * FDB_PAGE_SIZE);
    bufferedPages = 0;
    currentPageId = 0;
    currentPageOffset = 0;
    currentTuple = 0;

    runTotal = 0;
    currentRunCount = 0;
    currentRunBeginningPos = 0;
    currentRunValue = NULL;

    currentPackedByte = 0;
    currentBitOffset = 0;
    rootPageStart = 0;
    rootPageCount = 0;
    rootPageLevel = 0;
    leafPageCount = 0;

    entryInCurrentPage = 0;
    hashmap = NULL;
    if (column.compression == UNCOMPRESSED) {
      leafEntrySize = column.maxLength;
      entryPerLeafPage = (FDB_PAGE_SIZE - sizeof (FPageHeader)) / leafEntrySize;
    } else if (column.compression == RLE_COMPRESSED) {
      leafEntrySize = column.maxLength + sizeof(int); // value + runlength
      entryPerLeafPage = (FDB_PAGE_SIZE - sizeof (FPageHeader)) / leafEntrySize;
    } else {
      assert (column.compression == DICTIONARY_COMPRESSED);

      // create a new merged dictionary
      for (sizet i = 0; i < readers.size(); ++i) {
        FractureReadBufferColumn *reader = readers[i]->readBuffers[columnIndex];
        assert (reader->dictionary != NULL);
        mergedSortedSet.insert (reader->dictionary->begin(), reader->dictionary->end());
      }
      assert (mergedSortedSet.size() < (1 << 16));
      dictionarySize = mergedSortedSet.size();
      hashmap = new StringHashMap<uint16t>(column.maxLength, 16);
      std::set<std::string>::constiterator iter = mergedSortedSet.begin();
      for (sizet i = 0; i < dictionarySize; ++i) {
        assert (iter != mergedSortedSet.end());
        hashmap->insert(iter->data(), i);
        ++iter;
      }

      if (dictionarySize <= (1 << 1)) {
        dictionaryBits = 1;
        leafEntrySize = 1;
        entryPerLeafPage = (FDB_PAGE_SIZE - sizeof (FPageHeader)) * 8;
      } else if (dictionarySize <= (1 << 2)) {
        dictionaryBits = 2;
        leafEntrySize = 1;
        entryPerLeafPage = (FDB_PAGE_SIZE - sizeof (FPageHeader)) * 4;
      } else if (dictionarySize <= (1 << 4)) {
        dictionaryBits = 4;
        leafEntrySize = 1;
        entryPerLeafPage = (FDB_PAGE_SIZE - sizeof (FPageHeader)) * 2;
      } else if (dictionarySize <= (1 << 8)) {
        dictionaryBits = 8;
        leafEntrySize = 1;
        entryPerLeafPage = (FDB_PAGE_SIZE - sizeof (FPageHeader));
      } else {
        assert (dictionarySize <= (1 << 16));
        dictionaryBits = 16;
        leafEntrySize = 2;
        entryPerLeafPage = (FDB_PAGE_SIZE - sizeof (FPageHeader)) / 2;
      }
    }
  }
  ~FractureWriteBufferColumn () {
    DirectFileStream::deallocateMemoryForIO(FDB_USE_DIRECT_IO, buffer);
    if (hashmap != NULL) delete hashmap;
  }

  void updateFileSignature(FFileSignature &signature, TableType tableType, int64t tupleCount) const {
    signature.columnFile = true;
    signature.columnType = column.type;
    signature.columnMaxLength = column.maxLength;
    signature.columnIndex = columnIndex;
    signature.columnOffset = column.offset;
    signature.columnCompression = column.compression;

    signature.totalTupleCount = tupleCount;
    signature.keyEntrySize = 0; // no meaning in column store
    signature.leafEntrySize = 0; // no meaning in column store
    signature.keyCompareFuncType = KEY_CMP_INVALID;
    signature.rootPageStart = rootPageStart;
    signature.rootPageCount = rootPageCount;
    signature.rootPageLevel = rootPageLevel;
    signature.tableType = tableType;
    signature.pageCount = currentPageId;
    signature.leafPageCount = leafPageCount;
    signature.dictionaryBits = dictionaryBits;
    signature.dictionaryEntryCount = dictionarySize;
  }

  bool CStoreDumpContext::flushBufferIfNeeded() {
    // check if we need to flush buffered pages
    assert (currentPageOffset == 0);
    assert (bufferedPages <= FDB_DISK_WRITE_BUFFER_PAGES);
    if (bufferedPages == FDB_DISK_WRITE_BUFFER_PAGES) {
      flushBuffer();
      return true;
    }
    return false;
  }
  void flushBuffer() {
    assert (currentPageOffset == 0);
    assert (currentBitOffset == 0);
    assert (bufferedPages <= FDB_DISK_WRITE_BUFFER_PAGES);
    if (bufferedPages > 0) {
      VLOG(2) << "flush!";
      fd->write (buffer, FDB_PAGE_SIZE * bufferedPages);
      bufferedPages = 0;
      ::memset (buffer, 0, FDB_DISK_WRITE_BUFFER_PAGES * FDB_PAGE_SIZE);
    }
  }

  bool flipPageIfNeeded() {
    assert (entryInCurrentPage <= entryPerLeafPage);
    if (entryPerLeafPage == entryInCurrentPage) {
      flipPage();
      return true;
    }
    return false;
  }
  void flipPage() {
    if (currentPageOffset > 0) {
      ++(currentPageId);
      currentPageOffset = 0;
      entryInCurrentPage = 0;
      ++(bufferedPages);
      assert (bufferedPages <= FDB_DISK_WRITE_BUFFER_PAGES);
    }
  }

  void writeLeafPageHeader(int countInThisPage, bool lastSibling, int64_t beginningPos) {
    writePageHeader (countInThisPage, lastSibling, beginningPos, 0, false, leafEntrySize);
  }
  void writePageHeader(int countInThisPage, bool lastSibling, int64_t beginningPos, int level, bool root, int entrySize) {
    FPageHeader *header = reinterpret_cast<FPageHeader*> (buffer + (FDB_PAGE_SIZE * bufferedPages));
    header->magicNumber = MAGIC_NUMBER;
    header->fileId = fileId;
    header->pageId = currentPageId;
    header->level = level;
    header->root = root;
    header->entrySize = entrySize;
    header->count = countInThisPage;
    header->lastSibling = lastSibling;
    header->beginningPos = beginningPos;
    if (lastSibling) {
      VLOG(2) << "last page!";
    } else {
      VLOG(2) << "more page!";
    }
    currentPageOffset = sizeof (FPageHeader);
  }
  
  void writeLeafEntry(const char *entryData) {
    ::memcpy(buffer + (FDB_PAGE_SIZE * bufferedPages) + currentPageOffset, entryData, leafEntrySize);
    currentPageOffset += leafEntrySize;
    ++entryInCurrentPage;
    ++currentTuple;
  }
  
  void writeLeafEntryRLE(int runLength, const char *entryData) {
    assert ((int) sizeof(int) + column.maxLength == leafEntrySize);
    ::memcpy(buffer + (FDB_PAGE_SIZE * bufferedPages) + currentPageOffset, &runLength, sizeof(int));
    ::memcpy(buffer + (FDB_PAGE_SIZE * bufferedPages) + currentPageOffset + sizeof(int), entryData, column.maxLength);
    currentPageOffset += leafEntrySize;
    ++entryInCurrentPage;
  }
  
  void writeRootEntryRLE(int64_t beginningPos, int pageId) {
    ::memcpy(buffer + (FDB_PAGE_SIZE * bufferedPages) + currentPageOffset, &beginningPos, sizeof(int64_t));
    ::memcpy(buffer + (FDB_PAGE_SIZE * bufferedPages) + currentPageOffset + sizeof(int64_t), &pageId, sizeof(int));
    currentPageOffset += sizeof(int64_t) + sizeof(int);
    ++entryInCurrentPage;
  }
  
  void writeDictionary () {
    // flush last pages
    flipPage();
    flushBuffer();

    // then, write root (dictionary) pages
    int entriesInRootPage = (FDB_PAGE_SIZE - sizeof (FPageHeader)) / column.maxLength;
    rootPageStart = currentPageId;
    rootPageCount = (dictionarySize / entriesInRootPage) + (dictionarySize % entriesInRootPage == 0 ? 0 : 1);
    rootPageLevel = 1;
    for (int i = 0; i < rootPageCount; ++i) {
      bool lastSibling;
      int countInThisPage;
      if (i == rootPageCount - 1) {
        countInThisPage = dictionarySize - i * entriesInRootPage;
        assert (countInThisPage <= entriesInRootPage);
        assert (countInThisPage > 0);
        lastSibling = true;
      } else {
        countInThisPage = entriesInRootPage;
        lastSibling = false;
      }
      writePageHeader(countInThisPage, lastSibling, 0, 1, true, column.maxLength);
  
      char *p = buffer + (FDB_PAGE_SIZE * bufferedPages) + currentPageOffset;
      for (int j = 0; j < countInThisPage; ++j, p += column.maxLength) {
        ::memcpy(p, dictionaryEntries[j + i * entriesInRootPage], column.maxLength);
      }
      currentPageOffset += column.maxLength * countInThisPage;
      entryInCurrentPage += countInThisPage;
      flipPage();
      flushBufferIfNeeded();
    }
    VLOG(1) << "Wrote dictionary. " << dictionarySize << " entries. " << rootPageCount << " root pages";
    flipPage();
    flushBuffer();
  }
  
  void prepareForNewPageUniform () {
    flipPageIfNeeded();
    if (currentPageOffset == 0) {
      VLOG(2) << "new page!";
      flushBufferIfNeeded();
      int remainingCount = tupleCount - currentTuple;
      int countInThisPage;
      bool lastSibling;
      if (remainingCount >= entryPerLeafPage) {
        countInThisPage = entryPerLeafPage;
        lastSibling = false;
      } else {
        // uncompressed/dictionary file can simply calculate the remaining count
        countInThisPage = remainingCount;
        lastSibling = true;
      }
      writeLeafPageHeader(countInThisPage, lastSibling, currentTuple);
    }
  }
/*
// ========================================
//  Uncompressed Column
// ========================================
  void dumpUncompressedColumnCallback (void *context, const void *key, const void *data) {
    CStoreDumpContext *dumpContext = reinterpret_cast<CStoreDumpContext*>(context);
    assert (dumpContext->currentTuple < dumpContext->tupleCount);
    dumpContext->prepareForNewPageUniform();
    dumpContext->writeLeafEntry (((char*) data) + dumpContext->column.offset);
  }
  
  void dumpUncompressedColumn(CStoreDumpContext &context, const FMainMemoryBTree &btree) {
    btree.traverse(dumpUncompressedColumnCallback, &context);
  
    // flush last pages
    context.flipPage();
    context.flushBuffer();
  
    context.leafPageCount = context.currentPageId;
  }
// ========================================
//  RLE Compressed Column
// ========================================
  void flushCurrentRun (CStoreDumpContext *dumpContext) {
    // flush the last run
    dumpContext->flipPageIfNeeded();
    if (dumpContext->currentPageOffset == 0) {
      VLOG(2) << "new page!";
      dumpContext->flushBufferIfNeeded();
      // determining countInThisPage and whether it's last or not is difficult in RLE
      // this will be overwritten when this page turns out to be the last sibling
      dumpContext->writeLeafPageHeader(dumpContext->entryPerLeafPage, false, dumpContext->currentRunBeginningPos);
      dumpContext->pageBeginningPositions.push_back (dumpContext->currentRunBeginningPos);
      assert ((int) dumpContext->pageBeginningPositions.size() == dumpContext->currentPageId + 1);
    }
    if (dumpContext->currentRunValue != NULL) {
      dumpContext->writeLeafEntryRLE(dumpContext->currentRunCount, dumpContext->currentRunValue);
    }
  }
  void dumpRLECompressedColumnCallback (void *context, const void *key, const void *data) {
    CStoreDumpContext *dumpContext = reinterpret_cast<CStoreDumpContext*>(context);
    assert (dumpContext->currentTuple < dumpContext->tupleCount);
  
    // write RLE data only when the value changes
    const char *currentValue = ((char*) data) + dumpContext->column.offset;
    if (dumpContext->currentRunValue == NULL || ::memcmp(dumpContext->currentRunValue, currentValue, dumpContext->column.maxLength) != 0) {
      VLOG(2) << "new run!";
      flushCurrentRun (dumpContext);
  
      // Start a new run
      dumpContext->currentRunBeginningPos = dumpContext->currentTuple;
      dumpContext->currentRunValue = currentValue;
      dumpContext->currentRunCount = 1;
      ++(dumpContext->runTotal);
    } else {
      ++(dumpContext->currentRunCount);
    }
    ++(dumpContext->currentTuple);
  }
  
  void dumpRLECompressedColumn(CStoreDumpContext &context, const FMainMemoryBTree &btree) {
    btree.traverse(dumpRLECompressedColumnCallback, &context);
  
    // flush last run
    flushCurrentRun (&context);
    // overwrite count/lastSibling of the last page
    {
      FPageHeader *header = reinterpret_cast<FPageHeader*>(context.buffer + (FDB_PAGE_SIZE * context.bufferedPages));
      assert (header->lastSibling == false);
      assert (header->count == context.entryPerLeafPage);
      header->lastSibling = true;
      header->count = context.entryInCurrentPage;
    }
  
    // flush last pages
    context.flipPage();
    context.flushBuffer();
  
    context.leafPageCount = context.currentPageId;
  
    VLOG(1) << "in total " << context.runTotal << " runs";
  
    // then, write root pages for position search
    size_t rootEntrySize = sizeof(int64_t) + sizeof (int);
    int leafPageCount = context.pageBeginningPositions.size();
    assert (leafPageCount == context.currentPageId);
    int entriesInRootPage = (FDB_PAGE_SIZE - sizeof (FPageHeader)) / rootEntrySize;
    context.rootPageStart = context.currentPageId;
    context.rootPageCount = (leafPageCount / entriesInRootPage) + (leafPageCount % entriesInRootPage == 0 ? 0 : 1);
    context.rootPageLevel = 1;
    for (int i = 0; i < context.rootPageCount; ++i) {
      bool lastSibling;
      int countInThisPage;
      if (i == context.rootPageCount - 1) {
        countInThisPage = leafPageCount - i * entriesInRootPage;
        assert (countInThisPage <= entriesInRootPage);
        assert (countInThisPage > 0);
        lastSibling = true;
      } else {
        countInThisPage = entriesInRootPage;
        lastSibling = false;
      }
      context.writePageHeader(countInThisPage, lastSibling, context.pageBeginningPositions[i * entriesInRootPage], 1, true, rootEntrySize);
      for (int j = 0; j < countInThisPage; ++j) {
        int pageId = i * entriesInRootPage + j;
        int64_t beginningPos = context.pageBeginningPositions[pageId];
        context.writeRootEntryRLE(beginningPos, pageId);
      }
      context.flipPage();
      context.flushBufferIfNeeded();
    }
    VLOG(1) << "RLE " << context.rootPageCount << " root pages";
    context.flipPage();
    context.flushBuffer();
  }
  
// ========================================
//  Dictionary Encoded Column
// ========================================
  void flushCurrentPackedByte(CStoreDumpContext *dumpContext) {
    assert (dumpContext->currentBitOffset >= 0);
    assert (dumpContext->currentBitOffset <= 8);
    dumpContext->currentBitOffset = 0;
    *reinterpret_cast<unsigned char*>(dumpContext->buffer + (FDB_PAGE_SIZE * dumpContext->bufferedPages) + dumpContext->currentPageOffset) = dumpContext->currentPackedByte;
    dumpContext->currentPackedByte = 0;
    ++(dumpContext->currentPageOffset);
  }
  
  void dumpSmallDictionaryCompressedColumnCallback (void *context, const void *key, const void *data) {
    CStoreDumpContext *dumpContext = reinterpret_cast<CStoreDumpContext*>(context);
    assert (dumpContext->dictionaryBits <= 4);
    assert (dumpContext->currentTuple < dumpContext->tupleCount);
  
    if (dumpContext->currentBitOffset == 0) {
      dumpContext->prepareForNewPageUniform();
    }
  
    const char* value = ((char*) data) + dumpContext->column.offset;
    uint8_t foundIndex = dumpContext->dictionaryHashmap->find(value);
  
    // <=4bits have to pack to a byte.
    dumpContext->currentPackedByte |= (foundIndex << dumpContext->currentBitOffset);
    ++(dumpContext->entryInCurrentPage);
    (dumpContext->currentBitOffset) += dumpContext->dictionaryBits;
    if (dumpContext->currentBitOffset == 8) {
      flushCurrentPackedByte(dumpContext);
    }
    ++(dumpContext->currentTuple);
  }
  
  template <typename INT_TYPE>
  void dumpLargeDictionaryCompressedColumnCallback (void *context, const void *key, const void *data) {
    CStoreDumpContext *dumpContext = reinterpret_cast<CStoreDumpContext*>(context);
    assert (dumpContext->dictionaryBits >= 8);
    assert (dumpContext->currentTuple < dumpContext->tupleCount);
    assert (sizeof(INT_TYPE) == dumpContext->leafEntrySize);
  
    dumpContext->prepareForNewPageUniform();
    const char* value = ((char*) data) + dumpContext->column.offset;
    // simply write the current data, but in given length of int
    INT_TYPE foundIndex = dumpContext->dictionaryHashmap->find(value);
    dumpContext->writeLeafEntry(reinterpret_cast<char*>(&foundIndex));
  }
  
  void dumpDictionaryCompressedColumn(CStoreDumpContext &context, const FMainMemoryBTree &btree) {
  
  
    // flush last bits
    if (context.currentBitOffset != 0) {
      flushCurrentPackedByte(&context);
    }
    context.leafPageCount = context.currentPageId;
    context.writeDictionary();
  
    VLOG(2) << "Dumped Dictionary Compressed column";
  }
  */


  char *buffer;
  int fileId;
  FCStoreColumn column;
  int columnIndex;
  DirectFileOutputStream *fd;
  int bufferSize;

  int bufferedPages;
  int currentPageId;
  int currentPageInBuffer;
  int currentPageOffset;

  int leafEntrySize; // this is not same as column.maxLength with RLE/Dictionary encoding
  int entryInCurrentPage;
  int entryPerLeafPage;

  // for RLE
  int64t runTotal;
  int64t currentRunBeginningPos;
  int currentRunCount;
  const char *currentRunValue;
  std::vector<int64t> pageBeginningPositions;

  // for Dictionary Encoding
  StringHashMap<uint16t> *hashmap;
  std::set<std::string> mergedSortedSet; // to keep entity of strings
  int dictionarySize;
  int dictionaryBits;
  int currentBitOffset; // for 1bit-4bit dictionary
  unsigned char currentPackedByte;

  // for RLE/Dic
  int rootPageStart;
  int rootPageCount;
  int rootPageLevel;

private:
  FractureWriteBufferColumn (const FractureWriteBufferColumn &);
};
#endif

struct CStoreWriteBuffer {
public:

private:
  CStoreWriteBuffer (const CStoreWriteBuffer&);
};



std::string FFamilyImpl::mergeCStoreFractures (FEngine *engine, const std::vector<std::string> &fractureNames, bool deleteOldFractures, long long mergeBufferSize) {
  StopWatch watch;
  watch.init();

  const size_t fractureCount = fractureNames.size();
  int totalBufferedPages = mergeBufferSize / FDB_PAGE_SIZE;
  LOG (INFO) << "mergeCStoreFractures() start. merging " << fractureCount << " fractures into one with " << totalBufferedPages << " buffer pages...";

  std::vector<FCStoreColumn> columns = FCStoreUtil::getPhysicalDesignsOf(_type);
  const size_t columnCount = columns.size();
  LOG (INFO) << "columnCount=" << columnCount;

  int tupleSize = toDataSize(_type);

  // divide buffers based on each fracture's tuple count
  std::vector<boost::shared_ptr<CStoreReadBuffer> > readBufferPtrs;
  std::vector<CStoreReadBuffer*> readBuffers;
  int64_t grandTotalTupleCount = 0;
  std::vector<bool> finished;
  size_t finishedFractures = 0;
  for (size_t i = 0; i < fractureCount; ++i) {
    boost::shared_ptr<CStoreReadBuffer> ptr(new CStoreReadBuffer(engine, columns, fractureNames[i], tupleSize));
    readBufferPtrs.push_back (ptr);
    readBuffers.push_back (ptr.get());
    grandTotalTupleCount += ptr->_totalTupleCount;
  }
  grandTotalTupleCount *= 2; // for output buffer
  assert (grandTotalTupleCount > 0);
  for (size_t i = 0; i < fractureCount; ++i) {
    int bufferedPages = (int) ((double) totalBufferedPages * readBuffers[i]->_totalTupleCount / grandTotalTupleCount);
    if (bufferedPages == 0) bufferedPages = 1;
    readBuffers[i]->assignBuffers (bufferedPages);
    if (readBuffers[i]->_totalTupleCount > 0) {
      finished.push_back (false);
    } else {
      finished.push_back (true);
      ++finishedFractures;
    }
  }

  // sort-merge
  DataDataCompareFunc func = toDataDataCompareFunc(_type);
  while (finishedFractures < fractureCount) {
    const char *minTuple = NULL;
    int minTupleIndex = -1;
    for (size_t i = 0; i < fractureCount; ++i) {
      if (finished[i]) continue;
      const char *tuple = readBuffers[i]->_currentTuple;
      if (minTuple == NULL || func(tuple, minTuple) < 0) {
        minTuple = tuple;
        minTupleIndex = i;
      }
    }
    assert (minTuple);
    assert (minTupleIndex >= 0);
/*
    writeBuffer.write(minTuple);
    if (writeBuffer._totalTuplesWritten % 1000000 == 0) {
      VLOG (1) << "writing " << writeBuffer._totalTuplesWritten << "...";
    }
*/
    bool hasNext = readBuffers[minTupleIndex]->next ();
    if (!hasNext) {
      finished[minTupleIndex] = true;
      ++finishedFractures;
    }
  }


  watch.stop();
  LOG (INFO) << "mergeCStoreFractures() end. " << watch.getElapsed() << " micsosec";

  return ""; //TODO
}

} // fdb
