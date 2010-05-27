#include "fengine.h"
#include "ffamilyimpl.h"
#include "../io/fis.h"
#include "../storage/fbtree.h"
#include "../storage/fbtreeimpl.h" // reuse the btree dumping implementation.
#include "../storage/fbufferpool.h"
#include "../storage/fcstore.h"
#include "../storage/fcstoreimpl.h" // reuse the cstore dumping implementation.
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
        LOG (INFO) << "finished reading " << _signature.getFilepath();
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
  assert (grandTotalTupleCount > 0);
  for (size_t i = 0; i < fractureCount; ++i) {
    const FFileSignature &signature = signatures.getFileSignature (fractureNames[i]);
    int bufferedPages = (int) ((double) totalBufferedPages * signature.totalTupleCount / grandTotalTupleCount / 2);
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

  {
    FFileSignature signature;
    signature.fileId = signatures.issueNextFileId();
    signature.setFilepath(filepath);
    int outputBufferPages = totalBufferedPages / 2;
    if (outputBufferPages == 0) outputBufferPages = 1;
    ScopedMemoryForIO buffer(outputBufferPages * FDB_PAGE_SIZE, FDB_DIRECT_IO_ALIGNMENT, FDB_USE_DIRECT_IO);
    FBTreeWriter writer (signature, _type, reinterpret_cast<char*>(buffer.get()), outputBufferPages, grandTotalTupleCount, toKeySize(_type), tupleSize);
  
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
      writer.addTuple(minTuple);
      if (writer.currentTuple % 1000000 == 0) {
        VLOG (1) << "writing " << writer.currentTuple << "...";
      }
      bool hasNext = buffers[minTupleIndex]->next ();
      if (!hasNext) {
        finished[minTupleIndex] = true;
        ++finishedFractures;
      }
    }
    writer.finishWriting();
    signatures.addFileSignature(writer.signature);
    addOnDiskFracture(signature.getFilepath());
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
    _currentRemainingRunCount = *reinterpret_cast<const int*>(_currentCursor);
    assert (_currentRemainingRunCount > 0);
    _currentValue = _currentCursor + sizeof (int);
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
        ++(_impl->_currentTupleInPage);
      }
      retrieveCurrentDictionaryValue ();
    } else if (_column.compression == RLE_COMPRESSED) {
      --_currentRemainingRunCount;
      if (_currentRemainingRunCount <= 0) {
        _currentCursor += _column.maxLength + sizeof(int);
        ++(_impl->_currentTupleInPage);
        retrieveRLEValue ();
      }
    } else {
      assert (_column.compression == UNCOMPRESSED);
      _currentCursor += _column.maxLength;
      ++(_impl->_currentTupleInPage);
      retrieveUncompressedValue();
    }
  }
  // this method assumes there is at least one tuple to be read.
  void next () {
    // Note that in RLE, _currentTupleCountInPage is the number of runs, not tuples.
    if (_impl->_currentTupleInPage < _impl->_currentTupleCountInPage) {
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

// dumping class for CStore. unlike read buffer, this doesn't reuse FractureWriteBuffer. too different.
// instead, this class reuses the CStore storage class (FCStoreWriter)
struct FractureWriteBufferColumn {
public:
  FractureWriteBufferColumn (const FFileSignature &signature, const FCStoreColumn &column, DirectFileOutputStream *fd, int bufferSize, const std::vector<CStoreReadBuffer*> &readers, size_t columnIndex, int64_t totalTupleCount)
    : _signature(signature), _column(column), _columnIndex(columnIndex), _fd (fd), _bufferSize (bufferSize) {
    _buffer = reinterpret_cast<char*>(DirectFileStream::allocateMemoryForIO (bufferSize * FDB_PAGE_SIZE, FDB_DIRECT_IO_ALIGNMENT, FDB_USE_DIRECT_IO));
    ::memset (_buffer, 0, bufferSize * FDB_PAGE_SIZE);
    _writer = new FCStoreWriter(_signature.fileId, fd, _buffer, bufferSize, column, totalTupleCount);
    if (column.compression == DICTIONARY_COMPRESSED) {
      // create a new merged dictionary
      for (size_t i = 0; i < readers.size(); ++i) {
        FractureReadBufferColumn *reader = readers[i]->_readBuffers[columnIndex];
        assert (reader->_dictionary != NULL);
        _mergedSortedSet.insert (reader->_dictionary->begin(), reader->_dictionary->end());
      }
      assert (_mergedSortedSet.size() < (1 << 16));
      _writer->dictionarySize = _mergedSortedSet.size();
      _writer->dictionaryHashmap = new StringHashMap<uint16_t>(column.maxLength, 16);
      std::set<std::string>::const_iterator iter = _mergedSortedSet.begin();
      for (size_t i = 0; i < _mergedSortedSet.size(); ++i) {
        assert (iter != _mergedSortedSet.end());
        const char *data = iter->data();
        _writer->dictionaryHashmap->insert(data, i);
        _writer->dictionaryEntries.push_back (data);
        ++iter;
      }
      _writer->determineDictionaryBits();
    }
  }
  ~FractureWriteBufferColumn () {
    DirectFileStream::deallocateMemoryForIO(FDB_USE_DIRECT_IO, _buffer);
    delete _writer;
  }
  inline void write (const char* value) {
    _writer->addValue(value);
  }
  void flushClose (TableType type, FSignatureSet &signatureSet) {
    _writer->finishWriting();
    _writer->updateFileSignature(_signature, type, _columnIndex);
    _fd->sync();
    _fd->close();
    signatureSet.addFileSignature(_signature);
  }

  char *_buffer;
  FFileSignature _signature;
  FCStoreColumn _column;
  size_t _columnIndex;
  DirectFileOutputStream *_fd;
  int _bufferSize;

  // for Dictionary Encoding
  std::set<std::string> _mergedSortedSet; // to keep entity of strings
  FCStoreWriter *_writer;

private:
  FractureWriteBufferColumn (const FractureWriteBufferColumn &);
};

struct CStoreWriteBuffer {
public:
  CStoreWriteBuffer (FEngine *engine, TableType type, const std::vector<FCStoreColumn> &columns, const std::string &fracture, const std::vector<CStoreReadBuffer*> &readers, int64_t totalTupleCount, int bufferSize)
    : _engine(engine), _columns(columns), _fracture (fracture), _totalTuplesWritten(0), _totalTupleCount(totalTupleCount) {
    std::vector<FFileSignature> signatures = engine->getSignatureSet().createNewCStoreFileSignatures(engine->getDataFolder(), fracture, type);
    assert (columns.size() == signatures.size());

    // divide output buffer based on all fractures' all columns' page count
    int64_t grandTotalPageCount = 0;
    std::vector<int64_t> columnPageCounts (columns.size(), 0);
    assert (columnPageCounts.size() == columns.size());
    for (size_t i = 0; i < readers.size(); ++i) {
      for (size_t j = 0; j < columns.size(); ++j) {
        int count = readers[i]->_signatures[j].pageCount;
        grandTotalPageCount += count;
        columnPageCounts[j] += count;
      }
    }

    for (size_t i = 0; i < signatures.size(); ++i) {
      int columnBufferSize = (int) ((double) bufferSize * columnPageCounts[i] / grandTotalPageCount);
      if (columnBufferSize == 0) columnBufferSize = 1;
      std::string filepath (signatures[i].getFilepath());
      if (std::remove(filepath.c_str()) == 0) {
        VLOG(1) << "deleted existing file " << filepath << ".";
      }
      boost::shared_ptr<DirectFileOutputStream> fd(new DirectFileOutputStream(filepath, FDB_USE_DIRECT_IO));
      _fds.push_back (fd);
      _writeBuffers.push_back (new FractureWriteBufferColumn(signatures[i], columns[i], fd.get(), columnBufferSize, readers, i, totalTupleCount));
    }
  }
  ~CStoreWriteBuffer () {
    for (size_t i = 0; i < _columns.size(); ++i) {
      delete _writeBuffers[i];
    }
    _writeBuffers.clear();
  }
  void write (const char* tuple) {
    for (size_t i = 0; i < _columns.size(); ++i) {
      _writeBuffers[i]->write (tuple + _columns[i].offset);
    }
    ++_totalTuplesWritten;
  }
  void flushClose (TableType type) {
    for (size_t i = 0; i < _columns.size(); ++i) {
      _writeBuffers[i]->flushClose(type, _engine->getSignatureSet());
    }
  }
  FEngine *_engine;
  std::vector<FCStoreColumn> _columns;
  std::string _fracture;
  std::vector<FractureWriteBufferColumn*> _writeBuffers;
  std::vector<boost::shared_ptr<DirectFileOutputStream> > _fds;
  int64_t _totalTuplesWritten;
  int64_t _totalTupleCount;
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
  assert (grandTotalTupleCount > 0);
  for (size_t i = 0; i < fractureCount; ++i) {
    int bufferedPages = (int) ((double) totalBufferedPages * readBuffers[i]->_totalTupleCount / grandTotalTupleCount / 2);
    if (bufferedPages == 0) bufferedPages = 1;
    readBuffers[i]->assignBuffers (bufferedPages);
    if (readBuffers[i]->_totalTupleCount > 0) {
      finished.push_back (false);
    } else {
      finished.push_back (true);
      ++finishedFractures;
    }
  }

  std::stringstream str;
  str << _name << "." << _nextFractureId;
  std::string fracture = str.str();
  {
    CStoreWriteBuffer writeBuffer (engine, _type, columns, fracture, readBuffers, grandTotalTupleCount, totalBufferedPages / 2);
  
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
      writeBuffer.write(minTuple);
      if (writeBuffer._totalTuplesWritten % 1000000 == 0) {
        VLOG (1) << "writing " << writeBuffer._totalTuplesWritten << "...";
      }
  
      bool hasNext = readBuffers[minTupleIndex]->next ();
      if (!hasNext) {
        finished[minTupleIndex] = true;
        ++finishedFractures;
      }
    }
    writeBuffer.flushClose(_type);
    assert (writeBuffer._totalTuplesWritten == grandTotalTupleCount);
  }

  addOnDiskFracture(fracture);
  for (size_t i = 0; i < fractureCount; ++i) {
    std::vector<std::string>::iterator iter = std::find (_fractures.begin(), _fractures.end(), fractureNames[i]);
    _fractures.erase (iter);

    std::vector<FFileSignature> colsigs = engine->getSignatureSet().getCStoreFileSignatures(engine->getDataFolder(), columns, fractureNames[i]);
    for (size_t j = 0; j < colsigs.size(); ++j) {
      engine->getSignatureSet().removeFileSignature(colsigs[j].getFilepath());
      if (deleteOldFractures) {
        if (std::remove(colsigs[j].getFilepath().c_str()) == 0) {
          LOG(INFO) << "deleted existing file " << colsigs[j].getFilepath() << ".";
        }
      }
    }
  }

  watch.stop();
  LOG (INFO) << "mergeCStoreFractures() end. " << watch.getElapsed() << " micsosec";

  return fracture;
}

} // fdb
