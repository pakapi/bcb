#ifndef STORAGE_FBUFFERPOOLIMPL_H
#define STORAGE_FBUFFERPOOLIMPL_H

#include "../configvalues.h"
#include "ffilesig.h"
#include "../io/fis.h"

#include <map>
#include <boost/shared_array.hpp>
#include <log4cxx/logger.h>

namespace fdb {

typedef long long FilePageId;

inline FilePageId toFilePageId (int fileId, int pageId) {
  return ((FilePageId) fileId << 32) + (FilePageId) pageId;
}

// an entry in the buffer pool
struct PoolEntry {
  PoolEntry () : data(NULL), read (false) {};
  char *data; // NULL if this entry is empty
  int fileId;
  int pageId;
  bool read; // set to true when this page is read. set to false when the clock hand reaches this page.

  void clear ();
};

// represents the status of one file being read by the buffer pool
struct FBufferedFileStatus {
  FFileSignature signature;
  DirectFileInputStream *stream;
};

// pimpl object for FBufferPool
class FBufferPoolImpl {
public:
  FBufferPoolImpl (int maxPageCount);
  ~FBufferPoolImpl();

  void clear();

  PoolEntry* findEntry (int fileId, int pageId) const; // this "internal" method doesn't overwrite read flag
  void addPage (int fileId, int pageId, char *data);

  const char* readPage (const FFileSignature &signature, int pageId);
  std::vector<const char*> readPages (const FFileSignature &signature, int beginningPageId, int pageCount);

  DirectFileInputStream* getOrOpenFile (const FFileSignature &signature);

  int _maxPageCount;
  boost::shared_array<PoolEntry> _entriesAutoPtr; // auto ptr for convenience AND raw ptr for efficiency
  PoolEntry *_entries;
  int _clockHand;

  std::map<FilePageId, int> _idMap; // map<file-page-id, index in _entries>

  typedef std::map<int, FBufferedFileStatus> FileMap;
  typedef FileMap::iterator FileMapIter;
  FileMap _fileMap; // map<file-id, FBufferedFileStatus>

  log4cxx::LoggerPtr _logger;
};


} // fdb

#endif // STORAGE_FBUFFERPOOLIMPL_H
