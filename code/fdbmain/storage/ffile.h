#ifndef STORAGE_FFILE_H
#define STORAGE_FFILE_H

#include <string>
#include <vector>
#include <map>
#include <stdint.h>
#include "../configvalues.h"
#include "ffilesig.h"
#include "fpage.h"

namespace fdb {

class FBufferPool;

// represents a configuration file containing the list of file signatures.
class FMainMemoryBTree;
class FCStoreColumn;
class FSignatureSet {
public:
  FSignatureSet() : _lastFileId(0), _dirty (false) {};
  ~FSignatureSet();
  void load (const std::string &folder, const std::string &filename);
  void load (const std::string &filepath);
  void save (const std::string &folder, const std::string &filename);
  void save (const std::string &filepath);

  size_t size () const { return _idMap.size(); }
  bool isDirty () const { return _dirty; }

  bool existsFile (int fileId) const;
  bool existsFile (const std::string &filepath) const;

  const FFileSignature& getFileSignature (int fileId) const;
  const FFileSignature& getFileSignature (const std::string &filepath) const;
  std::vector<FFileSignature> getCStoreFileSignatures (const std::string &folder, const std::vector<FCStoreColumn> &columns, const std::string &filenamePrefix) const;

  void removeFileSignature (int fileId);
  void removeFileSignature (const std::string &filepath);

  void addFileSignature (const FFileSignature &signature);
  int issueNextFileId() {return ++_lastFileId;}

  // dumps the given on-memory BTree to disk and returns the newly registered file signature.
  // after calling this method, remember to call save()!! Otherwise the new signature will be lost.
  FFileSignature dumpToNewRowStoreFile (const std::string &folder, const std::string &filename, const FMainMemoryBTree &btree);

  // basically same as dumpToNewRowStoreFile, but the table is stored as CStore files
  // whose file name prefix is given in the parameter.
  // this is for temp=rowstore, file=cstore pattern.
  std::vector<FFileSignature> dumpToNewCStoreFiles (const std::string &folder, const std::string &filenamePrefix, const FMainMemoryBTree &btree);

  // TODO another method for temp=file=cstore pattern

  // output the content of this file to stdout
  void debugout() const;

private:
  std::map<int, FFileSignature> _idMap; // map<fileid, sig>
  std::map<std::string, FFileSignature> _pathMap; // map<filepath, sig>
  int _lastFileId;
  bool _dirty;
};


// represents a data file containing either columnar or row table data.
class FDataFile {
public:
  FDataFile (const FFileSignature &signature, FBufferPool *bp);

private:
  FBufferPool *_bp;
  FFileSignature _signature;
};

} // fdb
#endif // STORAGE_FFILE_H
