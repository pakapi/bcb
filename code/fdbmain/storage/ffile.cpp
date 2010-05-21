#include "ffile.h"
#include "fbtree.h"
#include "fcstore.h"
#include "../util/stopwatch.h"

#include <stdint.h>

#include <cassert>
#include <iostream>
#include <sstream>
#include <fstream>
#include <vector>
#include <string.h>
#include <glog/logging.h>
#include <boost/scoped_array.hpp>
#include <boost/filesystem/operations.hpp>

using namespace std;
using namespace boost;

namespace fdb {

FSignatureSet::~FSignatureSet () {
  if (_dirty) {
    LOG(ERROR) << "the signature file wasn't updated properly. some change might have been lost";
  }
}

void FSignatureSet::load (const std::string &folder, const std::string &filename) {
  load (folder + filename);
}
void FSignatureSet::load (const std::string &filepath) {
  _idMap.clear();
  _pathMap.clear();
  _lastFileId = 0;
  _dirty = false;

  StopWatch watch;
  watch.init();

  if (!boost::filesystem::exists(boost::filesystem::path(filepath))) {
    LOG(INFO) << "the signature file " << (filepath)
      << " does not exist. a new empty signature file will be created";
    return;
  }

  ifstream in(filepath.c_str(), ios::in | ios::binary);
  if (!in.is_open()) {
    LOG(ERROR) << "what's the heck? boost::filesystem::exists said ok!";
    return;
  }

  int filesize = boost::filesystem::file_size(boost::filesystem::path(filepath));
  assert (filesize % sizeof(FFileSignature) == 0);
  scoped_array<char> bufferPtr(new char[filesize]);
  char *buffer = bufferPtr.get();
  in.read (buffer, filesize);
  if (in.fail()) {
    LOG(ERROR) << "error on reading a signature file.";
  }
  FFileSignature *bufferSig = (FFileSignature *) buffer;
  for (int i = 0; i  < (int) (filesize / sizeof(FFileSignature)); ++i) {
    addFileSignature(bufferSig[i]);
    if (bufferSig[i].fileId > _lastFileId) _lastFileId = bufferSig[i].fileId;
  }

  in.close();
  if (in.fail()) {
    LOG(ERROR) << "error on closing a signature file.";
  }
  watch.stop();
  _dirty = false;
  LOG(INFO) << "read " << _idMap.size() << " signatures. " << watch.getElapsed() << " micsosec";
}
void FSignatureSet::save (const std::string &folder, const std::string &filename) {
  save (folder + filename);
}
void FSignatureSet::save (const std::string &filepath) {
  _dirty = false;

  StopWatch watch;
  watch.init();

  ofstream out(filepath.c_str(), ios::out | ios::binary | ios::trunc);
  if (!out.is_open()) {
    LOG(ERROR) << "could not open signature file";
    return;
  }

  scoped_array<char> bufferPtr(new char[sizeof(FFileSignature) * _idMap.size()]);
  char *buffer = bufferPtr.get();
  ::memset (buffer, 0, sizeof(FFileSignature) * _idMap.size());
  std::map<int, FFileSignature>::const_iterator iter = _idMap.begin();
  for (int i = 0; iter != _idMap.end(); ++i, ++iter) {
    ((FFileSignature *) buffer)[i] = iter->second;
  }

  out.write (buffer, sizeof(FFileSignature) * _idMap.size());
  if (out.fail()) {
    LOG(ERROR) << "error on writing a signature file.";
  }
  out.flush();
  if (out.fail()) {
    LOG(ERROR) << "error on flushing a signature file.";
  }
  out.close();
  if (out.fail()) {
    LOG(ERROR) << "error on closing a signature file.";
  }
  watch.stop();
  LOG(INFO) << "wrote " << _idMap.size() << " signatures. " << watch.getElapsed() << " micsosec";
}

bool FSignatureSet::existsFile (int fileId) const {
  return _idMap.find(fileId) != _idMap.end();
}
bool FSignatureSet::existsFile (const std::string &filepath) const {
  return _pathMap.find(filepath) != _pathMap.end();
}
const FFileSignature& FSignatureSet::getFileSignature (int fileId) const {
  std::map<int, FFileSignature>::const_iterator iter = _idMap.find(fileId);
  assert (iter != _idMap.end());
  return iter->second;
}
const FFileSignature& FSignatureSet::getFileSignature (const std::string &filepath) const {
  std::map<std::string, FFileSignature>::const_iterator iter = _pathMap.find(filepath);
  assert (iter != _pathMap.end());
  return iter->second;
}
void FSignatureSet::removeFileSignature (int fileId) {
  const FFileSignature& signature = getFileSignature(fileId);
  _idMap.erase(signature.fileId);
  _pathMap.erase(signature.filepath);
  _dirty = true;
}
void FSignatureSet::removeFileSignature (const std::string &filepath) {
  const FFileSignature& signature = getFileSignature(filepath);
  _idMap.erase(signature.fileId);
  _pathMap.erase(signature.filepath);
  _dirty = true;
}
void FSignatureSet::addFileSignature (const FFileSignature &signature) {
  assert (signature.signatureVersion > 0);
  assert (signature.rootPageStart >= 0);
  assert (signature.rootPageCount >= 0);
  assert (signature.rootPageLevel >= 0);
  assert (signature.pageCount >= 0);
  if (!signature.columnFile) {
    assert (signature.leafEntrySize > 0);
    assert (signature.keyEntrySize > 0);
    assert (signature.keyCompareFuncType > 0);
  }
  assert (signature.tableType > 0);
  assert (signature.filepath != NULL);
  assert (signature.fileId > 0);
  if (signature.columnFile) {
    assert (signature.columnType > 0);
    assert (signature.columnMaxLength > 0);
    assert (signature.columnIndex >= 0);
    assert (signature.columnOffset >= 0);
    assert (signature.columnCompression > 0);
  }
  _dirty = true;
  _idMap.insert(std::pair<int, FFileSignature>(signature.fileId, signature));
  _pathMap.insert(std::pair<string, FFileSignature>(signature.filepath, signature));
  assert (_idMap.size() == _pathMap.size());
}

FFileSignature FSignatureSet::dumpToNewRowStoreFile (const std::string &folder, const std::string &filename, const FMainMemoryBTree &btree) {
  int fileId = issueNextFileId();
  bool addsSl = (folder.size() > 0 && folder[folder.size() - 1] != '/');
  string filepath = folder + (addsSl ? "/" : "") + filename;

  if (_pathMap.find(filepath) != _pathMap.end()) {
    LOG(ERROR) << "the file " << filepath << " already exists in the database.";
    throw std::exception();
  }
  if (filepath.size() + 1 > FFILE_MAX_FILEPATH) {
    LOG(ERROR) << "the filepath " << filepath << " is too long.";
    throw std::exception();
  }

  // register the new signature
  FFileSignature signature;
  signature.fileId = fileId;
  ::strcpy(signature.filepath, filepath.c_str());
  btree.dumpToNewRowStoreFile(signature);
  addFileSignature(signature);

  return signature;
}

std::vector<FFileSignature> FSignatureSet::getCStoreFileSignatures (const std::string &folder, const std::vector<FCStoreColumn>  &columns, const std::string &filenamePrefix) const {
  std::vector<FFileSignature> ret;
  bool addsSl = (folder.size() > 0 && folder[folder.size() - 1] != '/');
  for (size_t i = 0; i < columns.size(); ++i) {
    const FCStoreColumn &column = columns[i];
    string filepath = folder + (addsSl ? "/" : "") + filenamePrefix + "." + column.name + ".db";
    if (!existsFile(filepath)) {
      LOG(ERROR) << "the file " << filepath << " wasn't found in the database.";
      throw std::exception();
    }
    ret.push_back (getFileSignature(filepath));
  }
  return ret;
}
std::vector<FFileSignature> FSignatureSet::dumpToNewCStoreFiles (const std::string &folder, const std::string &filenamePrefix, const FMainMemoryBTree &btree) {
  std::vector<FCStoreColumn> columns = FCStoreUtil::getPhysicalDesignsOf(btree.getTableType());
  std::vector<FFileSignature> signatures;

  bool addsSl = (folder.size() > 0 && folder[folder.size() - 1] != '/');
  for (size_t i = 0; i < columns.size(); ++i) {
    const FCStoreColumn &column = columns[i];
    assert (column.name.size() > 0);
    string filepath = folder + (addsSl ? "/" : "") + filenamePrefix + "." + column.name + ".db";

    if (_pathMap.find(filepath) != _pathMap.end()) {
      LOG(ERROR) << "the file " << filepath << " already exists in the database.";
      throw std::exception();
    }
    if (filepath.size() + 1 > FFILE_MAX_FILEPATH) {
      LOG(ERROR) << "the filepath " << filepath << " is too long.";
      throw std::exception();
    }

    FFileSignature signature;
    signature.fileId = issueNextFileId();
    ::strcpy(signature.filepath, filepath.c_str());
    signatures.push_back (signature);
  }
  assert (signatures.size () == columns.size());
  FCStoreUtil::dumpToNewCStoreFile(signatures, btree);
  for (size_t i = 0; i < columns.size(); ++i) {
    addFileSignature(signatures[i]);
  }
  return signatures;
}

void FSignatureSet::debugout() const {
  for (std::map<int, FFileSignature>::const_iterator iter = _idMap.begin(); iter != _idMap.end(); ++iter) {
    const FFileSignature &signature = iter->second;
    cout << "fileId=" << signature.fileId << ","
      << "filepath=" << signature.filepath << ","
      << "pageCount=" << signature.pageCount << ","
      << "rootPageStart=" << signature.rootPageStart << ","
      << "rootPageCount=" << signature.rootPageCount << ","
      << "rootPageLevel=" << signature.rootPageLevel << ","
      << "keyEntrySize=" << signature.keyEntrySize << ","
      << "keyCompareFuncType=" << signature.keyCompareFuncType << ","
      << "leafEntrySize=" << signature.leafEntrySize << ","
      << "tableType=" << signature.tableType << ","
      << "columnFile=" << signature.columnFile << ","
      << "columnIndex=" << signature.columnIndex << ","
      << "columnType=" << toColumnTypeName(signature.columnType) << ","
      << "columnMaxLength=" << signature.columnMaxLength << ","
      << "columnOffset=" << signature.columnOffset << ","
      << "columnCompression=" << toCompressionSchemeName(signature.columnCompression) << ","
      << "signatureVersion=" << signature.signatureVersion << ","
      << endl;
  }
}


} // fdb
