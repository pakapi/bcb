#include "fengine.h"
#include "fengineimpl.h"
#include "ffamily.h"
#include "../storage/fbtree.h"
#include "../storage/fbufferpool.h"

namespace fdb {

// ==========================================================================
//  Proxies
// ==========================================================================
FEngine::FEngine (const std::string &dataFolder, const std::string &configFilePath, int bufferPageCount)
  : _impl (new FEngineImpl (dataFolder, configFilePath, bufferPageCount)) {
}
FEngine::~FEngine () {
  delete _impl;
}

FMainMemoryBTree* FEngine::getMainMemoryTable (const std::string &name) {
  return _impl->getMainMemoryTable(name);
}
FMainMemoryBTree* FEngine::createNewMainMemoryTable (const std::string &name, TableType type, int64_t maxSize, bool sortedBuffer) {
  return _impl->createNewMainMemoryTable(name, type, maxSize, sortedBuffer);
}
bool FEngine::eraseMainMemoryTable (const std::string &name) {
  return _impl->eraseMainMemoryTable(name);
}

FFamily* FEngine::getFractureFamily (const std::string &name) {
  return _impl->getFractureFamily(name);
}
FFamily* FEngine::createNewFractureFamily (const std::string &name) {
  return _impl->createNewFractureFamily(name);
}
bool FEngine::eraseFractureFamily (const std::string &name) {
  return _impl->eraseFractureFamily(name);
}

FSignatureSet& FEngine::getSignatureSet () {
  return _impl->getSignatureSet ();
}
FBufferPool* FEngine::getBufferPool () {
  return _impl->getBufferPool ();
}
const std::string& FEngine::getDataFolder() const {
  return _impl->getDataFolder();
}

// ==========================================================================
//  Implementation
// ==========================================================================
FEngineImpl::FEngineImpl (const std::string &dataFolder, const std::string &configFilePath, int bufferPageCount) : _dataFolder(dataFolder), _configFilePath(configFilePath) {
  _signatures.load (configFilePath); // TODO : there should be separated file
  _bufferpool = boost::shared_ptr<FBufferPool>(new FBufferPool(bufferPageCount)); // TODO read the config from file
}
FEngineImpl::~FEngineImpl () {
  if (_signatures.isDirty()) {
    _signatures.save (_configFilePath);
  }
}

FSignatureSet& FEngineImpl::getSignatureSet () {
  return _signatures;
}
FBufferPool* FEngineImpl::getBufferPool () {
  return _bufferpool.get();
}
const std::string& FEngineImpl::getDataFolder() const {
  return _dataFolder;
}

// =====================
//  On memory table get/set
// =====================
FMainMemoryBTree* FEngineImpl::getMainMemoryTable (const std::string &name) {
  std::map<std::string, boost::shared_ptr<FMainMemoryBTree> >::const_iterator it = _onMemoryTables.find (name);
  if (it == _onMemoryTables.end()) {
    return NULL;
  } else {
    return it->second.get();
  }
}
FMainMemoryBTree* FEngineImpl::createNewMainMemoryTable (const std::string &name, TableType type, int64_t maxSize, bool sortedBuffer) {
  if (_onMemoryTables.find (name) != _onMemoryTables.end()) {
    assert (false);
    throw std::exception ();
  }
  boost::shared_ptr<FMainMemoryBTree> table (new FMainMemoryBTree(type, maxSize, sortedBuffer));
  _onMemoryTables [name] = table;
  return table.get();
}
bool FEngineImpl::eraseMainMemoryTable (const std::string &name) {
  size_t erased = _onMemoryTables.erase (name);
  return erased > 0;
}

// =====================
//  Fracture family get/set
// =====================

FFamily* FEngineImpl::getFractureFamily (const std::string &name) {
  std::map<std::string, boost::shared_ptr<FFamily> >::const_iterator it = _families.find (name);
  if (it == _families.end()) {
    return NULL;
  } else {
    return it->second.get();
  }
}
FFamily* FEngineImpl::createNewFractureFamily (const std::string &name) {
  if (_families.find (name) != _families.end()) {
    assert (false);
    throw std::exception ();
  }
  boost::shared_ptr<FFamily> family (new FFamily());
  _families [name] = family;
  return family.get();
}
bool FEngineImpl::eraseFractureFamily (const std::string &name) {
  size_t erased = _families.erase (name);
  return erased > 0;
}

} //fdb
