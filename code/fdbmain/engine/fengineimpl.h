#ifndef ENGINE_FENGINEIMPL_H
#define ENGINE_FENGINEIMPL_H

#include "fengine.h"
#include "../storage/ffile.h"
#include <boost/shared_ptr.hpp>
#include <map>
#include <string>

namespace fdb {

class FBufferPool;

// pimpl object of FEngine
class FEngineImpl {
public:
  FEngineImpl (const std::string &dataFolder, const std::string &configFilePath, int bufferPageCount);
  ~FEngineImpl ();

  FMainMemoryBTree* getMainMemoryTable (const std::string &name);
  FMainMemoryBTree* createNewMainMemoryTable (const std::string &name, TableType type, int64_t maxSize, bool sortedBuffer);
  bool eraseMainMemoryTable (const std::string &name);

  FFamily* getFractureFamily (const std::string &name);
  FFamily* createNewFractureFamily (const std::string &name, TableType type, bool cstore);
  bool eraseFractureFamily (const std::string &name);

  FSignatureSet& getSignatureSet ();
  FBufferPool* getBufferPool ();
  const std::string& getDataFolder() const;

  std::string _dataFolder;
  std::string _configFilePath;
  FSignatureSet _signatures;
  boost::shared_ptr<FBufferPool> _bufferpool;
  std::map<std::string, boost::shared_ptr<FMainMemoryBTree> > _onMemoryTables;
  std::map<std::string, boost::shared_ptr<FFamily> > _families;
};

} //fdb

#endif // ENGINE_FENGINEIMPL_H
