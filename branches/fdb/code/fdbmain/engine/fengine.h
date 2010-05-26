#ifndef ENGINE_FENGINE_H
#define ENGINE_FENGINE_H

#include <string>
#include <stdint.h>
#include "../configvalues.h"

namespace fdb {
// the class that represents a database.
// this class holds every configuration, data/signature file and on-memory data
// for the database.

class FEngineImpl;
class FMainMemoryBTree;
class FSignatureSet;
class FBufferPool;
class FFamily;

class FEngine {
public:
  FEngine (const std::string &dataFolder, const std::string &configFilePath, int bufferPageCount); // TODO the first/second param should be read from config file
  ~FEngine ();

  // get/set of main memory table to hold cached data.
  // getter. returns NULL if not the name found
  FMainMemoryBTree* getMainMemoryTable (const std::string &name);
  // create a new main memory table of give name managed by this engine.
  // the table will be deleted when eraseMainMemoryTable is called, or the engine class is revoked.
  // throws exception if the given name is already registered.
  FMainMemoryBTree* createNewMainMemoryTable (const std::string &name, TableType type, int64_t maxSize, bool sortedBuffer);
  // erases a main memory table from this engine. returns true if erased, false if the name is not found.
  bool eraseMainMemoryTable (const std::string &name);
  // no addTable() so far. pointer ownership is ambiguous.

  // get/set of fracture families.
  FFamily* getFractureFamily (const std::string &name);
  FFamily* createNewFractureFamily (const std::string &name, TableType type, bool cstore);
  bool eraseFractureFamily (const std::string &name);

  FSignatureSet& getSignatureSet ();
  FBufferPool* getBufferPool ();
  const std::string& getDataFolder() const;

  FEngineImpl* getImpl () {return _impl; } // just for testcases
private:
  FEngineImpl *_impl;

  FEngine (const FEngine &); // prohibit copy
  FEngine (); // prohibit default construction
};

} //fdb

#endif // ENGINE_FENGINE_H
