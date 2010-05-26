#ifndef ENGINE_FFAMILY_H
#define ENGINE_FFAMILY_H

#include "../configvalues.h"
#include <vector>
#include <string>

namespace fdb {
// the class that represents a family of fractures.
// a family consists of versions of fractures which might be on-disk file (up to any number)
// or a on-memory table (up to one, and always the latest).

class FEngine;
class FFamilyImpl;
class FMainMemoryBTree;
class FFamily {
public:
  FFamily (const std::string &name, TableType type, bool cstore); // TODO maybe constructed from some file?
  ~FFamily ();

  // returns the list of name (which chould be path of data file or its prefix if c-store)
  // for on-disk read-only fractures. the result is sorted by the version (first entry=oldest).
  const std::vector<std::string>& getOnDiskFractures () const;
  // registeres a new fracture as a latest on-disk fracture.
  // this method should be called when an on-memory fracture is saved to file.
  void addOnDiskFracture (const std::string &name);
  // un-registers a fracture. this method should be called when a fracture is merged and deleted 
  bool eraseOnDiskFracture(const std::string &name);

  // returns the pointer to current on-memory fracture.
  // returns NULL if current fracture is not registered yet.
  FMainMemoryBTree* getCurrentFracture();
  // sets the current on-memory fracture.
  // if you want to clear the current fracture, pass NULL.
  // this class does not gain the ownership thus never deletes the pointer.
  void setCurrentFracture(FMainMemoryBTree *fracture);

  const std::string& getName() const;
  TableType getTableType () const;
  bool isCStore () const;

  // TODO methods for saving to file?

  // merge the given files (specified by name) of the family to one new fracture.
  // returns the name of the new fracture.
  // @param deleteOldFractures if true, delete the old fractures from filesystem.
  // @param mergeBufferSize the total size of RAM in bytes to be consumed for reading/writing fractures.
  std::string mergeFractures (FEngine *engine, const std::vector<std::string> &fractureNames, bool deleteOldFractures, long long mergeBufferSize);


  FFamilyImpl* getImpl () {return _impl; } // just for testcases
private:
  FFamilyImpl *_impl;
};

} //fdb

#endif // ENGINE_FFAMILY_H
