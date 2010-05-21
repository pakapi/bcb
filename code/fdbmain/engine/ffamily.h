#ifndef ENGINE_FFAMILY_H
#define ENGINE_FFAMILY_H

#include <vector>
#include <string>

namespace fdb {
// the class that represents a family of fractures.
// a family consists of versions of fractures which might be on-disk file (up to any number)
// or a on-memory table (up to one, and always the latest).

class FFamilyImpl;
class FMainMemoryBTree;
class FFamily {
public:
  FFamily (); // TODO maybe constructed from some file?
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

  // TODO methods for saving to file?

  FFamilyImpl* getImpl () {return _impl; } // just for testcases
private:
  FFamilyImpl *_impl;
};

} //fdb

#endif // ENGINE_FFAMILY_H
