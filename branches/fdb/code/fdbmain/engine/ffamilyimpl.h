#ifndef ENGINE_FFAMILYIMPL_H
#define ENGINE_FFAMILYIMPL_H

#include "ffamily.h"

namespace fdb {
// pimpl class of FFamily.

class FFamilyImpl {
public:
  FFamilyImpl () : _current(NULL) {}; // TODO maybe constructed from some file?

  const std::vector<std::string>& getOnDiskFractures () const;
  void addOnDiskFracture (const std::string &name);
  bool eraseOnDiskFracture(const std::string &name);

  FMainMemoryBTree* getCurrentFracture();
  void setCurrentFracture(FMainMemoryBTree *fracture);

  std::vector<std::string> _fractures;
  FMainMemoryBTree *_current;
};

} //fdb

#endif // ENGINE_FFAMILYIMPL_H
