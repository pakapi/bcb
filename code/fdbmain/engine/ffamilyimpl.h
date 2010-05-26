#ifndef ENGINE_FFAMILYIMPL_H
#define ENGINE_FFAMILYIMPL_H

#include "ffamily.h"

namespace fdb {
// pimpl class of FFamily.

class FFamilyImpl {
public:
  FFamilyImpl (const std::string &name, TableType type, bool cstore) : _name(name), _current(NULL), _type(type), _cstore(cstore), _nextFractureId(0) {}; // TODO maybe constructed from some file?

  const std::vector<std::string>& getOnDiskFractures () const;
  void addOnDiskFracture (const std::string &name);
  bool eraseOnDiskFracture(const std::string &name);

  FMainMemoryBTree* getCurrentFracture();
  void setCurrentFracture(FMainMemoryBTree *fracture);
  const std::string& getName() const;
  TableType getTableType () const;
  bool isCStore () const;

  std::string mergeFractures (FEngine *engine, const std::vector<std::string> &fractureNames, bool deleteOldFractures, long long mergeBufferSize);

  // Btree version of merge implementation.
  std::string mergeBTreeFractures (FEngine *engine, const std::vector<std::string> &fractureNames, bool deleteOldFractures, long long mergeBufferSize);
  // CStore version of merge implementation.
  std::string mergeCStoreFractures (FEngine *engine, const std::vector<std::string> &fractureNames, bool deleteOldFractures, long long mergeBufferSize);

  std::vector<std::string> _fractures;
  std::string _name;
  FMainMemoryBTree *_current;
  TableType _type;
  bool _cstore;
  int _nextFractureId;
};

} //fdb

#endif // ENGINE_FFAMILYIMPL_H
