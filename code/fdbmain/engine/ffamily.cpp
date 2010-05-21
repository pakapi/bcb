#include "ffamilyimpl.h"
#include <algorithm>
#include <cassert>

namespace fdb {

FFamily::FFamily () : _impl (new FFamilyImpl()) {
}
FFamily::~FFamily () {
  delete _impl;
}

const std::vector<std::string>& FFamily::getOnDiskFractures () const {
  return _impl->getOnDiskFractures();
}
void FFamily::addOnDiskFracture (const std::string &name) {
  _impl->addOnDiskFracture(name);
}
bool FFamily::eraseOnDiskFracture(const std::string &name) {
  return _impl->eraseOnDiskFracture(name);
}
FMainMemoryBTree* FFamily::getCurrentFracture() {
  return _impl->getCurrentFracture();
}
void FFamily::setCurrentFracture(FMainMemoryBTree *fracture) {
  _impl->setCurrentFracture(fracture);
}


const std::vector<std::string>& FFamilyImpl::getOnDiskFractures () const {
  return _fractures;
}
void FFamilyImpl::addOnDiskFracture (const std::string &name) {
  assert (std::find(_fractures.begin(), _fractures.end(), name) == _fractures.end());
  _fractures.push_back(name);
}
bool FFamilyImpl::eraseOnDiskFracture(const std::string &name) {
  std::vector<std::string>::iterator iter = std::find(_fractures.begin(), _fractures.end(), name);
  if (iter == _fractures.end()) {
    return false;
  } else {
    _fractures.erase (iter);
    return true;
  }
}

FMainMemoryBTree* FFamilyImpl::getCurrentFracture() {
  return _current;
}
void FFamilyImpl::setCurrentFracture(FMainMemoryBTree *fracture) {
  _current = fracture;
}

} // fdb