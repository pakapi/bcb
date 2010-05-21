#ifndef SSB_MAKETINY_H
#define SSB_MAKETINY_H

#include <string>

namespace fdb {

// make a set of tiny (but consistent) version of SSB data files
// from specified tbl files.
// the tiny files are appropriate for testcases and debugging.
void makeTinySSB(const std::string &originalTblFolder, const std::string &tinyTblFolder, size_t tuples);

} // fdb
#endif // SSB_MAKETINY_H
