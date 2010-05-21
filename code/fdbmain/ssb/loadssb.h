#ifndef SSB_LOADSSB_H
#define SSB_LOADSSB_H

#include <string>
namespace fdb {

// loads tbl files
// void loadSSBPipedFile(const std::string &dataFolder, const std::string &dataSignatureFile, const std::string &tblFolder);

// converts tbl files to bin files for faster loading
void convertSSBPipedFile(const std::string &tblFolder);

// loads converted bin files
void loadSSBBinFile(const std::string &dataFolder, const std::string &dataSignatureFile, const std::string &tblFolder, bool cstore, size_t lineorderSize);

void loadSSBBinFileMV(const std::string &dataFolder, const std::string &dataSignatureFile, const std::string &tblFolder, bool cstore, size_t lineorderSize);

} // fdb
#endif // SSB_LOADSSB_H
