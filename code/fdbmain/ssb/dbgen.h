#ifndef SSB_DBGEN_H
#define SSB_DBGEN_H

#include "../configvalues.h"
#include "ssb.h"
#include <string>
#include <stdint.h>
#include <vector>
#include <map>

namespace fdb {

// yet another version of TPC/SSB dbgen.
// this class provides a fast and on-memory data generation for LINEORDER.
// this class should be used to 'add' more fact table tuples, assuming
// reasonably large initial dimension/fact tables.

// compact versions of SSB objects
struct SlimCustomer;
struct SlimSupplier;
struct SlimPart;
// date table is small. so just use the original class

class DBGen {
public:
  DBGen (const std::string &dimensionDataFolder, size_t batchSize);
  ~DBGen();

  void setLastOrderKey (int32_t lastOrderkey) {
    _lastOrderkey = lastOrderkey;
  }
  int32_t getLastOrderkey () const { return _lastOrderkey; }
  void setLastRand (int lastRand) {
    _lastRand = lastRand;
  }
  int getLastRand () const { return _lastRand; }

  size_t getCurrentBatchSize () const { return _currentBatchSize; };
  void generateNextBatch ();
  Lineorder* getLineorderBuffer () { return _lineorderBuffer;}
  MVProjection* getMVBuffer () { return _mvBuffer; }

private:
  std::string _dimensionDataFolder;
  size_t _batchSize;
  size_t _currentBatchSize;
  Lineorder *_lineorderBuffer;
  MVProjection *_mvBuffer;
  int32_t _lastOrderkey;
  int _lastUpdnum; //number of rows in current segment (32 consecutive orderkey), if I understand original dbgen correctly..
  int _lastRand; // kind of seed

  std::vector<SlimCustomer> _customers;
  std::vector<SlimSupplier> _suppliers;
  std::vector<SlimPart> _parts;
  std::vector<Date> _dates;

  std::vector<std::string> _cityStrings;
  std::map<std::string, size_t> _cityStringMap;
  std::vector<std::string> _nationStrings;
  std::map<std::string, size_t> _nationStringMap;
  std::vector<std::string> _regionStrings;
  std::map<std::string, size_t> _regionStringMap;
  std::vector<std::string> _mfgrStrings;
  std::map<std::string, size_t> _mfgrStringMap;
  std::vector<std::string> _categoryStrings;
  std::map<std::string, size_t> _categoryStringMap;
  std::vector<std::string> _brandStrings;
  std::map<std::string, size_t> _brandStringMap;

  size_t getStringId (const std::string &str, std::vector<std::string> &v, std::map<std::string, size_t> &m);
  void loadCustomer(char *buffer);
  void loadSupplier(char *buffer);
  void loadPart(char *buffer);
  void loadDate(char *buffer);

  std::vector<std::string> _orderpriorities;
  std::vector<std::string> _shipmodes;

  inline unsigned int getNextRandUInt () {
    _lastRand = ms_rand(_lastRand);
    return (unsigned int) _lastRand;
  }
  inline int getNextRandInt () {
    _lastRand = ms_rand(_lastRand);
    return _lastRand;
  }
};

// add more properties if needed. but keep them integer.
struct SlimCustomer {
  int32_t custkey;
  int16_t cityId;
  int8_t nationId;
  int8_t regionId;
};

struct SlimSupplier {
  int32_t suppkey;
  int16_t cityId;
  int8_t nationId;
  int8_t regionId;
};

struct SlimPart {
public:
  int32_t partkey;
  int8_t mfgrId;
  int8_t categoryId;
  int16_t brandId;
};

} //fdb

#endif // SSB_DBGEN_H

