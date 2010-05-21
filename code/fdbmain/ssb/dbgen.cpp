#include "dbgen.h"
#include <string.h>
#include <fstream>
#include <boost/scoped_array.hpp>
#include <glog/logging.h>

#define IO_BUFFER_SIZE (2 << 20)

// <definitions from dss.h>
#define  CUST_MORTALITY 3  /* portion with have no orders */
#define  O_LCNT_MIN      1
#define  O_LCNT_MAX      7
#define  PENNIES    100 /* for scaled int money arithmetic */

#define  L_QTY_MIN    1
#define  L_QTY_MAX    50
#define  L_TAX_MIN    0
#define  L_TAX_MAX    8
#define  L_DCNT_MIN   0
#define  L_DCNT_MAX   10

#define  L_SDTE_MIN   1
#define  L_SDTE_MAX   121
#define  L_CDTE_MIN   30
#define  L_CDTE_MAX   90
#define  L_RDTE_MIN   1
#define  L_RDTE_MAX   30
// </definitions from dss.h>

using namespace std;
using namespace boost;

namespace fdb {

DBGen::DBGen (const string &dimensionDataFolder, size_t batchSize) : _dimensionDataFolder(dimensionDataFolder), _batchSize (batchSize), _currentBatchSize (0), _lastOrderkey(0), _lastUpdnum(0), _lastRand (12345678) {
  LOG(INFO) << "loading dimension data...";

  scoped_array<char> bufferAutoPtr(new char[IO_BUFFER_SIZE]);
  char *buffer = bufferAutoPtr.get();
  loadCustomer(buffer);
  loadSupplier(buffer);
  loadPart(buffer);
  loadDate(buffer);

  // these are no longer needed.
  _cityStringMap.clear();
  _nationStringMap.clear();
  _regionStringMap.clear();
  _mfgrStringMap.clear();
  _categoryStringMap.clear();
  _brandStringMap.clear();

  Lineorder l;
  const char *priorities[] = {"1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECI", "5-LOW"};
  for (size_t i = 0; i < 5; ++i) {
    char str [sizeof(l.orderpriority)];
    ::memset (str, 0, sizeof(str));
    ::memcpy (str, priorities[i], ::strlen(priorities[i]));
    _orderpriorities.push_back (string(str, sizeof(str)));
  }
  const char *modes[] = {"AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK"};
  for (size_t i = 0; i < 7; ++i) {
    char str [sizeof(l.shipmode)];
    ::memset (str, 0, sizeof(str));
    ::memcpy (str, modes[i], ::strlen(modes[i]));
    _shipmodes.push_back (string(str, sizeof(str)));
  }

  _lineorderBuffer = new Lineorder[_batchSize + 32]; // +32 as it might have more than _batchSize
  _mvBuffer = new MVProjection[_batchSize + 32];
  LOG(INFO) << "completed initialization.";
}
DBGen::~DBGen() {
  delete[] _lineorderBuffer;
  delete[] _mvBuffer;
}

void DBGen::loadCustomer(char *buffer) {
  std::string filename = _dimensionDataFolder + "customer.bin";
  std::ifstream file(filename.c_str(), std::ios::in | std::ios::binary);
  if (!file) {
    LOG(ERROR) << "could not open " << filename;
    throw std::exception();
  }
  const size_t objectSize = sizeof(Customer);
  size_t maxBufCount = IO_BUFFER_SIZE / objectSize;
  size_t maxBufSize = maxBufCount * objectSize;
  int count = 0;
  while (true) {
    file.read (buffer, maxBufSize);
    size_t readSize = file.gcount();
    for (size_t pos = 0; pos + objectSize <= readSize; pos += objectSize) {
      ++count;
      const Customer *obj = reinterpret_cast<const Customer*>(buffer + pos);
      SlimCustomer slimObj;
      slimObj.custkey = obj->custkey;
      slimObj.cityId = getStringId(string(obj->city, sizeof(obj->city)), _cityStrings, _cityStringMap);
      slimObj.nationId = getStringId(string(obj->nation, sizeof(obj->nation)), _nationStrings, _nationStringMap);
      slimObj.regionId = getStringId(string(obj->region, sizeof(obj->region)), _regionStrings, _regionStringMap);
      _customers.push_back (slimObj);
    }
    if (readSize < maxBufSize) break;
  }
  file.close();
  LOG(INFO) << "read " << count << " customers";
}
void DBGen::loadSupplier(char *buffer) {
  std::string filename = _dimensionDataFolder + "supplier.bin";
  std::ifstream file(filename.c_str(), std::ios::in | std::ios::binary);
  if (!file) {
    LOG(ERROR) << "could not open " << filename;
    throw std::exception();
  }
  const size_t objectSize = sizeof(Supplier);
  size_t maxBufCount = IO_BUFFER_SIZE / objectSize;
  size_t maxBufSize = maxBufCount * objectSize;
  int count = 0;
  while (true) {
    file.read (buffer, maxBufSize);
    size_t readSize = file.gcount();
    for (size_t pos = 0; pos + objectSize <= readSize; pos += objectSize) {
      ++count;
      const Supplier *obj = reinterpret_cast<const Supplier*>(buffer + pos);
      SlimSupplier slimObj;
      slimObj.suppkey = obj->suppkey;
      slimObj.cityId = getStringId(string(obj->city, sizeof(obj->city)), _cityStrings, _cityStringMap);
      slimObj.nationId = getStringId(string(obj->nation, sizeof(obj->nation)), _nationStrings, _nationStringMap);
      slimObj.regionId = getStringId(string(obj->region, sizeof(obj->region)), _regionStrings, _regionStringMap);
      _suppliers.push_back (slimObj);
    }
    if (readSize < maxBufSize) break;
  }
  file.close();
  LOG(INFO) << "read " << count << " suppliers";
}
void DBGen::loadPart(char *buffer) {
  std::string filename = _dimensionDataFolder + "part.bin";
  std::ifstream file(filename.c_str(), std::ios::in | std::ios::binary);
  if (!file) {
    LOG(ERROR) << "could not open " << filename;
    throw std::exception();
  }
  const size_t objectSize = sizeof(Part);
  size_t maxBufCount = IO_BUFFER_SIZE / objectSize;
  size_t maxBufSize = maxBufCount * objectSize;
  int count = 0;
  while (true) {
    file.read (buffer, maxBufSize);
    size_t readSize = file.gcount();
    for (size_t pos = 0; pos + objectSize <= readSize; pos += objectSize) {
      ++count;
      const Part *obj = reinterpret_cast<const Part*>(buffer + pos);
      SlimPart slimObj;
      slimObj.partkey = obj->partkey;
      slimObj.mfgrId = getStringId(string(obj->mfgr, sizeof(obj->mfgr)), _mfgrStrings, _mfgrStringMap);
      slimObj.categoryId = getStringId(string(obj->category, sizeof(obj->category)), _categoryStrings, _categoryStringMap);
      slimObj.brandId = getStringId(string(obj->brand, sizeof(obj->brand)), _brandStrings, _brandStringMap);
      _parts.push_back (slimObj);
    }
    if (readSize < maxBufSize) break;
  }
  file.close();
  LOG(INFO) << "read " << count << " parts";
}
void DBGen::loadDate(char *buffer) {
  std::string filename = _dimensionDataFolder + "date.bin";
  std::ifstream file(filename.c_str(), std::ios::in | std::ios::binary);
  if (!file) {
    LOG(ERROR) << "could not open " << filename;
    throw std::exception();
  }
  const size_t objectSize = sizeof(Date);
  size_t maxBufCount = IO_BUFFER_SIZE / objectSize;
  size_t maxBufSize = maxBufCount * objectSize;
  int count = 0;
  while (true) {
    file.read (buffer, maxBufSize);
    size_t readSize = file.gcount();
    for (size_t pos = 0; pos + objectSize <= readSize; pos += objectSize) {
      ++count;
      _dates.push_back (*reinterpret_cast<const Date*>(buffer + pos));
    }
    if (readSize < maxBufSize) break;
  }
  file.close();
  LOG(INFO) << "read " << count << " dates";
}

size_t DBGen::getStringId (const string &str, vector<string> &v, map<string, size_t> &m) {
  map<string, size_t>::const_iterator mapI = m.find (str);
  if (mapI == m.end()) {
    // new string entry
    size_t newId = v.size();
    v.push_back (str);
    m[str] = newId;
    return newId;
  }
  return mapI->second;
}


int32_t rpb_routine(int32_t p) {
  int32_t price;
  price = 90000;
  price += (p/10) % 20001;        /* limit contribution to $200 */
  price += (p % 1000) * 100;
  return(price);
}

// see mk_order in build.c
void DBGen::generateNextBatch () {
  _currentBatchSize = 0;
  while (_currentBatchSize < _batchSize) {
    int32_t nextOrderKey = _lastOrderkey + 1;
    if (_lastUpdnum >= 32 || nextOrderKey % 32 >= 8) {
      nextOrderKey = ((_lastOrderkey / 32) + 1) * 32;
      assert (nextOrderKey > _lastOrderkey);
      _lastUpdnum = 0;
    }
    size_t orderdateIndex = getNextRandUInt() % _dates.size();
    const Date &orderdate = _dates[orderdateIndex];

    size_t customerIndex = getNextRandUInt() % _customers.size();
    if (_customers[customerIndex].custkey % CUST_MORTALITY == 0) {
      if (customerIndex == 0) ++customerIndex;
      else --customerIndex;
    }
    const SlimCustomer &customer = _customers[customerIndex];

    const string &orderpriority = _orderpriorities[getNextRandUInt() % _orderpriorities.size()];

    size_t lineCount = O_LCNT_MIN + (getNextRandUInt() % (O_LCNT_MAX - O_LCNT_MIN));
    int32_t totalprice = 0;
    for (size_t linenum = 1; linenum <= lineCount; ++linenum) {
      Lineorder &l = _lineorderBuffer[_currentBatchSize];
      MVProjection &m = _mvBuffer[_currentBatchSize];

      l.orderkey = m.key.l_orderkey = nextOrderKey;
      l.linenumber = m.key.l_linenumber = linenum;
      l.custkey = customer.custkey;
      ::memcpy(m.key.c_region, _regionStrings[customer.regionId].data(), sizeof(m.key.c_region));
      ::memcpy(m.key.c_nation, _nationStrings[customer.nationId].data(), sizeof(m.key.c_nation));
      ::memcpy(m.key.c_city, _cityStrings[customer.cityId].data(), sizeof(m.key.c_city));

      const SlimPart &part = _parts[getNextRandUInt() % _parts.size()];
      l.partkey = part.partkey;
      ::memcpy(m.p_brand, _brandStrings[part.brandId].data(), sizeof(m.p_brand));
      ::memcpy(m.p_mfgr, _mfgrStrings[part.mfgrId].data(), sizeof(m.p_mfgr));
      ::memcpy(m.p_category, _categoryStrings[part.categoryId].data(), sizeof(m.p_category));

      const SlimSupplier &supplier = _suppliers[getNextRandUInt() % _suppliers.size()];
      l.suppkey = supplier.suppkey;
      ::memcpy(m.key.s_region, _regionStrings[supplier.regionId].data(), sizeof(m.key.s_region));
      ::memcpy(m.key.s_nation, _nationStrings[supplier.nationId].data(), sizeof(m.key.s_nation));
      ::memcpy(m.key.s_city, _cityStrings[supplier.cityId].data(), sizeof(m.key.s_city));

      l.quantity = m.l_quantity = L_QTY_MIN + (getNextRandUInt() % (L_QTY_MAX - L_QTY_MIN));
      l.discount = m.l_discount = L_DCNT_MIN + (getNextRandUInt() % (L_DCNT_MAX - L_DCNT_MIN));
      l.tax = L_TAX_MIN + (getNextRandUInt() % (L_TAX_MAX - L_TAX_MIN));

      l.orderdate = orderdate.datekey;
      m.key.d_year = orderdate.year;
      m.key.d_yearmonthnum = orderdate.yearmonthnum;
      ::memcpy (m.key.d_yearmonth, orderdate.yearmonth, sizeof(m.key.d_yearmonth));

      ::memcpy (l.orderpriority, orderpriority.data(), sizeof(l.orderpriority));
      l.shippriority[0] = '0'; // always '0'

      size_t commitdateIndex = orderdateIndex + L_CDTE_MIN + (getNextRandUInt() % (L_CDTE_MAX - L_CDTE_MIN));
      if (commitdateIndex >= _dates.size()) {
        commitdateIndex = _dates.size() - 1;
      }
      const Date &commitdate = _dates[commitdateIndex];
      l.commitdate = commitdate.datekey;

      const string &shipmode = _shipmodes[getNextRandUInt() % _shipmodes.size()];
      ::memcpy (l.shipmode, shipmode.data(), sizeof(l.shipmode));

      int32_t rprice = rpb_routine(part.partkey);
      l.extendedprice = m.l_extendedprice = rprice * l.quantity;
      l.revenue = m.l_revenue = l.extendedprice * (100 - l.discount) / 100;
      l.supplycost = m.l_supplycost = 0.6 * rprice;

      totalprice += ((l.extendedprice *  (100 - l.discount)) / 100 ) * (100 + l.tax) / 100;

      ++_currentBatchSize;
    }

    for (size_t linenum = 1; linenum <= lineCount; ++linenum) {
      Lineorder &l = _lineorderBuffer[_currentBatchSize - linenum];
      assert (l.orderkey == nextOrderKey);
      l.ordertotalprice = totalprice;
    }

    _lastOrderkey = nextOrderKey;
  }
}

} // fdb

