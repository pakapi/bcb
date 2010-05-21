#include "queryssb.h"
#include "queryssbimpl.h"
#include "ssb.h"
#include "../engine/fengine.h"
#include "../engine/ffamily.h"
#include "../storage/fbtree.h"
#include "../storage/fbufferpool.h"
#include "../storage/fcstore.h"
#include "../storage/ffile.h"
#include "../storage/ffilesig.h"
#include "../storage/searchcond.h"
#include "../util/stopwatch.h"
#include <cassert>
#include <vector>
#include <map>
#include <set>
#include <sstream>
#include <glog/logging.h>
#include <boost/scoped_array.hpp>

using namespace std;
using namespace boost;

namespace fdb {

// ==========================================================================
//  Constructor/Destructor, proxies, etc
// ==========================================================================
SSBQueryExecutor::SSBQueryExecutor(FEngine *engine) {
  _impl = new SSBQueryExecutorImpl (engine);
}

SSBQueryExecutor::~SSBQueryExecutor() {
  delete _impl;
}
template <typename INT_TYPE>
void toStringAsInt (stringstream &str, const std::string &s) {
    assert (s.size() == sizeof(INT_TYPE));
    str << *reinterpret_cast<const INT_TYPE *>(s.data());
}
string SSBQueryResult::toString () const {
  stringstream str;
  str << "elapsedMicrosec=" << elapsedMicrosec << ", singleIntResult=" << singleIntResult << endl;
  if (groupedResults.size() > 0) {
    str << "groupedResults:" << endl;
    for (ResultMapIter it = groupedResults.begin(); it != groupedResults.end(); ++it) {
      const ResultKey &key = it->first;
      int64_t sum = it->second;
      assert (key.size() == groupColumnTypes.size());
      str << "  [";
      for (size_t j = 0; j < key.size(); ++j) {
        const std::string &s = key[j];
        if (j > 0) str << ",";
        switch (groupColumnTypes[j]) {
        case RESULT_GROUP_INT8:
          toStringAsInt <int8_t> (str, s);
          break;
        case RESULT_GROUP_INT16:
          toStringAsInt <int16_t> (str, s);
          break;
        case RESULT_GROUP_INT32:
          toStringAsInt <int32_t> (str, s);
          break;
        case RESULT_GROUP_INT64:
          toStringAsInt <int64_t> (str, s);
          break;
        default:
          str << "'" << s << "'";
        }
      }
      str << "] = " << sum << endl;
    }
  }
  return str.str();
}
void SSBQueryResult::sumResult (const SSBQueryResult &other) {
  assert (groupColumnTypes == other.groupColumnTypes);

  // merge single result
  singleIntResult += other.singleIntResult;

  // merge grouped results
  for (ResultMapIter it = other.groupedResults.begin(); it != other.groupedResults.end(); ++it) {
    const ResultKey &key = it->first;
    int64_t sum = it->second;
    ResultMap::iterator jt = groupedResults.find (key);
    if (jt == groupedResults.end()) {
      // new group
      groupedResults[key] = sum;
    } else {
      // merge same group
      jt->second += sum;
    }
  }
}

FEngine* SSBQueryExecutor::getEngine() {
  return _impl->getEngine();
}
FEngine* SSBQueryExecutorImpl::getEngine() {
  return _engine;
}

SSBQueryExecutorImpl::SSBQueryExecutorImpl (FEngine *engine)
  : _engine(engine), _dataFolder(engine->getDataFolder()), _bufferpool (engine->getBufferPool()), _signatures(engine->getSignatureSet()) {
}

boost::shared_ptr<SSBQueryResult> SSBQueryExecutor::query (int query, bool cstore, const SSBQueryParam &param) {
  return _impl->query (query, cstore, param);
}

boost::shared_ptr<SSBQueryResult> SSBQueryExecutorImpl::query (int query, bool cstore, const SSBQueryParam &param) {
  switch (query) {
  case 11:
    if (cstore) {
      return query11C (param);
    } else {
      return query11B (param);
    }
    break;
  case 12:
    if (cstore) {
      return query12C (param);
    } else {
      return query12B (param);
    }
    break;
  case 13:
    if (cstore) {
      return query13C (param);
    } else {
      return query13B (param);
    }
    break;
  case 21:
    if (cstore) {
      return query21C (param);
    } else {
      return query21B (param);
    }
    break;
  case 22:
    if (cstore) {
      return query22C (param);
    } else {
      return query22B (param);
    }
    break;
  case 23:
    if (cstore) {
      return query23C (param);
    } else {
      return query23B (param);
    }
    break;
  default:
    LOG(ERROR) << "Not implemented yet: query=" << query;
    assert (false);
    return boost::shared_ptr<SSBQueryResult>();
  }
}


// ==========================================================================
//  Query Implementations
// ==========================================================================
// ===========
//  for BTree search with d_year
// ===========
#define REGION_SIZE 12
const char* REGIONS[] = {"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"};
void setRegionString (char *dest, const char *str) {
  ::memset (dest, 0, REGION_SIZE);
  ::memcpy (dest, str, ::strlen(str));
}
MVProjection createBtreeMVSearchYearKey (int year, size_t regionId) {
  MVProjection key;
  assert (REGION_SIZE == sizeof (key.key.s_region));
  ::memset (&key, 0, sizeof (MVProjection::PKType));
  key.key.d_year = year;
  setRegionString (key.key.s_region, REGIONS[regionId]);
  return key;
}
struct BtreeMVSearchYearContext {
  BtreeMVSearchYearContext (int year_, size_t regionId_, BtreeMVSearchCallback childCallback_, void *childContext_)
    : year (year_), childCallback (childCallback_), childContext (childContext_) {
    setRegionString(s_region, REGIONS[regionId_]);
  }

  int year;
  char s_region[REGION_SIZE];
  BtreeMVSearchCallback childCallback;
  void *childContext;
};
TupleCallbackRet btreeMVSearchYearCallback (void *context, const void *tuple) {
  BtreeMVSearchYearContext* parentContext = reinterpret_cast<BtreeMVSearchYearContext*>(context);
  const MVProjection *tup = reinterpret_cast<const MVProjection*>(tuple);
  if (tup->key.d_year > parentContext->year || ::memcmp(tup->key.s_region, parentContext->s_region, REGION_SIZE) != 0) {
    return TUPLE_CALLBACK_QUIT;
  }
  if (tup->key.d_year == parentContext->year) {
    parentContext->childCallback (parentContext->childContext, tup);
  } 
  return TUPLE_CALLBACK_OK;
}

void SSBQueryExecutorImpl::btreeMVSearchYearMainMemory (const std::string &familyName, BtreeMVSearchCallback callback, void* childContext, int year) {
  FFamily *family = _engine->getFractureFamily(familyName);
  if (family == NULL) return;
  FMainMemoryBTree *fracture = family->getCurrentFracture();
  if (fracture == NULL) return;
  if (fracture->isSortedBuffer()) {
    VLOG(1) << "btreeMVSearchYearMainMemory: scanning sorted on-memory BTree..";
    //then, we do exactly same thing to onmemory btree
    for (int i = 0; i < 5; ++i) {
      BtreeMVSearchYearContext context (year, i, callback, childContext);
      MVProjection key = createBtreeMVSearchYearKey (year, i);
      fracture->scanTuplesGreaterEqual(btreeMVSearchYearCallback, &context, reinterpret_cast<const char*>(&key));
    }
  } else {
    // in this case, we have to fully scan the onmemory btree
    VLOG(1) << "btreeMVSearchYearMainMemory: scanning unsorted on-memory BTree..";
    const MVProjection *buffer = reinterpret_cast<const MVProjection*>(fracture->getUnsortedBuffer());
    int64_t tuples = fracture->size();
    for (int64_t i = 0; i < tuples; ++i) {
      const MVProjection *p = buffer + i;
      if (p->key.d_year == year) {
        callback (childContext, p);
      }
    }
  }
  VLOG(1) << "btreeMVSearchYearMainMemory: scanning on-memory BTree done.";
}
void SSBQueryExecutorImpl::btreeMVSearchYear (BtreeMVSearchCallback callback, void* childContext, int year) {
  FReadOnlyDiskBTree mv (_bufferpool, _signatures.getFileSignature(_dataFolder + BTREE_MV_MAIN_FILENAME));

  // to skip the first s_region sort order, search for every 5 value of s_region.
  for (int i = 0; i < 5; ++i) {
    BtreeMVSearchYearContext context (year, i, callback, childContext);
    MVProjection key = createBtreeMVSearchYearKey (year, i);
    mv.scanTuplesGreaterEqual(btreeMVSearchYearCallback, &context, reinterpret_cast<const char*>(&key));
  }
  // if there is current fracture, read from it too with the same callback function
  btreeMVSearchYearMainMemory (BTREE_MV_FAMILY, callback, childContext, year);
}

// ===========
//  for BTree search with s_region
// ===========
struct BtreeMVSearchSRegionContext {
  BtreeMVSearchSRegionContext (const std::string &region, BtreeMVSearchCallback childCallback_, void *childContext_)
    : childCallback(childCallback_), childContext(childContext_) {
    setRegionString(s_region, region.c_str());
  }
  char s_region[REGION_SIZE];
  BtreeMVSearchCallback childCallback;
  void *childContext;
};
MVProjection createBtreeMVSearchSRegionKey (const std::string &region) {
  MVProjection key;
  ::memset (&key, 0, sizeof (MVProjection::PKType));
  setRegionString(key.key.s_region, region.c_str());
  return key;
}
TupleCallbackRet btreeMVSearchSRegionCallback (void *context, const void *tuple) {
  BtreeMVSearchSRegionContext* parentContext = reinterpret_cast<BtreeMVSearchSRegionContext*>(context);
  const MVProjection *tup = reinterpret_cast<const MVProjection*>(tuple);
  if (::memcmp(tup->key.s_region, parentContext->s_region, REGION_SIZE) != 0) {
    return TUPLE_CALLBACK_QUIT;
  }
  parentContext->childCallback (parentContext->childContext, tup);
  return TUPLE_CALLBACK_OK;
}
void SSBQueryExecutorImpl::btreeMVSearchSRegionMainMemory (const std::string &familyName, BtreeMVSearchCallback callback, void* childContext, const std::string &region) {
  FFamily *family = _engine->getFractureFamily(familyName);
  if (family == NULL) return;
  FMainMemoryBTree *fracture = family->getCurrentFracture();
  if (fracture == NULL) return;
  if (fracture->isSortedBuffer()) {
    VLOG(1) << "btreeMVSearchSRegionMainMemory: scanning sorted on-memory BTree..";
    MVProjection key = createBtreeMVSearchSRegionKey (region);
    BtreeMVSearchSRegionContext context (region, callback, childContext);
    fracture->scanTuplesGreaterEqual(btreeMVSearchSRegionCallback, &context, reinterpret_cast<const char*>(&key));
  } else {
    // in this case, we have to fully scan the onmemory btree
    VLOG(1) << "btreeMVSearchSRegionMainMemory: scanning unsorted on-memory BTree..";
    const MVProjection *buffer = reinterpret_cast<const MVProjection*>(fracture->getUnsortedBuffer());
    int64_t tuples = fracture->size();
    char s_region[REGION_SIZE];
    setRegionString(s_region, region.c_str());
    for (int64_t i = 0; i < tuples; ++i) {
      const MVProjection *p = buffer + i;
      if (::memcmp(p->key.s_region, s_region, REGION_SIZE) == 0) {
        callback (childContext, p);
      }
    }
  }
  VLOG(1) << "btreeMVSearchSRegionMainMemory: scanning on-memory BTree done.";
}
void SSBQueryExecutorImpl::btreeMVSearchSRegion (BtreeMVSearchCallback callback, void* childContext, const std::string &region) {
  FReadOnlyDiskBTree mv (_bufferpool, _signatures.getFileSignature(_dataFolder + BTREE_MV_MAIN_FILENAME));
  MVProjection key = createBtreeMVSearchSRegionKey (region);
  BtreeMVSearchSRegionContext context (region, callback, childContext);
  mv.scanTuplesGreaterEqual(btreeMVSearchSRegionCallback, &context, reinterpret_cast<const char*>(&key));
  // if there is current fracture, read from it too with the same callback function
  btreeMVSearchSRegionMainMemory (BTREE_MV_FAMILY, callback, childContext, region);
}

// ===========
//  Q1.1
// ===========
// select sum(lo_extendedprice*lo_discount) as revenue from lineorder,
// dwdate where lo_orderdate = d_datekey and d_year = 1993 and lo_discount
// between 1 and 3 and lo_quantity < 25
// $$: dwdate where lo_orderdate = d_datekey and d_year = $1 and lo_discount
// $$: between $2 and $3 and lo_quantity < $4

void SSBQueryParam::generateRandomParamQ11 (int &seed) {
  int year = generateUInt(seed, 1992, 1999);
  int discFrom = generateUInt(seed, 0, 8);
  int discTo = discFrom + 3;
  int quanTo = generateUInt(seed, 0, 50);
  ints.push_back (year);
  ints.push_back (discFrom);
  ints.push_back (discTo);
  ints.push_back (quanTo);
}

struct Q11BContext {
  Q11BContext (int discFrom_, int discTo_, int quanTo_)
    : sum (0), discFrom(discFrom_), discTo(discTo_), quanTo(quanTo_) {}
  int64_t sum;
  int discFrom;
  int discTo;
  int quanTo;
};
void query11BCallback (void *context, const MVProjection *tuple) {
  Q11BContext* con = reinterpret_cast<Q11BContext*>(context);
  if (tuple->l_discount >= con->discFrom && tuple->l_discount <= con->discTo && tuple->l_quantity < con->quanTo) {
    con->sum += tuple->l_extendedprice * tuple->l_discount;
  }
}
shared_ptr<SSBQueryResult> SSBQueryExecutorImpl::query11B (const SSBQueryParam &param) {
  assert (param.ints.size() >= 4);
  StopWatch watch;
  watch.init();
  int16_t year = param.ints[0];
  Q11BContext context (param.ints[1], param.ints[2], param.ints[3]);
  btreeMVSearchYear (query11BCallback, &context, year);
  watch.stop();
  shared_ptr<SSBQueryResult> result (new SSBQueryResult(watch.getElapsed(), context.sum));
  VLOG(1) << "Q11B done: sum=" << context.sum << ". " << watch.getElapsed() << " microsec";
  return result;
}
shared_ptr<SSBQueryResult> SSBQueryExecutorImpl::query11C (const SSBQueryParam &param) {
  assert (param.ints.size() >= 4);
  StopWatch watch;
  watch.init();
  FReadOnlyCStore mv (_bufferpool, MV_PROJECTION, _signatures, _dataFolder, CSTORE_MV_MAIN_PREFIX);

  FColumnReader *yearReader = mv.getColumnReader("d_year");
  assert (yearReader->getColumn().compression == RLE_COMPRESSED);
  assert (yearReader->getColumn().type == COLUMN_INT16);

  FColumnReader *discReader = mv.getColumnReader("l_discount");
  assert (discReader->getColumn().compression == UNCOMPRESSED);
  FColumnReader *quanReader = mv.getColumnReader("l_quantity");
  assert (quanReader->getColumn().compression == UNCOMPRESSED);
  FColumnReader *extReader = mv.getColumnReader("l_extendedprice");
  assert (extReader->getColumn().compression == UNCOMPRESSED);

  int16_t year = param.ints[0];
  vector <PositionRange> ranges;
  yearReader->getPositionRanges(SearchCond(EQUAL, &year), ranges);
  assert (ranges.size() > 0);
  int64_t sum = 0;
  size_t maxLen = 0;
  for (size_t i = 0; i < ranges.size(); ++i) {
    const PositionRange &range = ranges[i];
    size_t length = range.end - range.begin;
    if (length > maxLen) maxLen = length;
  }

  // minor TODO : split the reading to suppress memory consumption.
  assert (discReader->getColumn().maxLength == sizeof(int8_t));
  scoped_array<int8_t> discBufferPtr(new int8_t[maxLen]);
  int8_t *discBuffer = (discBufferPtr.get());

  assert (extReader->getColumn().maxLength == sizeof(int32_t));
  scoped_array<int32_t> extBufferPtr(new int32_t[maxLen]);
  int32_t *extBuffer = (extBufferPtr.get());

  assert (quanReader->getColumn().maxLength == sizeof(int8_t));
  scoped_array<int8_t> quanBufferPtr(new int8_t[maxLen]);
  int8_t *quanBuffer = (quanBufferPtr.get());

  int discFrom = param.ints[1];
  int discTo = param.ints[2];
  int quanTo = param.ints[3];
  for (size_t i = 0; i < ranges.size(); ++i) {
    const PositionRange &range = ranges[i];
    size_t length = range.end - range.begin;
    discReader->getDecompressedData(range, discBuffer, maxLen * sizeof(int8_t));
    extReader->getDecompressedData(range, extBuffer, maxLen * sizeof(int32_t));
    quanReader->getDecompressedData(range, quanBuffer, maxLen * sizeof(int8_t));
    for (size_t j = 0; j < length; ++j) {
      if (discBuffer[j] >= discFrom && discBuffer[j] <= discTo && quanBuffer[j] < quanTo) {
        sum += extBuffer[j] * discBuffer[j];
      }
    }
  }

  // in case there is on-memory current fracture, search on it too.
  Q11BContext context (param.ints[1], param.ints[2], param.ints[3]);
  btreeMVSearchYearMainMemory(CSTORE_MV_FAMILY, query11BCallback, &context, year);
  sum += context.sum;

  watch.stop();
  shared_ptr<SSBQueryResult> result (new SSBQueryResult(watch.getElapsed(), sum));
  VLOG(1) << "Q11C done: sum=" << sum << ". " << watch.getElapsed() << " microsec";
  return result;
}

// ===========
//  Q1.2
// ===========
// select sum(lo_extendedprice*lo_discount) as revenue from lineorder,
// dwdate where lo_orderdate = d_datekey and d_yearmonthnum = 199401 and
// lo_discount between 4 and 6 and lo_quantity between 26 and 35;

// $$: dwdate where lo_orderdate = d_datekey and d_yearmonthnum = $1 and
// $$: lo_discount between $2 and $3 and lo_quantity between $4 and $5;
void SSBQueryParam::generateRandomParamQ12 (int &seed) {
  int year = generateUInt(seed, 1992, 1999);
  int month = generateUInt(seed, 1, 13);
  int yearMonthNum = year * 100 + month;
  int discFrom = generateUInt(seed, 0, 8);
  int discTo = discFrom + 3;
  int quanFrom = generateUInt(seed, 0, 50);
  int quanTo = quanFrom + 10;
  ints.push_back (yearMonthNum);
  ints.push_back (discFrom);
  ints.push_back (discTo);
  ints.push_back (quanFrom);
  ints.push_back (quanTo);
}

struct Q12BContext {
  Q12BContext (int yearMonthNum_, int discFrom_, int discTo_, int quanFrom_, int quanTo_)
    : sum (0), yearMonthNum(yearMonthNum_), discFrom(discFrom_), discTo(discTo_), quanFrom(quanFrom_), quanTo(quanTo_) {}
  int64_t sum;
  int yearMonthNum;
  int discFrom;
  int discTo;
  int quanFrom;
  int quanTo;
};
void query12BCallback (void *context, const MVProjection *tuple) {
  Q12BContext* con = reinterpret_cast<Q12BContext*>(context);
  if (tuple->key.d_yearmonthnum == con->yearMonthNum) {
    if (tuple->l_discount >= con->discFrom && tuple->l_discount <= con->discTo && tuple->l_quantity >= con->quanFrom && tuple->l_quantity <= con->quanTo) {
      con->sum += tuple->l_extendedprice * tuple->l_discount;
    }
  }
}
shared_ptr<SSBQueryResult> SSBQueryExecutorImpl::query12B (const SSBQueryParam &param) {
  assert (param.ints.size() >= 5);
  StopWatch watch;
  watch.init();
  Q12BContext context (param.ints[0], param.ints[1], param.ints[2], param.ints[3], param.ints[4]);
  btreeMVSearchYear (query12BCallback, &context, context.yearMonthNum / 100);
  watch.stop();
  shared_ptr<SSBQueryResult> result (new SSBQueryResult(watch.getElapsed(), context.sum));
  VLOG(1) << "Q12B done: sum=" << context.sum << ". " << watch.getElapsed() << " microsec";
  return result;
}
shared_ptr<SSBQueryResult> SSBQueryExecutorImpl::query12C (const SSBQueryParam &param) {
  assert (param.ints.size() >= 5);
  StopWatch watch;
  watch.init();
  FReadOnlyCStore mv (_bufferpool, MV_PROJECTION, _signatures, _dataFolder, CSTORE_MV_MAIN_PREFIX);

  FColumnReader *yearmonthnumReader = mv.getColumnReader("d_yearmonthnum");
  assert (yearmonthnumReader->getColumn().compression == RLE_COMPRESSED);
  assert (yearmonthnumReader->getColumn().type == COLUMN_INT32);

  FColumnReader *discReader = mv.getColumnReader("l_discount");
  assert (discReader->getColumn().compression == UNCOMPRESSED);
  FColumnReader *quanReader = mv.getColumnReader("l_quantity");
  assert (quanReader->getColumn().compression == UNCOMPRESSED);
  FColumnReader *extReader = mv.getColumnReader("l_extendedprice");
  assert (extReader->getColumn().compression == UNCOMPRESSED);

  int32_t yearMonthNum = param.ints[0];
  vector <PositionRange> ranges;
  yearmonthnumReader->getPositionRanges(SearchCond(EQUAL, &yearMonthNum), ranges);
  assert (ranges.size() > 0);
  int64_t sum = 0;
  size_t maxLen = 0;
  for (size_t i = 0; i < ranges.size(); ++i) {
    const PositionRange &range = ranges[i];
    size_t length = range.end - range.begin;
    if (length > maxLen) maxLen = length;
  }

  scoped_array<int8_t> discBufferPtr(new int8_t[maxLen]);
  int8_t *discBuffer = (discBufferPtr.get());

  scoped_array<int32_t> extBufferPtr(new int32_t[maxLen]);
  int32_t *extBuffer = (extBufferPtr.get());

  scoped_array<int8_t> quanBufferPtr(new int8_t[maxLen]);
  int8_t *quanBuffer = (quanBufferPtr.get());

  int discFrom = param.ints[1];
  int discTo = param.ints[2];
  int quanFrom = param.ints[3];
  int quanTo = param.ints[4];
  for (size_t i = 0; i < ranges.size(); ++i) {
    const PositionRange &range = ranges[i];
    size_t length = range.end - range.begin;
    discReader->getDecompressedData(range, discBuffer, maxLen * sizeof(int8_t));
    extReader->getDecompressedData(range, extBuffer, maxLen * sizeof(int32_t));
    quanReader->getDecompressedData(range, quanBuffer, maxLen * sizeof(int8_t));
    for (size_t j = 0; j < length; ++j) {
      if (discBuffer[j] >= discFrom && discBuffer[j] <= discTo && quanBuffer[j] >= quanFrom && quanBuffer[j] <= quanTo) {
        sum += extBuffer[j] * discBuffer[j];
      }
    }
  }

  // in case there is on-memory current fracture, search on it too.
  Q12BContext context (param.ints[0], param.ints[1], param.ints[2], param.ints[3], param.ints[4]);
  btreeMVSearchYearMainMemory (CSTORE_MV_FAMILY, query12BCallback, &context, context.yearMonthNum / 100);
  sum += context.sum;

  watch.stop();
  shared_ptr<SSBQueryResult> result (new SSBQueryResult(watch.getElapsed(), sum));
  VLOG(1) << "Q12C done: sum=" << sum << ". " << watch.getElapsed() << " microsec";
  return result;
}



// ===========
//  Q1.3
// ===========
// select sum(lo_extendedprice*lo_discount) as revenue from lineorder,
// dwdate where lo_orderdate = d_datekey and d_weeknuminyear = 6 and d_year
// = 1994 and lo_discount between 5 and 7 and lo_quantity between 26 and 35;
// $$: dwdate where lo_orderdate = d_datekey and d_weeknuminyear = $1 and d_year
// $$: = $2 and lo_discount between $3 and $4 and lo_quantity between $5 and $6;
void SSBQueryParam::generateRandomParamQ13 (int &seed) {
  int year = generateUInt(seed, 1992, 1999);
  int weeknuminyear = generateUInt(seed, 1, 53);
  int discFrom = generateUInt(seed, 0, 8);
  int discTo = discFrom + 3;
  int quanFrom = generateUInt(seed, 0, 50);
  int quanTo = quanFrom + 10;
  ints.push_back (year);
  ints.push_back (weeknuminyear);
  ints.push_back (discFrom);
  ints.push_back (discTo);
  ints.push_back (quanFrom);
  ints.push_back (quanTo);
}

struct Q13BContext {
  Q13BContext (int weeknuminyear_, int discFrom_, int discTo_, int quanFrom_, int quanTo_)
    : sum (0), weeknuminyear(weeknuminyear_), discFrom(discFrom_), discTo(discTo_), quanFrom(quanFrom_), quanTo(quanTo_) {}
  int64_t sum;
  int weeknuminyear;
  int discFrom;
  int discTo;
  int quanFrom;
  int quanTo;
};
void query13BCallback (void *context, const MVProjection *tuple) {
  Q13BContext* con = reinterpret_cast<Q13BContext*>(context);
  if (tuple->d_weeknuminyear == con->weeknuminyear && tuple->l_discount >= con->discFrom && tuple->l_discount <= con->discTo && tuple->l_quantity < con->quanTo) {
    con->sum += tuple->l_extendedprice * tuple->l_discount;
  }
}
shared_ptr<SSBQueryResult> SSBQueryExecutorImpl::query13B (const SSBQueryParam &param) {
  assert (param.ints.size() >= 6);
  StopWatch watch;
  watch.init();
  Q13BContext context(param.ints[1], param.ints[2], param.ints[3], param.ints[4], param.ints[5]);
  btreeMVSearchYear (query13BCallback, &context, param.ints[0]);
  watch.stop();
  shared_ptr<SSBQueryResult> result (new SSBQueryResult(watch.getElapsed(), context.sum));
  VLOG(1) << "Q13B done: sum=" << context.sum << ". " << watch.getElapsed() << " microsec";
  return result;
}
shared_ptr<SSBQueryResult> SSBQueryExecutorImpl::query13C (const SSBQueryParam &param) {
  assert (param.ints.size() >= 6);
  StopWatch watch;
  watch.init();
  FReadOnlyCStore mv (_bufferpool, MV_PROJECTION, _signatures, _dataFolder, CSTORE_MV_MAIN_PREFIX);

  FColumnReader *yearReader = mv.getColumnReader("d_year");
  FColumnReader *discReader = mv.getColumnReader("l_discount");
  FColumnReader *quanReader = mv.getColumnReader("l_quantity");
  FColumnReader *extReader = mv.getColumnReader("l_extendedprice");
  FColumnReader *weekReader = mv.getColumnReader("d_weeknuminyear");
  assert (weekReader->getColumn().compression == UNCOMPRESSED);

  int16_t year = param.ints[0];
  vector <PositionRange> ranges;
  yearReader->getPositionRanges(SearchCond(EQUAL, &year), ranges);
  assert (ranges.size() > 0);
  int64_t sum = 0;
  size_t maxLen = 0;
  for (size_t i = 0; i < ranges.size(); ++i) {
    const PositionRange &range = ranges[i];
    size_t length = range.end - range.begin;
    if (length > maxLen) maxLen = length;
  }

  // minor TODO : split the reading to suppress memory consumption.
  scoped_array<int8_t> discBufferPtr(new int8_t[maxLen]);
  int8_t *discBuffer = (discBufferPtr.get());

  scoped_array<int32_t> extBufferPtr(new int32_t[maxLen]);
  int32_t *extBuffer = (extBufferPtr.get());

  scoped_array<int8_t> quanBufferPtr(new int8_t[maxLen]);
  int8_t *quanBuffer = (quanBufferPtr.get());

  assert (weekReader->getColumn().maxLength == sizeof(int8_t));
  scoped_array<int8_t> weekBufferPtr(new int8_t[maxLen]);
  int8_t *weekBuffer = (weekBufferPtr.get());

  int weeknuminyear = param.ints[1];
  int discFrom = param.ints[2];
  int discTo = param.ints[3];
  int quanFrom = param.ints[4];
  int quanTo = param.ints[5];
  for (size_t i = 0; i < ranges.size(); ++i) {
    const PositionRange &range = ranges[i];
    size_t length = range.end - range.begin;
    discReader->getDecompressedData(range, discBuffer, maxLen * sizeof(int8_t));
    extReader->getDecompressedData(range, extBuffer, maxLen * sizeof(int32_t));
    quanReader->getDecompressedData(range, quanBuffer, maxLen * sizeof(int8_t));
    weekReader->getDecompressedData(range, weekBuffer, maxLen * sizeof(int8_t));
    for (size_t j = 0; j < length; ++j) {
      if (weekBuffer[j] == weeknuminyear && discBuffer[j] >= discFrom && discBuffer[j] <= discTo && quanBuffer[j] >= quanFrom && quanBuffer[j] <= quanTo) {
        sum += extBuffer[j] * discBuffer[j];
      }
    }
  }

  // in case there is on-memory current fracture, search on it too.
  Q13BContext context(param.ints[1], param.ints[2], param.ints[3], param.ints[4], param.ints[5]);
  btreeMVSearchYearMainMemory (CSTORE_MV_FAMILY, query13BCallback, &context, param.ints[0]);
  sum += context.sum;

  watch.stop();
  shared_ptr<SSBQueryResult> result (new SSBQueryResult(watch.getElapsed(), sum));
  VLOG(1) << "Q13C done: sum=" << sum << ". " << watch.getElapsed() << " microsec";
  return result;
}

// ===========
//  Q2.1
// ===========
// select sum(lo_revenue), d_year, p_brand1 from lineorder, dwdate,
// part, supplier where lo_orderdate = d_datekey and lo_partkey = p_partkey
// and lo_suppkey = s_suppkey and p_category = 'MFGR#12' and s_region =
// 'AMERICA' group by d_year, p_brand1 order by d_year, p_brand1;
// $$: and lo_suppkey = s_suppkey and p_category = '$1' and s_region =
// $$: '$2' group by d_year, p_brand1 order by d_year, p_brand1;
#define  P_CATEGORY_SIZE 7
#define  P_BRAND_SIZE 9

void SSBQueryParam::generateRandomParamQ21 (int &seed) {
  strings.push_back (generateRandomCategory(seed));
  strings.push_back (generateRandomRegion(seed));
}

struct Q21BContext {
  Q21BContext (const std::string &category) {
    ::memset (p_category, 0, P_CATEGORY_SIZE);
    ::memcpy (p_category, category.data(), category.size());
  }
  char p_category[P_CATEGORY_SIZE];
  map<int16_t, map<string, int64_t> > resultMap; //map<year, map<brand, sum>>

  shared_ptr<SSBQueryResult> toResult () {
    shared_ptr<SSBQueryResult> result (new SSBQueryResult(RESULT_GROUP_INT16, RESULT_GROUP_STRING));
    SSBQueryResult *resultRaw = result.get(); 
    for (map<int16_t, map<string, int64_t> >::const_iterator yearIt = resultMap.begin(); yearIt != resultMap.end(); ++yearIt) {
      int16_t year = yearIt->first;
      string yearStr (reinterpret_cast<char *>(&year), sizeof(int16_t));
      const map<string, int64_t> &brandMap = yearIt->second;
      for (map<string, int64_t>::const_iterator brandIt = brandMap.begin(); brandIt != brandMap.end(); ++brandIt) {
        vector<string> groupString;
        groupString.push_back(yearStr);
        groupString.push_back(brandIt->first);
        resultRaw->groupedResults[groupString] = brandIt->second;
      }
    }
    return result;
  }
};
void query21BCallback (void *context, const MVProjection *tuple) {
  Q21BContext* con = reinterpret_cast<Q21BContext*>(context);
  assert (P_CATEGORY_SIZE == sizeof (tuple->p_category));
  assert (P_BRAND_SIZE == sizeof (tuple->p_brand));
  if (::memcmp (con->p_category, tuple->p_category, P_CATEGORY_SIZE) == 0) {
    // con->sum += tuple->l_revenue;
    map<int16_t, map<string, int64_t> >::iterator yearIt = con->resultMap.find(tuple->key.d_year);
    if (yearIt == con->resultMap.end()) {
      con->resultMap[tuple->key.d_year] = map<string, int64_t>();
      yearIt = con->resultMap.find(tuple->key.d_year);
    }
    assert (yearIt != con->resultMap.end());
    yearIt->second;

    map<string, int64_t> &brandMap = yearIt->second;
    map<string, int64_t>::iterator brandIt = brandMap.find(string(tuple->p_brand, P_BRAND_SIZE));
    if (brandIt == brandMap.end()) {
      brandMap[string(tuple->p_brand, P_BRAND_SIZE)] = tuple->l_revenue;
    } else {
      (brandIt->second) += tuple->l_revenue;
    }
  }
}
shared_ptr<SSBQueryResult> SSBQueryExecutorImpl::query21B (const SSBQueryParam &param) {
  assert (param.strings.size() >= 2);
  StopWatch watch;
  watch.init();
  Q21BContext context (param.strings[0]);
  btreeMVSearchSRegion(query21BCallback, &context, param.strings[1]);
  shared_ptr<SSBQueryResult> result = context.toResult();
  watch.stop();
  result->elapsedMicrosec = watch.getElapsed();
  VLOG(1) << "Q21B done: " << result->groupedResults.size() << " rows. " << result->elapsedMicrosec << " microsec";
  return result;
}
shared_ptr<SSBQueryResult> SSBQueryExecutorImpl::query21C (const SSBQueryParam &param) {
  assert (param.strings.size() >= 2);
  StopWatch watch;
  watch.init();
  FReadOnlyCStore mv (_bufferpool, MV_PROJECTION, _signatures, _dataFolder, CSTORE_MV_MAIN_PREFIX);

  FColumnReader *sregionReader = mv.getColumnReader("s_region");
  assert (sregionReader->getColumn().compression == RLE_COMPRESSED);
  FColumnReaderRLE *yearReader = dynamic_cast<FColumnReaderRLE*>(mv.getColumnReader("d_year"));
  assert (yearReader != NULL);
  FColumnReader *revReader = mv.getColumnReader("l_revenue");
  assert (revReader->getColumn().compression == UNCOMPRESSED);
  FColumnReaderDictionary *brandReader = dynamic_cast<FColumnReaderDictionary*>(mv.getColumnReader("p_brand"));
  assert (brandReader->getColumn().compression == DICTIONARY_COMPRESSED_16BIT);
  FColumnReaderDictionary *categoryReader = dynamic_cast<FColumnReaderDictionary*>(mv.getColumnReader("p_category"));
  assert (categoryReader->getColumn().compression == DICTIONARY_COMPRESSED_8BIT);

  string p_category = categoryReader->normalize(param.strings[0]);
  string s_region = sregionReader->normalize(param.strings[1]);

  vector <PositionRange> regionRanges;
  sregionReader->getPositionRanges(SearchCond(EQUAL, s_region.data()), regionRanges);
  assert (regionRanges.size() == 1);
  const PositionRange &regionRange = regionRanges[0];

  // get ranges of each year, to make grouping/ordering efficient
  vector <pair<PositionRange, int16_t> > yearRanges;
  yearReader->getRLECompressedData(regionRange, yearRanges);
  assert (yearRanges.size() > 0);
  size_t maxLen = 0;
  vector <PositionRange> yearRangesVec;
  for (size_t i = 0; i < yearRanges.size(); ++i) {
    const PositionRange &range = yearRanges[i].first;
    yearRangesVec.push_back (range);
    size_t length = range.end - range.begin;
    if (length > maxLen) maxLen = length;
  }

  scoped_array<int32_t> revBufferPtr(new int32_t[maxLen]);
  int32_t *revBuffer = (revBufferPtr.get());
  scoped_array<int16_t> brandBufferPtr(new int16_t[maxLen]);
  int16_t *brandBuffer = (brandBufferPtr.get());
  vector<string> brands;
  brandReader->getAllDictionaryEntries(brands);
  size_t brandDictionarySize = brands.size();

  shared_ptr<SSBQueryResult> result (new SSBQueryResult(RESULT_GROUP_INT16, RESULT_GROUP_STRING));
  SSBQueryResult *resultRaw = result.get();

  vector<shared_ptr<PositionBitmap> > positions;
  categoryReader->setSearchRanges(yearRangesVec);
  categoryReader->getPositionBitmaps(SearchCond(EQUAL, p_category.data()), positions);
  assert (positions.size () == yearRanges.size());

  int rows = 0;
  for (size_t i = 0; i < yearRanges.size(); ++i) {
    const PositionRange &range = yearRanges[i].first;
    size_t length = range.end - range.begin;
    int16_t year = yearRanges[i].second;
    string yearStr (reinterpret_cast<char *>(&year), sizeof(int16_t));

    int64_t results[brandDictionarySize];
    ::memset (results, 0, sizeof(int64_t) * brandDictionarySize);
    revReader->getDecompressedData(range, revBuffer, maxLen * sizeof(int32_t));

    shared_ptr<PositionBitmap> position = positions[i];
    const unsigned char *bitmap = position->bitmap;
    int bitOffset = 0;
    brandReader->getDictionaryCompressedData(range, brandBuffer, maxLen * sizeof(int16_t), bitOffset);
    assert (bitOffset == 0);

    for (size_t j = 0; j < length; ++j) {
      unsigned char byte = bitmap[j / 8];
      unsigned char bitmask = (unsigned char) 1 << (j % 8);
      if ((byte & bitmask) == bitmask) {
        int16_t brandId = brandBuffer[j];
        assert (brandId >= 0);
        assert ((size_t) brandId < brandDictionarySize);
        results[brandId] += revBuffer[j];
      }
    }
    for (size_t brandId = 0; brandId < brandDictionarySize; ++brandId) {
      assert (results[brandId] >= 0);
      if (results[brandId] == 0) continue;
      ++rows;

      vector<string> groupString;
      groupString.push_back(yearStr);
      groupString.push_back(brands[brandId]);
      resultRaw->groupedResults[groupString] = results[brandId];
    }
  }

  // in case there is on-memory current fracture, search on it too.
  Q21BContext context (param.strings[0]);
  btreeMVSearchSRegionMainMemory(CSTORE_MV_FAMILY, query21BCallback, &context, param.strings[1]);
  resultRaw->sumResult(*(context.toResult()));

  watch.stop();
  resultRaw->elapsedMicrosec = watch.getElapsed();
  VLOG(1) << "Q21C done: " << rows << " rows. " << watch.getElapsed() << " microsec";
  return result;
}

// ===========
//  Q2.2
// ===========
// select sum(lo_revenue), d_year, p_brand1 from lineorder, dwdate,
// part, supplier where lo_orderdate = d_datekey and lo_partkey = p_partkey
// and lo_suppkey = s_suppkey and p_brand1 between 'MFGR#2221' and
// 'MFGR#2228' and s_region = 'ASIA' group by d_year, p_brand1 order by
// d_year, p_brand1;
// $$: and lo_suppkey = s_suppkey and p_brand1 between '$1' and
// $$: '$2' and s_region = '$3' group by d_year, p_brand1 order by

void SSBQueryParam::generateRandomParamQ22 (int &seed) {
  int brandId = generateRandomBrandId(seed);
  stringstream str;
  str << "MFGR#" << brandId;
  strings.push_back (str.str());

  stringstream str2;
  str2 << "MFGR#" << (brandId + 7);
  strings.push_back (str2.str());

  strings.push_back (generateRandomRegion(seed));
}

struct Q22BContext {
  Q22BContext (const std::string &brand_from, const std::string &brand_to) {
    ::memset (p_brand_from, 0, P_BRAND_SIZE);
    ::memcpy (p_brand_from, brand_from.data(), brand_from.size());
    ::memset (p_brand_to, 0, P_BRAND_SIZE);
    ::memcpy (p_brand_to, brand_to.data(), brand_to.size());
  }
  char p_brand_from[P_BRAND_SIZE];
  char p_brand_to[P_BRAND_SIZE];
  map<int16_t, map<string, int64_t> > resultMap; //map<year, map<brand, sum>>

  shared_ptr<SSBQueryResult> toResult () {
    shared_ptr<SSBQueryResult> result (new SSBQueryResult(RESULT_GROUP_INT16, RESULT_GROUP_STRING));
    SSBQueryResult *resultRaw = result.get(); 
    for (map<int16_t, map<string, int64_t> >::const_iterator yearIt = resultMap.begin(); yearIt != resultMap.end(); ++yearIt) {
      int16_t year = yearIt->first;
      string yearStr (reinterpret_cast<char *>(&year), sizeof(int16_t));
      const map<string, int64_t> &brandMap = yearIt->second;
      for (map<string, int64_t>::const_iterator brandIt = brandMap.begin(); brandIt != brandMap.end(); ++brandIt) {
        vector<string> groupString;
        groupString.push_back(yearStr);
        groupString.push_back(brandIt->first);
        resultRaw->groupedResults[groupString] = brandIt->second;
      }
    }
    return result;
  }
};
void query22BCallback (void *context, const MVProjection *tuple) {
  Q22BContext* con = reinterpret_cast<Q22BContext*>(context);
  assert (P_BRAND_SIZE == sizeof (tuple->p_brand));
  if (::memcmp (tuple->p_brand, con->p_brand_from, P_BRAND_SIZE) >= 0
    && ::memcmp (tuple->p_brand, con->p_brand_to, P_BRAND_SIZE) <= 0) {
    map<int16_t, map<string, int64_t> >::iterator yearIt = con->resultMap.find(tuple->key.d_year);
    if (yearIt == con->resultMap.end()) {
      con->resultMap[tuple->key.d_year] = map<string, int64_t>();
      yearIt = con->resultMap.find(tuple->key.d_year);
    }
    assert (yearIt != con->resultMap.end());
    yearIt->second;

    map<string, int64_t> &brandMap = yearIt->second;
    map<string, int64_t>::iterator brandIt = brandMap.find(string(tuple->p_brand, P_BRAND_SIZE));
    if (brandIt == brandMap.end()) {
      brandMap[string(tuple->p_brand, P_BRAND_SIZE)] = tuple->l_revenue;
    } else {
      (brandIt->second) += tuple->l_revenue;
    }
  }
}
shared_ptr<SSBQueryResult> SSBQueryExecutorImpl::query22B (const SSBQueryParam &param) {
  assert (param.strings.size() >= 3);
  StopWatch watch;
  watch.init();
  Q22BContext context (param.strings[0], param.strings[1]);
  btreeMVSearchSRegion(query22BCallback, &context, param.strings[2]);
  shared_ptr<SSBQueryResult> result = context.toResult();
  watch.stop();
  result->elapsedMicrosec = watch.getElapsed();
  VLOG(1) << "Q22B done: " << result->groupedResults.size() << " rows. " << result->elapsedMicrosec << " microsec";
  return result;
}
shared_ptr<SSBQueryResult> SSBQueryExecutorImpl::query22C (const SSBQueryParam &param) {
  assert (param.strings.size() >= 3);
  StopWatch watch;
  watch.init();
  FReadOnlyCStore mv (_bufferpool, MV_PROJECTION, _signatures, _dataFolder, CSTORE_MV_MAIN_PREFIX);

  FColumnReader *sregionReader = mv.getColumnReader("s_region");
  FColumnReaderRLE *yearReader = dynamic_cast<FColumnReaderRLE*>(mv.getColumnReader("d_year"));
  FColumnReader *revReader = mv.getColumnReader("l_revenue");
  FColumnReaderDictionary *brandReader = dynamic_cast<FColumnReaderDictionary*>(mv.getColumnReader("p_brand"));

  string p_brand_from = brandReader->normalize(param.strings[0]);
  string p_brand_to = brandReader->normalize(param.strings[1]);
  string s_region = sregionReader->normalize(param.strings[2]);

  vector <PositionRange> regionRanges;
  sregionReader->getPositionRanges(SearchCond(EQUAL, s_region.data()), regionRanges);
  assert (regionRanges.size() == 1);
  const PositionRange &regionRange = regionRanges[0];

  vector <pair<PositionRange, int16_t> > yearRanges;
  yearReader->getRLECompressedData(regionRange, yearRanges);
  assert (yearRanges.size() > 0);
  size_t maxLen = 0;
  for (size_t i = 0; i < yearRanges.size(); ++i) {
    const PositionRange &range = yearRanges[i].first;
    size_t length = range.end - range.begin;
    if (length > maxLen) maxLen = length;
  }

  scoped_array<int32_t> revBufferPtr(new int32_t[maxLen]);
  int32_t *revBuffer = (revBufferPtr.get());
  scoped_array<int16_t> brandBufferPtr(new int16_t[maxLen]);
  int16_t *brandBuffer = (brandBufferPtr.get());
  vector<string> brands;
  brandReader->getAllDictionaryEntries(brands);
  size_t brandDictionarySize = brands.size();
  vector<int> matchingBrandIds = brandReader->searchDictionary(SearchCond(p_brand_from.data(), p_brand_to.data()));

  shared_ptr<SSBQueryResult> result (new SSBQueryResult(RESULT_GROUP_INT16, RESULT_GROUP_STRING));
  SSBQueryResult *resultRaw = result.get();

  int rows = 0;
  for (size_t i = 0; i < yearRanges.size(); ++i) {
    const PositionRange &range = yearRanges[i].first;
    size_t length = range.end - range.begin;
    int16_t year = yearRanges[i].second;
    string yearStr (reinterpret_cast<char *>(&year), sizeof(int16_t));

    int64_t results[brandDictionarySize];
    ::memset (results, 0, sizeof(int64_t) * brandDictionarySize);
    revReader->getDecompressedData(range, revBuffer, maxLen * sizeof(int32_t));

    int bitOffset = 0;
    brandReader->getDictionaryCompressedData(range, brandBuffer, maxLen * sizeof(int16_t), bitOffset);
    assert (bitOffset == 0);

    for (size_t j = 0; j < length; ++j) {
      int16_t brandId = brandBuffer[j];
      assert (brandId >= 0);
      assert ((size_t) brandId < brandDictionarySize);
      bool matched = std::find(matchingBrandIds.begin(), matchingBrandIds.end(), brandId) != matchingBrandIds.end();
      if (matched) {
        results[brandId] += revBuffer[j];
      }
    }
    for (size_t brandId = 0; brandId < brandDictionarySize; ++brandId) {
      assert (results[brandId] >= 0);
      if (results[brandId] == 0) continue;
      ++rows;

      vector<string> groupString;
      groupString.push_back(yearStr);
      groupString.push_back(brands[brandId]);
      resultRaw->groupedResults[groupString] = results[brandId];
    }
  }

  // in case there is on-memory current fracture, search on it too.
  Q22BContext context (param.strings[0], param.strings[1]);
  btreeMVSearchSRegionMainMemory(CSTORE_MV_FAMILY, query22BCallback, &context, param.strings[2]);
  resultRaw->sumResult(*(context.toResult()));

  watch.stop();
  resultRaw->elapsedMicrosec = watch.getElapsed();
  VLOG(1) << "Q22C done: " << rows << " rows. " << watch.getElapsed() << " microsec";
  return result;
}

// ===========
//  Q2.3
// ===========
// select sum(lo_revenue), d_year, p_brand1 from lineorder, dwdate,
// part, supplier where lo_orderdate = d_datekey and lo_partkey = p_partkey
// and lo_suppkey = s_suppkey and p_brand1= 'MFGR#2239' and s_region =
// 'EUROPE' group by d_year, p_brand1 order by d_year, p_brand1;
// $$: and lo_suppkey = s_suppkey and p_brand1= '$1' and s_region =
// $$: '$2' group by d_year, p_brand1 order by d_year, p_brand1;
// NOTE: GROUP BY/ORDER BY p_brand makes no sense because of equality constraint. ignored it.
void SSBQueryParam::generateRandomParamQ23 (int &seed) {
  strings.push_back (generateRandomBrand(seed));
  strings.push_back (generateRandomRegion(seed));
}

struct Q23BContext {
  Q23BContext (const std::string &brand) {
    ::memset (p_brand, 0, P_BRAND_SIZE);
    ::memcpy (p_brand, brand.data(), brand.size());
  }
  char p_brand[P_BRAND_SIZE];
  map<int16_t, int64_t> resultMap; //map<year, sum>

  shared_ptr<SSBQueryResult> toResult () {
    shared_ptr<SSBQueryResult> result (new SSBQueryResult(RESULT_GROUP_INT16));
    SSBQueryResult *resultRaw = result.get();
    for (map<int16_t, int64_t>::const_iterator yearIt = resultMap.begin(); yearIt != resultMap.end(); ++yearIt) {
      int16_t year = yearIt->first;
      string yearStr (reinterpret_cast<char *>(&year), sizeof(int16_t));
      vector<string> groupString;
      groupString.push_back(yearStr);
      resultRaw->groupedResults[groupString] = yearIt->second;
    }
    return result;
  }
};
void query23BCallback (void *context, const MVProjection *tuple) {
  Q23BContext* con = reinterpret_cast<Q23BContext*>(context);
  assert (P_BRAND_SIZE == sizeof (tuple->p_brand));
  if (::memcmp (tuple->p_brand, con->p_brand, P_BRAND_SIZE) == 0) {
    map<int16_t, int64_t>::iterator yearIt = con->resultMap.find(tuple->key.d_year);
    if (yearIt == con->resultMap.end()) {
      con->resultMap[tuple->key.d_year] = tuple->l_revenue;
    } else {
      (yearIt->second) += tuple->l_revenue;
    }
  }
}
shared_ptr<SSBQueryResult> SSBQueryExecutorImpl::query23B (const SSBQueryParam &param) {
  assert (param.strings.size() >= 2);
  StopWatch watch;
  watch.init();
  Q23BContext context (param.strings[0]);
  btreeMVSearchSRegion(query23BCallback, &context, param.strings[1]);
  shared_ptr<SSBQueryResult> result = context.toResult();
  watch.stop();
  result->elapsedMicrosec = watch.getElapsed();
  VLOG(1) << "Q23B done: " << result->groupedResults.size() << " rows. " << result->elapsedMicrosec << " microsec";
  return result;
}
shared_ptr<SSBQueryResult> SSBQueryExecutorImpl::query23C (const SSBQueryParam &param) {
  assert (param.strings.size() >= 2);
  StopWatch watch;
  watch.init();

  FReadOnlyCStore mv (_bufferpool, MV_PROJECTION, _signatures, _dataFolder, CSTORE_MV_MAIN_PREFIX);

  FColumnReader *sregionReader = mv.getColumnReader("s_region");
  FColumnReaderRLE *yearReader = dynamic_cast<FColumnReaderRLE*>(mv.getColumnReader("d_year"));
  FColumnReader *revReader = mv.getColumnReader("l_revenue");
  FColumnReaderDictionary *brandReader = dynamic_cast<FColumnReaderDictionary*>(mv.getColumnReader("p_brand"));

  string p_brand = brandReader->normalize(param.strings[0]);
  string s_region = sregionReader->normalize(param.strings[1]);

  vector <PositionRange> regionRanges;
  sregionReader->getPositionRanges(SearchCond(EQUAL, s_region.data()), regionRanges);
  assert (regionRanges.size() == 1);
  const PositionRange &regionRange = regionRanges[0];

  vector <pair<PositionRange, int16_t> > yearRanges;
  yearReader->getRLECompressedData(regionRange, yearRanges);
  assert (yearRanges.size() > 0);
  size_t maxLen = 0;
  vector <PositionRange> yearRangesVec;
  for (size_t i = 0; i < yearRanges.size(); ++i) {
    const PositionRange &range = yearRanges[i].first;
    yearRangesVec.push_back (range);
    size_t length = range.end - range.begin;
    if (length > maxLen) maxLen = length;
  }

  scoped_array<int32_t> revBufferPtr(new int32_t[maxLen]);
  int32_t *revBuffer = (revBufferPtr.get());

  shared_ptr<SSBQueryResult> result (new SSBQueryResult(RESULT_GROUP_INT16));
  SSBQueryResult *resultRaw = result.get();

  vector<shared_ptr<PositionBitmap> > positions;
  brandReader->setSearchRanges(yearRangesVec);
  brandReader->getPositionBitmaps(SearchCond(EQUAL, p_brand.data()), positions);
  assert (positions.size () == yearRanges.size());

  int rows = 0;
  for (size_t i = 0; i < yearRanges.size(); ++i) {
    const PositionRange &range = yearRanges[i].first;
    size_t length = range.end - range.begin;
    int16_t year = yearRanges[i].second;
    string yearStr (reinterpret_cast<char *>(&year), sizeof(int16_t));

    revReader->getDecompressedData(range, revBuffer, maxLen * sizeof(int32_t));

    shared_ptr<PositionBitmap> position = positions[i];
    const unsigned char *bitmap = position->bitmap;

    int64_t sum = 0;
    for (size_t j = 0; j < length; ++j) {
      unsigned char byte = bitmap[j / 8];
      unsigned char bitmask = (unsigned char) 1 << (j % 8);
      if ((byte & bitmask) == bitmask) {
        sum += revBuffer[j];
      }
    }
    if (sum != 0) {
      ++rows;
      vector<string> groupString;
      groupString.push_back(yearStr);
      resultRaw->groupedResults[groupString] = sum;
    }
  }

  // in case there is on-memory current fracture, search on it too.
  Q23BContext context (param.strings[0]);
  btreeMVSearchSRegionMainMemory(CSTORE_MV_FAMILY, query23BCallback, &context, param.strings[1]);
  resultRaw->sumResult(*(context.toResult()));

  watch.stop();
  resultRaw->elapsedMicrosec = watch.getElapsed();
  VLOG(1) << "Q23C done: " << rows << " rows. " << watch.getElapsed() << " microsec";
  return result;
}

// Q3.1
// select c_nation, s_nation, d_year, sum(lo_revenue) as revenue from
// customer, lineorder, supplier, dwdate where lo_custkey = c_custkey and
// lo_suppkey = s_suppkey and lo_orderdate = d_datekey and c_region = 'ASIA'
// and s_region = 'ASIA' and d_year >= 1992 and d_year <= 1997 group by
// c_nation, s_nation, d_year order by d_year asc, revenue desc;

// Q3.2
// select c_city, s_city, d_year, sum(lo_revenue) as revenue from
// customer, lineorder, supplier, dwdate where lo_custkey = c_custkey and
// lo_suppkey = s_suppkey and lo_orderdate = d_datekey and c_nation =
// 'UNITED STATES' and s_nation = 'UNITED STATES' and d_year >= 1992 and
// d_year <= 1997 group by c_city, s_city, d_year order by d_year asc,
// revenue desc;

// Q3.3
// select c_city, s_city, d_year, sum(lo_revenue) as revenue from
// customer, lineorder, supplier, dwdate where lo_custkey = c_custkey and
// lo_suppkey = s_suppkey and lo_orderdate = d_datekey and (c_city='UNITED
// KI1' or c_city='UNITED KI5') and (s_city='UNITED KI1' or s_city='UNITED
// KI5') and d_year >= 1992 and d_year <= 1997 group by c_city, s_city,
// d_year order by d_year asc, revenue desc;

// Q3.4
// select c_city, s_city, d_year, sum(lo_revenue) as revenue from
// customer, lineorder, supplier, dwdate where lo_custkey = c_custkey and
// lo_suppkey = s_suppkey and lo_orderdate = d_datekey and (c_city='UNITED
// KI1' or c_city='UNITED KI5') and (s_city='UNITED KI1' or s_city='UNITED
// KI5') and d_yearmonth = 'Dec1997' group by c_city, s_city, d_year order
// by d_year asc, revenue desc;

// Q4.1
// select d_year, c_nation, sum(lo_revenue - lo_supplycost) as profit
// from dwdate, customer, supplier, part, lineorder where lo_custkey =
// c_custkey and lo_suppkey = s_suppkey and lo_partkey = p_partkey and
// lo_orderdate = d_datekey and c_region = 'AMERICA' and s_region =
// 'AMERICA' and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2') group by d_year,
// c_nation order by d_year, c_nation;

// Q4.2
// select d_year, s_nation, p_category, sum(lo_revenue - lo_supplycost)
// as profit from dwdate, customer, supplier, part, lineorder where
// lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_partkey =
// p_partkey and lo_orderdate = d_datekey and c_region = 'AMERICA' and
// s_region = 'AMERICA' and (d_year = 1997 or d_year = 1998) and (p_mfgr =
// 'MFGR#1' or p_mfgr = 'MFGR#2') group by d_year, s_nation, p_category
// order by d_year, s_nation, p_category;

// Q4.3
// select d_year, s_city, p_brand1, sum(lo_revenue - lo_supplycost) as
// profit from dwdate, customer, supplier, part, lineorder where lo_custkey
// = c_custkey and lo_suppkey = s_suppkey and lo_partkey = p_partkey and
// lo_orderdate = d_datekey and s_nation = 'UNITED STATES' and (d_year =
// 1997 or d_year = 1998) and p_category = 'MFGR#14' group by d_year,
// s_city, p_brand1; order by d_year, s_city, p_brand1;


// ==========================================================================
//  Query parameter random generation
// ==========================================================================
void SSBQueryParam::generateRandomParam (int query, int &seed) {
  ints.clear ();
  strings.clear();
  switch (query) {
  case 11: generateRandomParamQ11(seed);
    break;
  case 12: generateRandomParamQ12(seed);
    break;
  case 13: generateRandomParamQ13(seed);
    break;
  case 21: generateRandomParamQ21(seed);
    break;
  case 22: generateRandomParamQ22(seed);
    break;
  case 23: generateRandomParamQ23(seed);
    break;
  default:
    assert (false);
  }
}


// from shared.h and dss.h
#define  P_CAT_MIN     1
#define  P_CAT_MAX     5
// #define  P_MFG_FMT     "%s%01d"
#define  P_MFG_MIN     1
#define  P_MFG_MAX     5
// #define  P_BRND_TAG   "Brand#"
// #define  P_BRND_FMT   "%s%02d"
#define  P_BRND_MIN     1
#define  P_BRND_MAX 40
unsigned int generateUInt (int &seed, unsigned int beginVal, unsigned int endVal) {
  seed = ms_rand(seed);
  unsigned int seedVal = (unsigned int) seed;
  unsigned int val = beginVal + (seedVal % (endVal - beginVal));
  return val;
}

std::string generateRandomRegion (int &seed) {
  return REGIONS[generateUInt(seed, 0, 5)];
}
std::string generateRandomMfgr (int &seed) {
  stringstream str;
  str << "MFGR#" << generateUInt(seed, P_MFG_MIN, P_MFG_MAX);
  return str.str();
}
std::string generateRandomCategory (int &seed) {
  int mfgrId = generateUInt(seed, P_MFG_MIN, P_MFG_MAX);
  int categoryId = mfgrId * 10 + generateUInt(seed, P_CAT_MIN, P_CAT_MAX);
  stringstream str;
  str << "MFGR#" << categoryId;
  return str.str();
}
int generateRandomBrandId (int &seed) {
  int mfgrId = generateUInt(seed, P_MFG_MIN, P_MFG_MAX);
  int categoryId = mfgrId * 10 + generateUInt(seed, P_CAT_MIN, P_CAT_MAX);
  int brandId = generateUInt(seed, P_BRND_MIN, P_BRND_MAX);
  if (brandId < 10) {
    brandId = categoryId * 10 + brandId;
  } else {
    brandId = categoryId * 100 + brandId;
  }
  return brandId;
}
std::string generateRandomBrand (int &seed) {
  stringstream str;
  str << "MFGR#" << generateRandomBrandId(seed);
  return str.str();
}

int generateRandomQuery (int &seed) {
  const int implementedQueries [] = {11, 12, 13, 21, 22, 23};
  const size_t queryCount = 6;
  int query = implementedQueries[generateUInt(seed, 0, queryCount)];
  return query;
}

} //fdb
