#ifndef SSB_QUERYSSB_H
#define SSB_QUERYSSB_H

#include <stdint.h>
#include <map>
#include <string>
#include <vector>
#include <boost/shared_ptr.hpp>

namespace fdb {


unsigned int generateUInt (int &seed, unsigned int beginVal, unsigned int endVal);
int generateRandomQuery (int &seed);
std::string generateRandomRegion (int &seed);
std::string generateRandomMfgr (int &seed);
std::string generateRandomCategory (int &seed);
std::string generateRandomBrand (int &seed);
int generateRandomBrandId (int &seed);
struct SSBQueryParam {
  std::vector<int> ints;
  std::vector<std::string> strings;

  // randomly generate a well-formed query param for given query.
  // seed will be used and updated to assure deterministic randomness.
  void generateRandomParam (int query, int &seed);
  void generateRandomParamQ11 (int &seed);
  void generateRandomParamQ12 (int &seed);
  void generateRandomParamQ13 (int &seed);
  void generateRandomParamQ21 (int &seed);
  void generateRandomParamQ22 (int &seed);
  void generateRandomParamQ23 (int &seed);
};

enum ResultGroupColumType {
  RESULT_GROUP_INT8,
  RESULT_GROUP_INT16,
  RESULT_GROUP_INT32,
  RESULT_GROUP_INT64,
  RESULT_GROUP_STRING,
};
struct SSBQueryResult {
  SSBQueryResult () : elapsedMicrosec (0), singleIntResult(0) {}
  SSBQueryResult (int64_t elapsedMicrosec_) : elapsedMicrosec (elapsedMicrosec_), singleIntResult(0) {}
  SSBQueryResult (int64_t elapsedMicrosec_, int64_t singleIntResult_) : elapsedMicrosec (elapsedMicrosec_), singleIntResult (singleIntResult_) {}
  SSBQueryResult (ResultGroupColumType groupColumnType) : elapsedMicrosec (0), singleIntResult(0) {
    groupColumnTypes.push_back (groupColumnType);
  }
  SSBQueryResult (ResultGroupColumType groupColumnType1, ResultGroupColumType groupColumnType2) : elapsedMicrosec (0), singleIntResult(0) {
    groupColumnTypes.push_back (groupColumnType1);
    groupColumnTypes.push_back (groupColumnType2);
  }
  SSBQueryResult (ResultGroupColumType groupColumnType1, ResultGroupColumType groupColumnType2, ResultGroupColumType groupColumnType3) : elapsedMicrosec (0), singleIntResult(0) {
    groupColumnTypes.push_back (groupColumnType1);
    groupColumnTypes.push_back (groupColumnType2);
    groupColumnTypes.push_back (groupColumnType3);
  }
  SSBQueryResult (const std::vector<ResultGroupColumType> groupColumnTypes_) : elapsedMicrosec (0), singleIntResult(0), groupColumnTypes(groupColumnTypes_) {}

  // merge another results into this result, assuming the results are SUM
  void sumResult (const SSBQueryResult &other);

  std::string toString () const;

  int64_t elapsedMicrosec;

  // one of followings
  int64_t singleIntResult;

  typedef std::vector<std::string> ResultKey;
  typedef std::map<ResultKey, int64_t> ResultMap;
  typedef ResultMap::const_iterator ResultMapIter;

  ResultMap groupedResults;
  std::vector<ResultGroupColumType> groupColumnTypes;
};

class SSBQueryExecutorImpl;
class FEngine;
class SSBQueryExecutor {
public:
  SSBQueryExecutor(FEngine *engine);

  ~SSBQueryExecutor();

  // execute the specified SSB query (out of 13 queries defined in SSB spec)
  boost::shared_ptr<SSBQueryResult> query (int query, bool cstore, const SSBQueryParam &param);

  FEngine* getEngine();
  SSBQueryExecutorImpl* getImpl() {return _impl;} // only for testcases
private:
  SSBQueryExecutorImpl *_impl; //pimpl object
};


} // fdb
#endif // SSB_QUERYSSB_H
