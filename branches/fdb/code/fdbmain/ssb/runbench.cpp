#include "runbench.h"

#include "ssb.h"
#include "dbgen.h"
#include "queryssb.h"
#include "queryssbimpl.h"
#include "../engine/fengine.h"
#include "../engine/ffamily.h"
#include "../storage/fbtree.h"
#include "../util/stopwatch.h"
#include <fstream>
#include <glog/logging.h>

using namespace std;
using namespace boost;

namespace fdb {

int64_t finishFracture (FMainMemoryBTree &btree) {
  StopWatch watch;
  watch.init();
  btree.finishInserts();
  watch.stop();
  return watch.getElapsed();
}

void runSSBBench(size_t bufferPoolSize, bool cstore, bool sortedBuffer, int batchCount, int batchSize, int queriesBetweenBatch) {
  LOG(INFO) << "starting. bufferPoolSize=" << bufferPoolSize << ", cstore=" << cstore << ", sortedBuffer=" << sortedBuffer << ", batchCount=" << batchCount << ", batchSize=" << batchSize << ",queriesBetweenBatch=" << queriesBetweenBatch;


  FEngine engine ("../../data/", "../../data/data.sig", bufferPoolSize);
  DBGen dbGen ("../../data/ssb1/", batchSize);
  const int MAX_TUPLE = 5200000;
  if ((batchCount * batchSize) > MAX_TUPLE) {
    LOG(INFO) << "Too large. needs flushing.";
    return;
  }
  FMainMemoryBTree fractureMv (MV_PROJECTION, MAX_TUPLE, sortedBuffer);
  FMainMemoryBTree fractureLineorder (LINEORDER_PK_SORT, MAX_TUPLE, sortedBuffer);
  FFamily *family;
  if (cstore) {
    family = engine.createNewFractureFamily(CSTORE_MV_FAMILY);
  } else {
    family = engine.createNewFractureFamily(BTREE_MV_FAMILY);
  }
  family->setCurrentFracture(&fractureMv);
  SSBQueryExecutor exec (&engine);
  StopWatch watchTotal;
  watchTotal.init();

  SSBQueryParam param;
  int seed = 452345345;
  int64_t queryTotal = 0, insertTotal = 0;
  for (int i = 0; i < batchCount; ++i) {
    // query
    StopWatch watchQuery;
    watchQuery.init();
    for (int j = 0; j < queriesBetweenBatch; ++j) {
      int query = generateRandomQuery(seed);
      param.generateRandomParam(query, seed);
      exec.query(query, cstore, param);
    }
    watchQuery.stop();
    LOG(INFO) << "Query batch done. " << i << ". " << watchQuery.getElapsed() << " microsec";
    queryTotal += watchQuery.getElapsed();

    // insert
    StopWatch watchInsert;
    watchInsert.init();
    dbGen.generateNextBatch();
    size_t currentBatchSize = dbGen.getCurrentBatchSize();
    MVProjection *mvs = dbGen.getMVBuffer();
    Lineorder *lineorders = dbGen.getLineorderBuffer();
    for (size_t j = 0; j < currentBatchSize; ++j) {
      fractureMv.insert(&(mvs[j].key), &mvs[j]);
      int64_t pk = lineorders[j].getPK();
      fractureLineorder.insert(&pk, &lineorders[j]);
    }
    watchInsert.stop();
    LOG(INFO) << "Insert batch done. " << i << ". " << watchInsert.getElapsed() << " microsec";
    insertTotal += watchInsert.getElapsed();
  }

  int64_t mvTime = finishFracture (fractureMv);
  int64_t lineorderTime = finishFracture (fractureLineorder);

  watchTotal.stop();
  LOG(INFO) << "Finished benchmark: total:" << watchTotal.getElapsed() << " microsec. queryTotal=" << queryTotal << ", insertTotal=" << insertTotal << ", mvTime=" << mvTime << ", lineorderTime=" << lineorderTime;
}

} // fdb
