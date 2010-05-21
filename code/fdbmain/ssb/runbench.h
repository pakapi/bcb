#ifndef SSB_RUNBENCH_H
#define SSB_RUNBENCH_H

#include <string>

namespace fdb {

void runSSBBench(size_t bufferPoolSize, bool cstore, bool sortedBuffer, int batchCount, int batchSize, int queriesBetweenBatch);

} // fdb
#endif // SSB_RUNBENCH_H
