#include <cstdlib>
#include <string>
#include <string.h>
#include <iostream>

#include <glog/logging.h>

#include "configvalues.h"
#include "storage/ffile.h"
#include "ssb/loadssb.h"
#include "ssb/queryssb.h"
#include "ssb/runbench.h"
#include "ssb/maketiny.h"
#include "util/stopwatch.h"


using namespace std;
using namespace fdb;

// path to the data folder
#define DEFAULT_DATA_FOLDER "../../data/"

// name of the default data signature file
#define DEFAULT_SIGNATURE_FILE "data.sig"


int main(int argc, char *argv[]) {
  google::InitGoogleLogging ("fdbmain");
  google::SetLogDestination(google::INFO, "fdbmain.log.");
  google::SetStderrLogging(google::INFO);

  try {
    LOG(INFO) << "started.";
    if (argc < 2) {
      LOG(ERROR) << "Usage: fdbmain <command> ...";
      return EXIT_FAILURE;
    }
    string command(argv[1]);

    //if (command == "loadssbpiped") {
      // loadSSBPipedFile(DEFAULT_DATA_FOLDER, DEFAULT_SIGNATURE_FILE, "../../data/ssb1/");
    //} else
    if (command == "convertssb") {
      convertSSBPipedFile("../../data/ssb1/");
    } else if (command == "loadssb") {
      loadSSBBinFile(DEFAULT_DATA_FOLDER, DEFAULT_SIGNATURE_FILE, "../../data/ssb1/", false, 6001171);
    } else if (command == "loadssbcstore") {
      loadSSBBinFile(DEFAULT_DATA_FOLDER, DEFAULT_SIGNATURE_FILE, "../../data/ssb1/", true, 6001171);
    } else if (command == "loadssbmv") {
      loadSSBBinFileMV(DEFAULT_DATA_FOLDER, DEFAULT_SIGNATURE_FILE, "../../data/ssb1/", false, 6001171);
    } else if (command == "loadssbmvcstore") {
      loadSSBBinFileMV(DEFAULT_DATA_FOLDER, DEFAULT_SIGNATURE_FILE, "../../data/ssb1/", true, 6001171);
    } else if (command == "maketinyssb") {
      if (argc < 3) {
        LOG(ERROR) << "Usage: fdbmain maketinyssb <tuplecount>";
        return EXIT_FAILURE;
      }
      int tuples = ::atoi(argv[2]);
      makeTinySSB("../../data/ssb1/", "../../data/tinyssb/", tuples);
    } else if (command == "runbench") {
      if (argc < 8) {
        LOG(ERROR) << "Usage: fdbmain runbench <int:bufferPageCount> <flag:cstore> <flag:sortedBuffer> <int:batchCount> <int:batchSize> <int:queriesBetweenBatch>";
        return EXIT_FAILURE;
      }
      int bufferPageCount = ::atol(argv[2]);
      assert (bufferPageCount > 10);
      bool cstore = string(argv[3]) == "true";
      bool sortedBuffer = string(argv[4]) == "true";
      int batchCount = ::atol(argv[5]);
      assert (batchCount >= 0);
      int batchSize = ::atol(argv[6]);
      assert (batchSize >= 0);
      int queriesBetweenBatch = ::atol(argv[7]);
      assert (queriesBetweenBatch >= 0);

      runSSBBench(bufferPageCount, cstore, sortedBuffer, batchCount, batchSize, queriesBetweenBatch);
    } else if (command == "describe") {
      if (argc < 3) {
        LOG(ERROR) << "Usage: fdbmain describe <path of signature file>";
        return EXIT_FAILURE;
      }
      string filepath(argv[2]);
      FSignatureSet signatureFile;
      signatureFile.load ("", filepath);
      signatureFile.debugout();
/* this experiment is done
    } else if (command == "testmem") {
// performance test of memory caching.
      char *mem = new char[1 << 30]; // 1GB
      ::memset (mem, 1, 1 << 30);
      //suppose 128 (=1 << 7) bytes of tuples
      //500MB is 1 << 23 tuples

      // repeat 3 times to take average
      int64_t jumpingTotal = 0, nonJumpingTotal = 0;
      for (int i = 0; i < 3; ++i) {
        {
          LOG(INFO) << "testing jumping memory access...";
          // we are going to calculate sum of the first 1 byte of each tuple
          StopWatch w;
          w.init();
          int64_t sum = 0;
          const char *end = mem + (1 << 30);
          for (const char *cursor = mem; cursor != end; cursor += (1 << 7)) {
            sum += *cursor;
          }
          w.stop();
          jumpingTotal += w.getElapsed();
          LOG (INFO) << "done. " << w.getElapsed() << " microsec. sum=" << sum;
        }

        {
          LOG (INFO) << "testing sequential memory access...";
          // we are going to calculate sum of first 1 << 23 bytes
          StopWatch w;
          w.init();
          int64_t sum = 0;
          const char *end = mem + (1 << 23);
          for (const char *cursor = mem; cursor != end; ++cursor) {
            sum += *cursor;
          }
          w.stop();
          nonJumpingTotal += w.getElapsed();
          LOG(INFO) << "done. " << w.getElapsed() << " microsec. sum=" << sum;
        }
      }
      delete[] mem;
      LOG(INFO) << "total: jumping=" << jumpingTotal << " microsec";
      LOG(INFO) << "total: non-jumping=" << nonJumpingTotal << " microsec";
*/
    } else {
      LOG(ERROR) << "unknown command.";
      return EXIT_FAILURE;
    }


    LOG(INFO) << "ended.";
  } catch(std::exception &e) {
    LOG(ERROR) << "stdexception caught in main(): " << e.what();
  } 

  return EXIT_SUCCESS;
}
