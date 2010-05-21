#include "maketiny.h"
#include "ssb.h"
#include <fstream>
#include <map>
#include <set>
#include <boost/shared_ptr.hpp>
#include <boost/scoped_array.hpp>
#include <log4cxx/logger.h>

#define IO_BUFFER_SIZE (1 << 20)

using namespace std;
using namespace boost;
using namespace log4cxx;

namespace fdb {


shared_ptr< map<int, string> > readDimension(LoggerPtr logger, char *inBuffer,
    const std::string &originalTblFolder, const std::string &name) {
  shared_ptr<map<int, string> > resultPtr (new map<int, string>());
  map<int, string> *result = resultPtr.get();
  std::string inFilename = originalTblFolder + name + ".tbl";

  LOG4CXX_INFO(logger, "reading " << inFilename << "...");
  std::ifstream inFile(inFilename.c_str(), std::ios::in);
  if (!inFile) {
    LOG4CXX_ERROR(logger, "could not open " << inFilename);
    throw std::exception();
  }
  inFile.rdbuf()->pubsetbuf(inBuffer, IO_BUFFER_SIZE);

  std::string line;
  while (true) {
    std::getline(inFile, line);
    if (line == "") break;
    // first column should be ID column
    size_t pos = line.find ('|');
    assert (pos > 0 && pos < line.size());
    int id = ::atoi(line.substr(0, pos).c_str());
    assert (result->find (id) == result->end());
    (*result)[id] = line;
  }
  inFile.close();

  LOG4CXX_INFO(logger, "read.");
  return resultPtr;
}


void makeTinySSB(const std::string &originalTblFolder, const std::string &tinyTblFolder, size_t tuples) {
  LoggerPtr logger(Logger::getLogger("tinyssb"));
  LOG4CXX_INFO(logger, "converting SSB files in " << originalTblFolder << ", to tiny files into " << tinyTblFolder << " upto " << tuples << " tuples.");
  scoped_array<char> inPtr(new char[IO_BUFFER_SIZE]);
  char *inb = inPtr.get();

  vector<string> dimensions;
  dimensions.push_back("customer");
  dimensions.push_back("date");
  dimensions.push_back("part");
  dimensions.push_back("supplier");

  // first, read all tuples from dimension tables to collect referenced tuples.
  vector<shared_ptr<map<int, string> > > maps;
  std::vector<set<int> > ids; // referenced IDs. note that it's SET.
  for (size_t i = 0; i < dimensions.size(); ++i) {
    maps.push_back (readDimension(logger, inb, originalTblFolder, dimensions[i]));
    ids.push_back (set<int>());
  }

  // then read fact upto tuples tuples
  LOG4CXX_INFO(logger, "reading lineorder...");
  {
    std::string inFilename = originalTblFolder + "lineorder.tbl";
    std::string outFilename = tinyTblFolder + "lineorder.tbl";
  
    std::ifstream inFile(inFilename.c_str(), std::ios::in);
    if (!inFile) {
      LOG4CXX_ERROR(logger, "could not open " << inFilename);
      throw std::exception();
    }
    inFile.rdbuf()->pubsetbuf(inb, IO_BUFFER_SIZE);

    if (std::remove(outFilename.c_str()) == 0) {
      LOG4CXX_INFO(logger, "deleted existing file " << outFilename << ".");
    }
    std::ofstream outFile(outFilename.c_str());
    if (!outFile) {
      LOG4CXX_ERROR(logger, "could not open " << outFilename);
      throw std::exception();
    }
  
    std::string line;
    Lineorder l;
    for (size_t i = 0; i < tuples; ++i) {
      std::getline(inFile, line);
      if (line == "") break;
      l.loadDataPiped(line);
      outFile.write(line.c_str(), line.size());
      outFile.write("\n", 1);
      ids[0].insert (l.custkey);
      ids[1].insert (l.orderdate);
      ids[2].insert (l.partkey);
      ids[3].insert (l.suppkey);
    }
    outFile.flush();
    outFile.close();
    inFile.close();
  }

  for (size_t i = 0; i < dimensions.size(); ++i) {
    std::string outFilename = tinyTblFolder + dimensions[i] + ".tbl";

    if (std::remove(outFilename.c_str()) == 0) {
      LOG4CXX_INFO(logger, "deleted existing file " << outFilename << ".");
    }
    std::ofstream outFile(outFilename.c_str());
    if (!outFile) {
      LOG4CXX_ERROR(logger, "could not open " << outFilename);
      throw std::exception();
    }

    for (set<int>::const_iterator iter = ids[i].begin(); iter != ids[i].end(); ++iter) {
      int id = *iter;
      const string &line = (*maps[i])[id];
      outFile.write(line.c_str(), line.size());
      outFile.write("\n", 1);
    }
    outFile.flush();
    outFile.close();
  }

  LOG4CXX_INFO(logger, "converted.");
}

} // fdb
