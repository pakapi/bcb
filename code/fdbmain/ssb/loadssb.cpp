#include "loadssb.h"
#include "ssb.h"
#include "../storage/ffile.h"
#include "../storage/fbtree.h"
#include "../storage/fcstore.h"
#include "../util/stopwatch.h"
#include "../configvalues.h"
#include <algorithm>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string.h>
#include <log4cxx/logger.h>
#include <boost/scoped_array.hpp>
#include <boost/shared_ptr.hpp>

#define IO_BUFFER_SIZE (2 << 20)

using namespace std;
using namespace log4cxx;
using namespace boost;

namespace fdb {
/*
template <typename SSBObject>
void loadOneSSBPipedFile(
    LoggerPtr logger, char *buffer, FSignatureSet &signatureFile, const std::string &dataFolder,
    TableType tableType, const std::string &tblFolder, const std::string &tblName) {
  FMainMemoryBTree btree (tableType);
  scoped_array<char> keyBufferPtr(new char[toKeySize(tableType)]);
  char *keyBuffer = keyBufferPtr.get();
  ExtractKeyFromTupleFunc extractFunc = toExtractKeyFromTupleFunc(tableType);

  std::string filename = tblFolder + tblName;
  LOG4CXX_INFO(logger, "reading " << filename << "...");
  std::ifstream file(filename.c_str(), std::ios::in);
  if (!file) {
    LOG4CXX_ERROR(logger, "could not open " << filename);
    throw std::exception();
  }
  file.rdbuf()->pubsetbuf(buffer, IO_BUFFER_SIZE);
  std::string line;
  SSBObject obj;
  int count = 0;
  while (true) {
    std::getline(file, line);
    if (line == "") break;
    if (++count % 100000 == 0) {
      LOG4CXX_INFO(logger, "reading " << count);
    }
    obj.loadDataPiped(line);
    extractFunc (&obj, keyBuffer);
    btree.insert(keyBuffer, &obj);
  }

  LOG4CXX_INFO(logger, "constructred main memory BTree. writing to disk...");
  signatureFile.dumpToNewRowStoreFile(dataFolder, tblName + ".db", btree);
}

void loadSSBPipedFile(const std::string &dataFolder,
  const std::string &dataSignatureFile,
  const std::string &tblFolder) {

  LoggerPtr logger(Logger::getLogger("loadssb"));

  StopWatch watch;
  watch.init();
  if (std::remove((dataFolder + dataSignatureFile).c_str()) == 0) {
    LOG4CXX_INFO(logger, "deleted existing file " << (dataFolder + dataSignatureFile) << ".");
  }

  FSignatureSet signatureFile;
  signatureFile.load(dataFolder, dataSignatureFile);

  scoped_array<char> bufferAutoPtr(new char[IO_BUFFER_SIZE]);
  char *buffer = bufferAutoPtr.get();
  loadOneSSBPipedFile<Customer>(logger, buffer, signatureFile, dataFolder, CUSTOMER_PK_SORT, tblFolder, "customer.tbl");
  loadOneSSBPipedFile<Date>(logger, buffer, signatureFile, dataFolder, DATE_PK_SORT, tblFolder, "date.tbl");
  loadOneSSBPipedFile<Lineorder>(logger, buffer, signatureFile, dataFolder, LINEORDER_PK_SORT, tblFolder, "lineorder.tbl");
  loadOneSSBPipedFile<Part>(logger, buffer, signatureFile, dataFolder, PART_PK_SORT, tblFolder, "part.tbl");
  loadOneSSBPipedFile<Supplier>(logger, buffer, signatureFile, dataFolder, SUPPLIER_PK_SORT, tblFolder, "supplier.tbl");

  signatureFile.save(dataFolder, dataSignatureFile);

  watch.stop();
  LOG4CXX_INFO(logger, "loaded all SSB data from piped data file. " << watch.getElapsed() << " micsosec");
}
*/
template <typename SSBObject>
void convertOneSSBPipedFile(
    LoggerPtr logger, char *inBuffer, char *outBuffer,
    const std::string &tblFolder, const std::string &name) {
  std::string inFilename = tblFolder + name + ".tbl";
  std::string outFilename = tblFolder + name + ".bin";

  LOG4CXX_INFO(logger, "converting " << inFilename << " to " << outFilename << "...");
  std::ifstream inFile(inFilename.c_str(), std::ios::in);
  if (!inFile) {
    LOG4CXX_ERROR(logger, "could not open " << inFilename);
    throw std::exception();
  }
  inFile.rdbuf()->pubsetbuf(inBuffer, IO_BUFFER_SIZE);

  if (std::remove(outFilename.c_str()) == 0) {
    LOG4CXX_INFO(logger, "deleted existing file " << outFilename << ".");
  }
  std::ofstream outFile(outFilename.c_str(), std::ofstream::binary);
  if (!outFile) {
    LOG4CXX_ERROR(logger, "could not open " << outFilename);
    throw std::exception();
  }
  int outBufferPos = 0;
  ::memset (outBuffer, 0, IO_BUFFER_SIZE);

  std::string line;
  SSBObject obj;
  ::memset (&obj, 0, sizeof(SSBObject));
  int count = 0;
  while (true) {
    std::getline(inFile, line);
    if (line == "") break;
    if (++count % 100000 == 0) {
      LOG4CXX_INFO(logger, "converting " << count);
    }
    obj.loadDataPiped(line);
    ::memcpy (outBuffer + outBufferPos, &obj, sizeof(SSBObject));
    outBufferPos += sizeof(SSBObject);
    if (outBufferPos >= IO_BUFFER_SIZE * 9 / 10) {
      outFile.write(outBuffer, outBufferPos);
      ::memset (outBuffer, 0, IO_BUFFER_SIZE);
      outBufferPos = 0;
    }
  }
  if (outBufferPos > 0) {
    outFile.write(outBuffer, outBufferPos);
  }

  outFile.flush();
  outFile.close();
  inFile.close();

  LOG4CXX_INFO(logger, "converted.");
}

void convertSSBPipedFile(const std::string &tblFolder) {
  LoggerPtr logger(Logger::getLogger("convertssb"));
  StopWatch watch;
  watch.init();
  scoped_array<char> inPtr(new char[IO_BUFFER_SIZE]);
  scoped_array<char> outPtr(new char[IO_BUFFER_SIZE]);
  char *inb = inPtr.get();
  char *outb = outPtr.get();
  convertOneSSBPipedFile<Customer>(logger, inb, outb, tblFolder, "customer");
  convertOneSSBPipedFile<Date>(logger, inb, outb, tblFolder, "date");
  convertOneSSBPipedFile<Lineorder>(logger, inb, outb, tblFolder, "lineorder");
  convertOneSSBPipedFile<Part>(logger, inb, outb, tblFolder, "part");
  convertOneSSBPipedFile<Supplier>(logger, inb, outb, tblFolder, "supplier");
  ::sync ();
  watch.stop();
  LOG4CXX_INFO(logger, "converted all SSB tbl files to bin files. " << watch.getElapsed() << " micsosec");
}

shared_ptr<FMainMemoryBTree> loadOneSSBBinFile(
    LoggerPtr logger, char *buffer, FSignatureSet &signatureFile, const std::string &dataFolder,
    TableType tableType, int64_t maxSize, const std::string &tblFolder, const std::string &tblName, bool cstore) {
  shared_ptr<FMainMemoryBTree> btreePtr(new FMainMemoryBTree(tableType, maxSize, false));
  FMainMemoryBTree *btree = btreePtr.get();
  scoped_array<char> keyBufferPtr(new char[toKeySize(tableType)]);
  char *keyBuffer = keyBufferPtr.get();
  ::memset (keyBuffer, 0, toKeySize(tableType));
  ExtractKeyFromTupleFunc extractFunc = toExtractKeyFromTupleFunc(tableType);

  StopWatch watch;
  watch.init();
  std::string filename = tblFolder + tblName;
  LOG4CXX_INFO(logger, "reading " << filename << "...");
  std::ifstream file(filename.c_str(), std::ios::in | std::ios::binary);
  if (!file) {
    LOG4CXX_ERROR(logger, "could not open " << filename);
    throw std::exception();
  }
  size_t maxBufCount = IO_BUFFER_SIZE / btree->getDataSize();
  size_t maxBufSize = maxBufCount * btree->getDataSize();
  assert (maxBufSize <= IO_BUFFER_SIZE);
  int count = 0;
  while (true) {
    file.read (buffer, maxBufSize);
    size_t readSize = file.gcount();
    for (size_t pos = 0; pos + btree->getDataSize() <= readSize; pos += btree->getDataSize()) {
      if (++count % 100000 == 0) {
        LOG4CXX_INFO(logger, "reading " << count);
      }
      extractFunc (buffer + pos, keyBuffer);
      btree->insert(keyBuffer, buffer + pos);
    }
    if (readSize < maxBufSize) break;
  }
  LOG4CXX_INFO(logger, "read " << count << " lines");
  btree->finishInserts();
  file.close();
  watch.stop();
  LOG4CXX_INFO(logger, "constructred main memory BTree (" << watch.getElapsed() << " micsosec for reading and constructing). writing to disk...");
  if (cstore) {
    LOG4CXX_INFO(logger, "cstore");
    signatureFile.dumpToNewCStoreFiles(dataFolder, tblName, *btree);
  } else {
    LOG4CXX_INFO(logger, "rowstore");
    signatureFile.dumpToNewRowStoreFile(dataFolder, tblName + ".db", *btree);
  }
  return btreePtr;
}

void loadSSBBinFile(const std::string &dataFolder,
  const std::string &dataSignatureFile,
  const std::string &tblFolder, bool cstore, size_t lineorderSize) {
  LoggerPtr logger(Logger::getLogger("loadssb"));

  StopWatch watch;
  watch.init();

//  if (std::remove((dataFolder + dataSignatureFile).c_str()) == 0) {
//    LOG4CXX_INFO(logger, "deleted existing file " << dataFolder + dataSignatureFile << ".");
//  }

  FSignatureSet signatureFile;
  signatureFile.load(dataFolder, dataSignatureFile);

  scoped_array<char> bufferAutoPtr(new char[IO_BUFFER_SIZE]);
  char *buffer = bufferAutoPtr.get();
  loadOneSSBBinFile(logger, buffer, signatureFile, dataFolder, CUSTOMER_PK_SORT, 30000, tblFolder, "customer.bin", cstore);
  loadOneSSBBinFile(logger, buffer, signatureFile, dataFolder, DATE_PK_SORT, 2556, tblFolder, "date.bin", cstore);
  loadOneSSBBinFile(logger, buffer, signatureFile, dataFolder, LINEORDER_PK_SORT, lineorderSize, tblFolder, "lineorder.bin", cstore);
  loadOneSSBBinFile(logger, buffer, signatureFile, dataFolder, PART_PK_SORT, 200000, tblFolder, "part.bin", cstore);
  loadOneSSBBinFile(logger, buffer, signatureFile, dataFolder, SUPPLIER_PK_SORT, 10000, tblFolder, "supplier.bin", cstore);

  signatureFile.save(dataFolder, dataSignatureFile);
  ::sync ();

  watch.stop();
  LOG4CXX_INFO(logger, "loaded all SSB data from bin files. " << watch.getElapsed() << " micsosec");
}

void loadLineorderMVAndBase(
    LoggerPtr logger, char *buffer, FSignatureSet &signatureFile, const std::string &dataFolder,
    const std::string &tblFolder, const std::string &tblName, bool cstore, size_t lineorderSize,
    FMainMemoryBTree *cb, FMainMemoryBTree *db, FMainMemoryBTree *pb, FMainMemoryBTree *sb) {
  FMainMemoryBTree baseBtree(LINEORDER_PK_SORT, lineorderSize, false);
  FMainMemoryBTree mvBtree(MV_PROJECTION, lineorderSize, false);

  StopWatch watch;
  watch.init();
  std::string filename = tblFolder + tblName;
  LOG4CXX_INFO(logger, "reading " << filename << " to base table and MV...");
  std::ifstream file(filename.c_str(), std::ios::in | std::ios::binary);
  if (!file) {
    LOG4CXX_ERROR(logger, "could not open " << filename);
    throw std::exception();
  }
  size_t maxBufCount = IO_BUFFER_SIZE / baseBtree.getDataSize();
  size_t maxBufSize = maxBufCount * baseBtree.getDataSize();
  assert (maxBufSize <= IO_BUFFER_SIZE);
  int count = 0;
  MVProjection mvp;
  ::memset (&mvp, 0, sizeof(MVProjection));
  while (true) {
    file.read (buffer, maxBufSize);
    size_t readSize = file.gcount();
    for (size_t pos = 0; pos + baseBtree.getDataSize() <= readSize; pos += baseBtree.getDataSize()) {
      if (++count % 100000 == 0) {
        LOG4CXX_INFO(logger, "reading " << count);
      }
      Lineorder *l = reinterpret_cast<Lineorder*>(buffer + pos);
      Lineorder::PKType pk = l->getPK();
      baseBtree.insert(&pk, buffer + pos);

      const Customer *c = reinterpret_cast<const Customer*> (cb->getSingleTupleByKey(&(l->custkey)));
      assert (c != NULL);
      const Date *d = reinterpret_cast<const Date*> (db->getSingleTupleByKey(&(l->orderdate)));
      assert (d != NULL);
      const Part *p = reinterpret_cast<const Part*> (pb->getSingleTupleByKey(&(l->partkey)));
      assert (p != NULL);
      const Supplier *s = reinterpret_cast<const Supplier*> (sb->getSingleTupleByKey(&(l->suppkey)));
      assert (s != NULL);
      mvp.assign(*l, *c, *d, *p, *s);
      mvBtree.insert (&(mvp.key), &mvp);
    }
    if (readSize < maxBufSize) break;
  }
  LOG4CXX_INFO(logger, "read " << count << " lines");
  baseBtree.finishInserts();
  mvBtree.finishInserts();
  file.close();
  watch.stop();
  LOG4CXX_INFO(logger, "constructred main memory BTree (" << watch.getElapsed() << " micsosec for reading and constructing). writing to disk...");
  if (cstore) {
    LOG4CXX_INFO(logger, "cstore");
    signatureFile.dumpToNewCStoreFiles(dataFolder, tblName, baseBtree);
    signatureFile.dumpToNewCStoreFiles(dataFolder, "mvprojection", mvBtree);
  } else {
    LOG4CXX_INFO(logger, "rowstore");
    signatureFile.dumpToNewRowStoreFile(dataFolder, tblName + ".db", baseBtree);
    signatureFile.dumpToNewRowStoreFile(dataFolder, "mvprojection.db", mvBtree);
  }
  ::sync ();
}

void loadSSBBinFileMV(const std::string &dataFolder,
  const std::string &dataSignatureFile,
  const std::string &tblFolder, bool cstore, size_t lineorderSize) {
  LoggerPtr logger(Logger::getLogger("loadssbmv"));

  StopWatch watch;
  watch.init();

//  if (std::remove((dataFolder + dataSignatureFile).c_str()) == 0) {
//    LOG4CXX_INFO(logger, "deleted existing file " << dataFolder + dataSignatureFile << ".");
//  }
  FSignatureSet signatureFile;
  signatureFile.load(dataFolder, dataSignatureFile);

  scoped_array<char> bufferAutoPtr(new char[IO_BUFFER_SIZE]);
  char *buffer = bufferAutoPtr.get();
  shared_ptr<FMainMemoryBTree> cb = loadOneSSBBinFile(logger, buffer, signatureFile, dataFolder, CUSTOMER_PK_SORT, 30000, tblFolder, "customer.bin", cstore);
  shared_ptr<FMainMemoryBTree> db = loadOneSSBBinFile(logger, buffer, signatureFile, dataFolder, DATE_PK_SORT, 2556, tblFolder, "date.bin", cstore);
  shared_ptr<FMainMemoryBTree> pb = loadOneSSBBinFile(logger, buffer, signatureFile, dataFolder, PART_PK_SORT, 200000, tblFolder, "part.bin", cstore);
  shared_ptr<FMainMemoryBTree> sb = loadOneSSBBinFile(logger, buffer, signatureFile, dataFolder, SUPPLIER_PK_SORT, 10000, tblFolder, "supplier.bin", cstore);

  loadLineorderMVAndBase(
    logger, buffer, signatureFile, dataFolder,
    tblFolder, "lineorder.bin", cstore, lineorderSize,
    cb.get(), db.get(), pb.get(), sb.get());

  signatureFile.save(dataFolder, dataSignatureFile);
  ::sync ();

  watch.stop();
  LOG4CXX_INFO(logger, "loaded all SSB data from bin files. " << watch.getElapsed() << " micsosec");
}

} //fdb
