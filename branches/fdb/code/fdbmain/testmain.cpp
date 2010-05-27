
#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE FDB
#include <boost/test/unit_test.hpp>

#include <glog/logging.h>

#include <string.h>
#include <algorithm>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <boost/shared_ptr.hpp>

#include "configvalues.h"
#include "engine/fengine.h"
#include "engine/ffamily.h"
#include "ssb/dbgen.h"
#include "ssb/ssb.h"
#include "ssb/loadssb.h"
#include "ssb/queryssb.h"
#include "ssb/queryssbimpl.h"
#include "ssb/maketiny.h"
#include "storage/ffile.h"
#include "storage/fbufferpool.h"
#include "storage/fbufferpoolimpl.h"
#include "storage/fbtree.h"
#include "storage/fcstore.h"
#include "storage/searchcond.h"
#include "util/hashmap.h"

#ifdef WIN32
  #define NOGDI
  #include <windows.h>
  void sleepSec (int sec) {
    ::Sleep (sec * 1000);
  }
#else //WIN32
  void sleepSec (int sec) {
    ::sleep (sec);
  }
#endif //WIN32


using namespace std;
using namespace fdb;

#define TEST_DATA_FOLDER "../../data/test/"

BOOST_AUTO_TEST_CASE(init) {
  BOOST_TEST_MESSAGE("===Initializing tests...");
  google::InitGoogleLogging ("testmain");
  google::SetLogDestination(google::INFO, "testmain.log.");
  google::SetStderrLogging(google::INFO);
  // google::SetVLOGLevel ("", 1); this crashes.. wtf?
  BOOST_TEST_MESSAGE("===Initialized tests.");
}

/*
BOOST_AUTO_TEST_CASE(storage_test_sig) {
  BOOST_TEST_MESSAGE("===Testing FSignatureSet...");

  std::remove((TEST_DATA_FOLDER + string("_test.sig")).c_str());

  FSignatureSet signatureFile;
  signatureFile.load(TEST_DATA_FOLDER, "_test.sig");
  BOOST_CHECK_EQUAL (signatureFile.size(), 0);
  BOOST_CHECK_EQUAL (signatureFile.isDirty(), false);
  signatureFile.save(TEST_DATA_FOLDER, "_test.sig");
  BOOST_CHECK_EQUAL (signatureFile.size(), 0);
  BOOST_CHECK_EQUAL (signatureFile.isDirty(), false);

  BOOST_CHECK_EQUAL (signatureFile.existsFile(1), false);
  BOOST_CHECK_EQUAL (signatureFile.existsFile(2), false);

  FFileSignature sig1;
  sig1.rootPageStart = 0;
  sig1.rootPageCount = 10;
  sig1.rootPageLevel = 1;
  sig1.pageCount = 200;
  sig1.leafEntrySize = 150;
  sig1.keyEntrySize = 16;
  sig1.keyCompareFuncType = INT32_ASC_NODUP;
  sig1.tableType = LINEORDER_PK_SORT;
  ::memcpy(sig1.filepath, "_test.1", 7);
  BOOST_CHECK_EQUAL (sig1.fileId, 0);
  sig1.fileId = signatureFile.issueNextFileId();
  BOOST_CHECK_EQUAL (sig1.fileId, 1);
  signatureFile.addFileSignature(sig1);

  BOOST_CHECK_EQUAL (signatureFile.size(), 1);
  BOOST_CHECK_EQUAL (signatureFile.isDirty(), true);

  BOOST_CHECK_EQUAL (signatureFile.existsFile(1), true);
  BOOST_CHECK_EQUAL (signatureFile.existsFile(2), false);

  FFileSignature sig2;
  sig2.rootPageStart = 0;
  sig2.rootPageCount = 15;
  sig2.rootPageLevel = 2;
  sig2.pageCount = 300;
  sig2.leafEntrySize = 250;
  sig2.keyEntrySize = 32;
  sig2.keyCompareFuncType = INT64_ASC_NODUP;
  sig2.tableType = LINEORDER_PK_SORT;
  ::memcpy(sig2.filepath, "_test.2", 7);
  BOOST_CHECK_EQUAL (sig2.fileId, 0);
  sig2.fileId = signatureFile.issueNextFileId();
  BOOST_CHECK_EQUAL (sig2.fileId, 2);
  signatureFile.addFileSignature(sig2);

  BOOST_CHECK_EQUAL (signatureFile.size(), 2);
  BOOST_CHECK_EQUAL (signatureFile.isDirty(), true);

  BOOST_CHECK_EQUAL (signatureFile.existsFile(1), true);
  BOOST_CHECK_EQUAL (signatureFile.existsFile(2), true);

  BOOST_CHECK_EQUAL (signatureFile.getFileSignature(1).pageCount, 200);
  BOOST_CHECK_EQUAL (signatureFile.getFileSignature(2).pageCount, 300);

  signatureFile.save(TEST_DATA_FOLDER, "_test.sig");
  BOOST_CHECK_EQUAL (signatureFile.size(), 2);
  BOOST_CHECK_EQUAL (signatureFile.isDirty(), false);

  FSignatureSet signatureFile2;
  signatureFile2.load(TEST_DATA_FOLDER, "_test.sig");
  BOOST_CHECK_EQUAL (signatureFile2.size(), 2);
  BOOST_CHECK_EQUAL (signatureFile2.isDirty(), false);

  BOOST_REQUIRE (signatureFile2.existsFile("_test.1"));
  const FFileSignature &sig1Loaded = signatureFile2.getFileSignature("_test.1");
  BOOST_CHECK_EQUAL (sig1Loaded.signatureVersion, FFILE_SIGNATURE_CUR_VER);
  BOOST_CHECK_EQUAL (sig1Loaded.rootPageStart, 0);
  BOOST_CHECK_EQUAL (sig1Loaded.rootPageCount, 10);
  BOOST_CHECK_EQUAL (sig1Loaded.rootPageLevel, 1);
  BOOST_CHECK_EQUAL (sig1Loaded.pageCount, 200);
  BOOST_CHECK_EQUAL (sig1Loaded.leafEntrySize, 150);
  BOOST_CHECK_EQUAL (sig1Loaded.keyEntrySize, 16);
  BOOST_CHECK_EQUAL (sig1Loaded.keyCompareFuncType, INT32_ASC_NODUP);
  BOOST_CHECK_EQUAL (sig1Loaded.filepath, "_test.1");
  BOOST_CHECK_EQUAL (sig1Loaded.fileId, 1);


  BOOST_REQUIRE (signatureFile2.existsFile("_test.2"));
  const FFileSignature &sig2Loaded = signatureFile2.getFileSignature("_test.2");
  BOOST_CHECK_EQUAL (sig2Loaded.signatureVersion, FFILE_SIGNATURE_CUR_VER);
  BOOST_CHECK_EQUAL (sig2Loaded.rootPageStart, 0);
  BOOST_CHECK_EQUAL (sig2Loaded.rootPageCount, 15);
  BOOST_CHECK_EQUAL (sig2Loaded.rootPageLevel, 2);
  BOOST_CHECK_EQUAL (sig2Loaded.pageCount, 300);
  BOOST_CHECK_EQUAL (sig2Loaded.leafEntrySize, 250);
  BOOST_CHECK_EQUAL (sig2Loaded.keyEntrySize, 32);
  BOOST_CHECK_EQUAL (sig2Loaded.keyCompareFuncType, INT64_ASC_NODUP);
  BOOST_CHECK_EQUAL (sig2Loaded.filepath, "_test.2");
  BOOST_CHECK_EQUAL (sig2Loaded.fileId, 2);

  BOOST_TEST_MESSAGE("===Tested FSignatureSet.");
}


BOOST_AUTO_TEST_CASE(storage_test_bp) {
  BOOST_TEST_MESSAGE("===Testing FBufferPool...");
  {
    FBufferPool pool (5);
    FBufferPoolImpl *impl = pool.getImpl();
    BOOST_CHECK_EQUAL (impl->_maxPageCount, 5);
    char *data[10];
    BOOST_TEST_MESSAGE("- adding entries...");
    for (int i = 0; i < 5; ++i) {
      data[i] = (char*) DirectFileStream::allocateMemoryForIO(FDB_DIRECT_IO_ALIGNMENT, FDB_DIRECT_IO_ALIGNMENT, FDB_USE_DIRECT_IO);
      pool.addPage(i * 3 + 1, i * 24 + 2, data[i]);
      BOOST_CHECK_EQUAL (impl->_clockHand, i + 1);
      PoolEntry *entry = impl->findEntry(i * 3 + 1, i * 24 + 2);
      BOOST_REQUIRE (entry != NULL);
      BOOST_CHECK_EQUAL (entry->fileId, i * 3 + 1);
      BOOST_CHECK_EQUAL (entry->pageId, i * 24 + 2);
      BOOST_CHECK_EQUAL (entry->data, data[i]);
      BOOST_CHECK_EQUAL (impl->_idMap.size(), i + 1);
      BOOST_CHECK_EQUAL (pool.findPage(i * 3 + 1, i * 24 + 2), data[i]);
    }
    BOOST_TEST_MESSAGE("- adding entries that evict something...");
    for (int i = 5; i < 10; ++i) {
      data[i] = (char*) DirectFileStream::allocateMemoryForIO(FDB_DIRECT_IO_ALIGNMENT, FDB_DIRECT_IO_ALIGNMENT, FDB_USE_DIRECT_IO);
      BOOST_CHECK_EQUAL (pool.findPage(2 * 3 + 1, 2 * 24 + 2), data[2]); // to keep 2
      pool.addPage(i * 3 + 1, i * 24 + 2, data[i]);
      PoolEntry *entry = impl->findEntry(i * 3 + 1, i * 24 + 2);
      BOOST_REQUIRE (entry != NULL);
      BOOST_CHECK_EQUAL (entry->fileId, i * 3 + 1);
      BOOST_CHECK_EQUAL (entry->pageId, i * 24 + 2);
      BOOST_CHECK_EQUAL (entry->data, data[i]);
      BOOST_CHECK_EQUAL (impl->_idMap.size(), 5);
    }
    BOOST_TEST_MESSAGE("- final checking...");
    for (int i = 0; i < 10; ++i) {
      if (i >= 6 || i == 2) {
        BOOST_CHECK (pool.findPage(i * 3 + 1, i * 24 + 2) ==  data[i]);
        BOOST_CHECK (impl->_idMap.find(toFilePageId(i * 3 + 1, i * 24 + 2)) != impl->_idMap.end());
      } else {
        BOOST_CHECK (pool.findPage(i * 3 + 1, i * 24 + 2) ==  NULL);
        BOOST_CHECK (impl->_idMap.find(toFilePageId(i * 3 + 1, i * 24 + 2)) == impl->_idMap.end());
      }
    }
  }
  BOOST_TEST_MESSAGE("===Tested FBufferPool.");
}


void testTraversalCallback (void *context, const void *key, const void *data) {
  Lineorder::PKType k = *(reinterpret_cast<const Lineorder::PKType*>(key));
  const Lineorder *l = reinterpret_cast<const Lineorder*>(data);
  int *count = (int*) context;
  BOOST_CHECK_EQUAL (l->orderkey, *count);
  BOOST_CHECK_EQUAL (l->linenumber, (*count) / 100);
  BOOST_CHECK_EQUAL (k, (((int64_t)l->orderkey) << 8) + ((int64_t) l->linenumber));
  BOOST_CHECK_EQUAL (l->custkey, 2020);
  BOOST_CHECK_EQUAL (l->revenue, 1326730);
  BOOST_CHECK_EQUAL (l->shipmode, "RAIL");
  ++(*count);
}

BOOST_AUTO_TEST_CASE(storage_test_mainmemory_btree) {
  BOOST_TEST_MESSAGE("===Testing FMainMemoryBTree...");
  std::remove((TEST_DATA_FOLDER + string("_test2.sig")).c_str());

  FSignatureSet signatureFile;
  for (int sorted = 0; sorted < 2; ++sorted) {
    BOOST_TEST_MESSAGE("--inserting to btree...");
    FMainMemoryBTree btree (LINEORDER_PK_SORT, 2500, sorted != 0);
    for (int i = 0; i < 2500; ++i) {
      Lineorder l;
      stringstream line;
      line << (i) <<"|" << (i/100) << "|2020|53077|626|19931213|3-MEDIUM|0|14|1442098|24363867|8|1326730|61804|0|19940130|RAIL";
      string str = line.str();
      l.loadDataPiped(str);
      Lineorder::PKType pk = l.getPK();
      BOOST_REQUIRE_EQUAL (pk, (((int64_t)l.orderkey) << 8) + ((int64_t) l.linenumber));
      btree.insert(&pk, &l);
    }
    btree.finishInserts();

    BOOST_TEST_MESSAGE("--traversing btree...");
    int count = 0;
    btree.traverse(testTraversalCallback, &count);
    BOOST_CHECK_EQUAL (count, 2500);

    BOOST_TEST_MESSAGE("--dumping to disk...");
    FFileSignature sig = signatureFile.dumpToNewRowStoreFile(TEST_DATA_FOLDER, sorted == 0 ? "test1_unsorted.db" : "test1_sorted.db", btree);
    BOOST_CHECK_EQUAL (sig.totalTupleCount, 2500);
  }
  signatureFile.save(TEST_DATA_FOLDER, "_test2.sig");
  BOOST_TEST_MESSAGE("===Tested FMainMemoryBTree.");
}
#if 0
BOOST_AUTO_TEST_CASE(ssb_test_load) {
  BOOST_TEST_MESSAGE("===Testing SSB loading...");
  std::remove((TEST_DATA_FOLDER + string("_tinyssb.sig")).c_str());
  loadSSBPipedFile(TEST_DATA_FOLDER, "_tinyssb.sig", "../../data/tinyssb/");
  BOOST_TEST_MESSAGE("===Tested SSB loading.");
}
#endif
BOOST_AUTO_TEST_CASE(ssb_test_maketiny) {
  BOOST_TEST_MESSAGE("===Testing tiny SSB data generation...");
  makeTinySSB("../../data/tinyssb/", TEST_DATA_FOLDER, 5);
  BOOST_TEST_MESSAGE("===Tested tiny SSB data generation.");
}
BOOST_AUTO_TEST_CASE(ssb_test_convert) {
  BOOST_TEST_MESSAGE("===Testing SSB converting...");
  convertSSBPipedFile("../../data/tinyssb/");
  std::remove((TEST_DATA_FOLDER + string("_tinyssb.sig")).c_str());
  loadSSBBinFile(TEST_DATA_FOLDER, "_tinyssb.sig", "../../data/tinyssb/", false, 20);
  loadSSBBinFile(TEST_DATA_FOLDER, "_tinyssb.sig", "../../data/tinyssb/", true, 20);
  sleepSec (1); // for some reason this makes following testcases happy. probably file flushing?
  std::remove((TEST_DATA_FOLDER + string("_tinyssb.sig")).c_str());
  loadSSBBinFileMV(TEST_DATA_FOLDER, "_tinyssb.sig", "../../data/tinyssb/", false, 20);
  loadSSBBinFileMV(TEST_DATA_FOLDER, "_tinyssb.sig", "../../data/tinyssb/", true, 20);
  sleepSec (2); // for some reason this makes following testcases happy. probably file flushing?
  BOOST_TEST_MESSAGE("===Tested SSB converting.");
}
BOOST_AUTO_TEST_CASE(storage_cstore_normalize) {
  BOOST_TEST_MESSAGE("===Testing CStore reader.normalize()...");
  FSignatureSet signatures;
  signatures.load (TEST_DATA_FOLDER, "_tinyssb.sig");
  FBufferPool bufferpool (100);
  FReadOnlyCStore customer (&bufferpool, CUSTOMER_PK_SORT, signatures, TEST_DATA_FOLDER, "customer.bin");
  FColumnReader *reader = customer.getColumnReader("name");
  string str = reader->normalize("Customer#000000003");
  BOOST_CHECK_EQUAL (str.size(), reader->getColumn().maxLength);
  BOOST_CHECK_EQUAL (::memcmp(str.data(), "Customer#000000003", string("Customer#000000003").size()), 0);
  for (int i = string("Customer#000000003").size(); i < reader->getColumn().maxLength; ++i) {
    BOOST_CHECK_EQUAL (str[i], '\0');
  }
  BOOST_TEST_MESSAGE("===Tested CStore reader.normalize().");
}

BOOST_AUTO_TEST_CASE(storage_cstore_uncompressed) {
  BOOST_TEST_MESSAGE("===Testing Uncompressed CStore column...");
  FSignatureSet signatures;
  signatures.load (TEST_DATA_FOLDER, "_tinyssb.sig");
  BOOST_REQUIRE (signatures.size() > 0);
  FBufferPool bufferpool (100);

  FReadOnlyCStore lineorder (&bufferpool, LINEORDER_PK_SORT, signatures, TEST_DATA_FOLDER, "lineorder.bin");
  FReadOnlyCStore customer (&bufferpool, CUSTOMER_PK_SORT, signatures, TEST_DATA_FOLDER, "customer.bin");

  PositionRange range (0, 10);
  vector<PositionRange> ranges;
  ranges.push_back (range);

  {
    FColumnReader *reader = lineorder.getColumnReader("revenue");
    BOOST_CHECK (reader != NULL);
    BOOST_CHECK_EQUAL (reader->getColumn().name, "revenue");
    reader->setSearchRanges(ranges);
    vector<boost::shared_ptr<PositionBitmap> > ret;
    int32_t key = 4763742;
    reader->getPositionBitmaps(SearchCond(SCT_EQUAL, &key), ret);
    BOOST_CHECK_EQUAL (ret.size(), 1);
    PositionBitmap *bitmap = ret[0].get();
    BOOST_CHECK (bitmap != NULL);
    BOOST_CHECK_EQUAL (bitmap->bitLength, 10);
    BOOST_CHECK_EQUAL (bitmap->byteLength, 2);
    BOOST_CHECK_EQUAL (bitmap->bitmap[0], (1 << 5));
    BOOST_CHECK_EQUAL (bitmap->bitmap[1], 0);
    BOOST_CHECK_EQUAL (bitmap->matchedCount, 1);

    key = 4000000;
    int32_t key2 = 5000000;
    ret.clear();
    reader->getPositionBitmaps(SearchCond(&key, &key2), ret);
    BOOST_CHECK_EQUAL (ret.size(), 1);
    bitmap = ret[0].get();
    BOOST_CHECK (bitmap != NULL);
    BOOST_CHECK_EQUAL (bitmap->bitLength, 10);
    BOOST_CHECK_EQUAL (bitmap->byteLength, 2);
    BOOST_CHECK_EQUAL (bitmap->bitmap[0], (1 << 1) | (1 << 3) | (1 << 4) | (1 << 5));
    BOOST_CHECK_EQUAL (bitmap->bitmap[1], (1 << 0));
    BOOST_CHECK_EQUAL (bitmap->matchedCount, 5);
  }

  vector<PositionRange> cranges;
  cranges.push_back (PositionRange (0, 6));
  {
    FColumnReader *reader = customer.getColumnReader(1);
    BOOST_CHECK (reader != NULL);
    BOOST_CHECK_EQUAL (reader->getColumn().name, "name");
    reader->setSearchRanges(cranges);
    vector<boost::shared_ptr<PositionBitmap> > ret;
    string key = reader->normalize("Customer#000013813");
    reader->getPositionBitmaps(SearchCond(SCT_EQUAL, key.data()), ret);
    BOOST_CHECK_EQUAL (ret.size(), 1);
    PositionBitmap *bitmap = ret[0].get();
    BOOST_CHECK (bitmap != NULL);
    BOOST_CHECK_EQUAL (bitmap->bitLength, 6);
    BOOST_CHECK_EQUAL (bitmap->byteLength, 1);
    BOOST_CHECK_EQUAL (bitmap->bitmap[0], (1 << 2));
    BOOST_CHECK_EQUAL (bitmap->matchedCount, 1);

    ret.clear();
    reader->getPositionBitmaps(SearchCond(SCT_LT, key.data()), ret);
    BOOST_CHECK_EQUAL (ret.size(), 1);
    bitmap = ret[0].get();
    BOOST_CHECK (bitmap != NULL);
    BOOST_CHECK_EQUAL (bitmap->bitLength, 6);
    BOOST_CHECK_EQUAL (bitmap->byteLength, 1);
    BOOST_CHECK_EQUAL (bitmap->bitmap[0], (1 << 0) | (1 << 1));
    BOOST_CHECK_EQUAL (bitmap->matchedCount, 2);
  }
  BOOST_TEST_MESSAGE("===Tested Uncompressed CStore column.");
}
BOOST_AUTO_TEST_CASE(storage_cstore_dictionary) {
  BOOST_TEST_MESSAGE("===Testing Dictionary compressed CStore column...");
  FSignatureSet signatures;
  signatures.load (TEST_DATA_FOLDER, "_tinyssb.sig");
  BOOST_REQUIRE (signatures.size() > 0);
  FBufferPool bufferpool (100);

  FReadOnlyCStore lineorder (&bufferpool, LINEORDER_PK_SORT, signatures, TEST_DATA_FOLDER, "lineorder.bin");
  FReadOnlyCStore customer (&bufferpool, CUSTOMER_PK_SORT, signatures, TEST_DATA_FOLDER, "customer.bin");

  PositionRange range (0, 10);
  vector<PositionRange> ranges;
  ranges.push_back (range);

  vector<PositionRange> cranges;
  cranges.push_back (PositionRange (0, 6));

  {
    FColumnReader *reader = lineorder.getColumnReader("orderpriority"); //4bit
    BOOST_CHECK (reader != NULL);
    BOOST_CHECK_EQUAL (reader->getColumn().name, "orderpriority");
    reader->setSearchRanges(ranges);
    vector<boost::shared_ptr<PositionBitmap> > ret;
    string key = reader->normalize("2-HIGH");
    reader->getPositionBitmaps(SearchCond(SCT_EQUAL, key.data()), ret);
    BOOST_CHECK_EQUAL (ret.size(), 1);
    PositionBitmap *bitmap = ret[0].get();
    BOOST_CHECK (bitmap != NULL);
    BOOST_CHECK_EQUAL (bitmap->bitLength, 10);
    BOOST_CHECK_EQUAL (bitmap->byteLength, 2);
    BOOST_CHECK_EQUAL (bitmap->bitmap[0], 0x7f);
    BOOST_CHECK_EQUAL (bitmap->bitmap[1], 0);
    BOOST_CHECK_EQUAL (bitmap->matchedCount, 7);

    ret.clear();
    reader->getPositionBitmaps(SearchCond(SCT_LT, key.data()), ret);
    BOOST_CHECK_EQUAL (ret.size(), 1);
    bitmap = ret[0].get();
    BOOST_CHECK (bitmap != NULL);
    BOOST_CHECK_EQUAL (bitmap->bitLength, 10);
    BOOST_CHECK_EQUAL (bitmap->byteLength, 2);
    BOOST_CHECK_EQUAL (bitmap->bitmap[0], 0x80);
    BOOST_CHECK_EQUAL (bitmap->bitmap[1], 0x3);
    BOOST_CHECK_EQUAL (bitmap->matchedCount, 3);
  }

  {
    FColumnReader *reader = lineorder.getColumnReader("shippriority"); //1bit
    BOOST_CHECK (reader != NULL);
    BOOST_CHECK_EQUAL (reader->getColumn().name, "shippriority");
    reader->setSearchRanges(ranges);
    vector<boost::shared_ptr<PositionBitmap> > ret;
    string key = reader->normalize("0");
    reader->getPositionBitmaps(SearchCond(SCT_EQUAL, key.data()), ret);
    BOOST_CHECK_EQUAL (ret.size(), 1);
    PositionBitmap *bitmap = ret[0].get();
    BOOST_CHECK (bitmap != NULL);
    BOOST_CHECK_EQUAL (bitmap->bitLength, 10);
    BOOST_CHECK_EQUAL (bitmap->byteLength, 2);
    BOOST_CHECK_EQUAL (bitmap->bitmap[0], 0xff);
    BOOST_CHECK_EQUAL (bitmap->bitmap[1], 0x03);
    BOOST_CHECK_EQUAL (bitmap->matchedCount, 10);

    ret.clear();
    key = reader->normalize("1");
    reader->getPositionBitmaps(SearchCond(SCT_EQUAL, key.data()), ret);
    BOOST_CHECK_EQUAL (ret.size(), 1);
    bitmap = ret[0].get();
    BOOST_CHECK (bitmap != NULL);
    BOOST_CHECK_EQUAL (bitmap->bitLength, 10);
    BOOST_CHECK_EQUAL (bitmap->byteLength, 2);
    BOOST_CHECK_EQUAL (bitmap->bitmap[0], 0);
    BOOST_CHECK_EQUAL (bitmap->bitmap[1], 0);
    BOOST_CHECK_EQUAL (bitmap->matchedCount, 0);
  }

  {
    FColumnReader *reader = customer.getColumnReader("nation"); //8bit
    BOOST_CHECK (reader != NULL);
    BOOST_CHECK_EQUAL (reader->getColumn().name, "nation");
    reader->setSearchRanges(cranges);
    vector<boost::shared_ptr<PositionBitmap> > ret;
    string china = reader->normalize("CHINA");
    string egypt = reader->normalize("EGYPT");
    string jordan = reader->normalize("JORDAN");
    reader->getPositionBitmaps(SearchCond(SCT_EQUAL, egypt.data()), ret);
    BOOST_CHECK_EQUAL (ret.size(), 1);
    PositionBitmap *bitmap = ret[0].get();
    BOOST_CHECK (bitmap != NULL);
    BOOST_CHECK_EQUAL (bitmap->bitLength, 6);
    BOOST_CHECK_EQUAL (bitmap->byteLength, 1);
    BOOST_CHECK_EQUAL (bitmap->bitmap[0], 0x8);
    BOOST_CHECK_EQUAL (bitmap->matchedCount, 1);

    ret.clear();
    vector<const void*> keys;
    keys.push_back (china.data());
    keys.push_back (egypt.data());
    reader->getPositionBitmaps(SearchCond(keys), ret);
    BOOST_CHECK_EQUAL (ret.size(), 1);
    bitmap = ret[0].get();
    BOOST_CHECK (bitmap != NULL);
    BOOST_CHECK_EQUAL (bitmap->bitLength, 6);
    BOOST_CHECK_EQUAL (bitmap->byteLength, 1);
    BOOST_CHECK_EQUAL (bitmap->bitmap[0], 0x0a);
    BOOST_CHECK_EQUAL (bitmap->matchedCount, 2);

    ret.clear();
    reader->getPositionBitmaps(SearchCond(egypt.data(), jordan.data()), ret);
    BOOST_CHECK_EQUAL (ret.size(), 1);
    bitmap = ret[0].get();
    BOOST_CHECK (bitmap != NULL);
    BOOST_CHECK_EQUAL (bitmap->bitLength, 6);
    BOOST_CHECK_EQUAL (bitmap->byteLength, 1);
    BOOST_CHECK_EQUAL (bitmap->bitmap[0], 0x38);
    BOOST_CHECK_EQUAL (bitmap->matchedCount, 3);
  }

  {
    FColumnReader *reader = customer.getColumnReader("city"); //16bit
    BOOST_CHECK (reader != NULL);
    BOOST_CHECK_EQUAL (reader->getColumn().name, "city");
    reader->setSearchRanges(cranges);
    vector<boost::shared_ptr<PositionBitmap> > ret;
    string edypt = reader->normalize("EGYPT    6");
    reader->getPositionBitmaps(SearchCond(SCT_EQUAL, edypt.data()), ret);
    BOOST_CHECK_EQUAL (ret.size(), 1);
    PositionBitmap *bitmap = ret[0].get();
    BOOST_CHECK (bitmap != NULL);
    BOOST_CHECK_EQUAL (bitmap->bitLength, 6);
    BOOST_CHECK_EQUAL (bitmap->byteLength, 1);
    BOOST_CHECK_EQUAL (bitmap->bitmap[0], 0x08);
    BOOST_CHECK_EQUAL (bitmap->matchedCount, 1);

    ret.clear();
    string us = reader->normalize("UNITED ST8");
    reader->getPositionBitmaps(SearchCond(SCT_LT, us.data()), ret);
    BOOST_CHECK_EQUAL (ret.size(), 1);
    bitmap = ret[0].get();
    BOOST_CHECK (bitmap != NULL);
    BOOST_CHECK_EQUAL (bitmap->bitLength, 6);
    BOOST_CHECK_EQUAL (bitmap->byteLength, 1);
    BOOST_CHECK_EQUAL (bitmap->bitmap[0], 0x3a);
    BOOST_CHECK_EQUAL (bitmap->matchedCount, 4);
  }
  BOOST_TEST_MESSAGE("===Tested dictionary compressed CStore column.");
}
BOOST_AUTO_TEST_CASE(storage_cstore_rle) {
  BOOST_TEST_MESSAGE("===Testing RLE CStore column...");
  FSignatureSet signatures;
  signatures.load (TEST_DATA_FOLDER, "_tinyssb.sig");
  BOOST_REQUIRE (signatures.size() > 0);
  FBufferPool bufferpool (100);

  FReadOnlyCStore lineorder (&bufferpool, LINEORDER_PK_SORT, signatures, TEST_DATA_FOLDER, "lineorder.bin");
  for (int rep = 0; rep < 2; ++rep) {
    FColumnReader *reader = lineorder.getColumnReader("orderkey");
    BOOST_CHECK (reader != NULL);
    BOOST_CHECK_EQUAL (reader->getColumn().name, "orderkey");
    if (rep == 1) {
      PositionRange range (0, 10);
      vector<PositionRange> ranges;
      ranges.push_back (range);
      reader->setSearchRanges(ranges);
      BOOST_TEST_MESSAGE("-- Testing with setting search range..");
    } else {
      BOOST_TEST_MESSAGE("-- Testing without setting search range..");
    }

    {
      vector<PositionRange> ret;
      int32_t key = 2;
      reader->getPositionRanges(SearchCond(SCT_EQUAL, &key), ret);
      BOOST_CHECK_EQUAL (ret.size(), 1);
      const PositionRange &range = ret[0];
      BOOST_CHECK_EQUAL (range.begin, 3);
      BOOST_CHECK_EQUAL (range.end, 7);
    }

    {
      vector<PositionRange> ret;
      int32_t key = 3;
      reader->getPositionRanges(SearchCond(SCT_LT, &key), ret);
      BOOST_CHECK_EQUAL (ret.size(), 1);
      const PositionRange &range = ret[0];
      BOOST_CHECK_EQUAL (range.begin, 0);
      BOOST_CHECK_EQUAL (range.end, 7);
    }


    {
      vector<PositionRange> ret;
      int32_t key = 1, key2 = 3;
      vector<const void *> keys;
      keys.push_back (&key);
      keys.push_back (&key2);
      ret.clear();
      reader->getPositionRanges(SearchCond(keys), ret);
      BOOST_CHECK_EQUAL (ret.size(), 2);
      BOOST_CHECK_EQUAL (ret[0].begin, 0);
      BOOST_CHECK_EQUAL (ret[0].end, 3);
      BOOST_CHECK_EQUAL (ret[1].begin, 7);
      if (rep == 1) {
        BOOST_CHECK_EQUAL (ret[1].end, 10);
      } else {
        BOOST_CHECK_EQUAL (ret[1].end, 12);
      }
    }
  }

  PositionRange range (2, 6);
  vector<PositionRange> ranges;
  ranges.push_back (range);
  FColumnReader *reader = lineorder.getColumnReader("orderkey");
  reader->setSearchRanges(ranges);
  vector<PositionRange> ret;
  int32_t key = 2;
  reader->getPositionRanges(SearchCond(SCT_EQUAL, &key), ret);
  BOOST_CHECK_EQUAL (ret.size(), 1);
  BOOST_CHECK_EQUAL (ret[0].begin, 3);
  BOOST_CHECK_EQUAL (ret[0].end, 6);

  BOOST_TEST_MESSAGE("===Tested RLE CStore column.");
}

struct TestTupleSearchContext {
  int searchingFrom; // -1 if no restriction
  int searchingTo; // -1 if no restriction
  int readTuples;
  int previousKey;
};
TupleCallbackRet testTupleCallback (void *context, const void *tuple) {
  const Lineorder *l = reinterpret_cast<const Lineorder*> (tuple);
  BOOST_CHECK_EQUAL (l->linenumber, l->orderkey / 100);
  BOOST_CHECK_EQUAL (l->revenue, 2032150);
  BOOST_CHECK_EQUAL (l->commitdate, 19950331);
  BOOST_CHECK_EQUAL (l->shipmode, "TRUCK");

  TestTupleSearchContext *c = reinterpret_cast<TestTupleSearchContext*> (context);
  if (c->searchingTo >= 0) {
    if (l->orderkey >= c->searchingTo) {
      return TUPLE_CALLBACK_QUIT;
    }
  }
  if (c->searchingFrom >= 0) {
    BOOST_CHECK (l->orderkey >= c->searchingFrom);
  }

  ++(c->readTuples);
  BOOST_CHECK_EQUAL (c->previousKey, l->orderkey - 1);
  c->previousKey = l->orderkey;

  return TUPLE_CALLBACK_OK;
}

BOOST_AUTO_TEST_CASE(storage_test_readonly_btree) {
  BOOST_TEST_MESSAGE("===Testing ReadOnlyBTree...");
  std::remove((TEST_DATA_FOLDER + string("_test4.sig")).c_str());

  FSignatureSet signatureFile;
  BOOST_TEST_MESSAGE("--making a somewhat large btree...");
  const int TUP_COUNT = 10000;
  FMainMemoryBTree btree (LINEORDER_PK_SORT, TUP_COUNT, false);
  {
    Lineorder l;
    l.loadDataPiped("1|1|25738|155190|4137|19950218|2-HIGH|0|17|2116823|7730149|4|2032150|74711|2|19950331|TRUCK");
    for (int i = 0; i < TUP_COUNT; ++i) {
      // changes only PK
      l.orderkey = i;
      l.linenumber = i / 100;
      Lineorder::PKType pk = l.getPK();
      btree.insert(&pk, &l);
    }
    BOOST_CHECK_EQUAL (btree.size(), TUP_COUNT);
  }
  btree.finishInserts();

  BOOST_TEST_MESSAGE("--dumping to disk...");
  FFileSignature signature = signatureFile.dumpToNewRowStoreFile(TEST_DATA_FOLDER, "test4.db", btree);
  signatureFile.save(TEST_DATA_FOLDER, "_test4.sig");

  BOOST_REQUIRE (signature.pageCount > 10);

  BOOST_TEST_MESSAGE("--reading from disk...");
  {
    FBufferPool pool (100);
    for (int i = 0; i < signature.pageCount; i += 2) {
      const char *page = pool.readPage(signature, i);
      BOOST_CHECK (page != NULL);
    }
    for (int i = 0; i < signature.pageCount; i += 3) {
      const char *page = pool.readPage(signature, i);
      BOOST_CHECK (page != NULL);
    }
    pool.readPages(signature, 0, 10);
    pool.clear();
  }
  BOOST_TEST_MESSAGE("--testing get methods...");
  {
    FBufferPool pool (100);
    FReadOnlyDiskBTree btree (&pool, signature);
    BOOST_TEST_MESSAGE("----testing getSingleTupleByKey...");
    for (int i = 1; i < TUP_COUNT / 2; i *= 2) {
      Lineorder l;
      l.orderkey = i;
      l.linenumber = i / 100;
      Lineorder::PKType key = l.getPK();
      const char *tuple = btree.getSingleTupleByKey(reinterpret_cast<char*>(&key));
      BOOST_CHECK (tuple != NULL);
      const Lineorder *l2 = reinterpret_cast<const Lineorder*> (tuple);
      BOOST_CHECK_EQUAL (l2->orderkey, i);
      BOOST_CHECK_EQUAL (l2->linenumber, i / 100);
      BOOST_CHECK_EQUAL (l2->revenue, 2032150);
      BOOST_CHECK_EQUAL (l2->commitdate, 19950331);
      BOOST_CHECK_EQUAL (l2->shipmode, "TRUCK");
    }
    for (int i = TUP_COUNT + 1; i < TUP_COUNT + 20; ++i) {
      Lineorder l;
      l.orderkey = i;
      l.linenumber = i / 100;
      Lineorder::PKType key = l.getPK();
      const char *tuple = btree.getSingleTupleByKey(reinterpret_cast<char*>(&key));
      BOOST_CHECK (tuple == NULL);
    }
    pool.clear();
    BOOST_TEST_MESSAGE("----testing scanAllTuples...");
    TestTupleSearchContext context;
    context.readTuples = 0;
    context.previousKey = -1;
    context.searchingFrom = -1;
    context.searchingTo = -1;
    btree.scanAllTuples(testTupleCallback, &context);
    BOOST_CHECK_EQUAL (context.readTuples, TUP_COUNT);

    pool.clear();
    context.readTuples = 0;
    context.previousKey = -1;
    context.searchingFrom = -1;
    context.searchingTo = 200;
    btree.scanAllTuples(testTupleCallback, &context);
    BOOST_CHECK_EQUAL (context.readTuples, 200);

    BOOST_TEST_MESSAGE("----testing scanTuplesGreaterEqual...");
    pool.clear();
    context.readTuples = 0;
    context.previousKey = 499;
    context.searchingFrom = 500;
    context.searchingTo = -1;
    Lineorder dummy;
    dummy.orderkey = 500;
    dummy.linenumber = 500 / 100;
    Lineorder::PKType key = dummy.getPK();
    btree.scanTuplesGreaterEqual(testTupleCallback, &context, reinterpret_cast<char*>(&key));
    BOOST_CHECK_EQUAL (context.readTuples, TUP_COUNT - 500);

    pool.clear();
    context.readTuples = 0;
    context.previousKey = 2499;
    context.searchingFrom = 2500;
    context.searchingTo = 3000;
    dummy.orderkey = 2500;
    dummy.linenumber = 2500 / 100;
    key = dummy.getPK();
    btree.scanTuplesGreaterEqual(testTupleCallback, &context, reinterpret_cast<char*>(&key));
    BOOST_CHECK_EQUAL (context.readTuples, 500);
  }

  BOOST_TEST_MESSAGE("===Tested ReadOnlyBTree.");
}


BOOST_AUTO_TEST_CASE(util_test_hashmap) {
  BOOST_TEST_MESSAGE("===Testing StringHashSet...");
  string strs[200];
  for (int i = 0;i < 200; ++i) {
    stringstream s;
    s << "somestr" << setfill('0') << setw(4) << i;
    strs[i] = s.str();
    BOOST_CHECK_EQUAL (strs[i].size(), 11);
  }
  for (int hashbits = 1; hashbits < 17; ++hashbits) {
    StringHashSet hashset (11, hashbits);
    BOOST_TEST_MESSAGE("hashtable with hashbits=" << hashbits << " .");
    for (int i = 0; i < 200; ++i) {
      BOOST_CHECK (hashset.find(strs[i].data()) == NULL);
      hashset.insert(strs[i].data());
      BOOST_CHECK (hashset.find(strs[i].data()) == strs[i].data());
    }
  }
  BOOST_TEST_MESSAGE("===Tested StringHashSet.");
}


bool compareByMem(int64_t v1, int64_t v2) {
  return (::memcmp (&v1, &v2, sizeof(int64_t)) < 0);
}
BOOST_AUTO_TEST_CASE(util_test_alignment) {
  BOOST_TEST_MESSAGE("===Testing alignment in this environment...");
  Lineorder l[4];
  l[0].orderkey = 0x12345677;
  l[0].linenumber = 9;
  l[1].orderkey = 0x77654321;
  l[1].linenumber = 1;
  l[2].orderkey = 0x12345677;
  l[2].linenumber = 0;
  l[3].orderkey = 0x77654321;
  l[3].linenumber = 10;
  int64_t k[4];
  const unsigned char *c[4];
  for (int i = 0; i < 4; ++i) {
    k[i] = l[i].getPK();
    c[i] = reinterpret_cast<const unsigned char*>(&k[i]);
  }
  for (int i = 0; i < 4; ++i) {
    for (int j = i + 1; j < 4; ++j) {
      int ret = ::memcmp (c[i], c[j], sizeof(int64_t));
      BOOST_CHECK (ret != 0);
      BOOST_TEST_MESSAGE("memcmp(" << i << "," << j << ")=" << ret);
    }
  }
  if (::memcmp (c[0], c[1], sizeof(int64_t)) < 0) {
    BOOST_TEST_MESSAGE("Seems like Big Endian!");
  } else {
    BOOST_TEST_MESSAGE("Seems like Little Endian!");
  }
  BOOST_TEST_MESSAGE("Before sorting:");
  for (int i = 0; i < 4; ++i) BOOST_TEST_MESSAGE("k[" << i << "]=0x" << hex << k[i] << "(=" << dec << k[i] << ")");
  sort (k, k + 4);
  BOOST_TEST_MESSAGE("After sorting as int64_t:");
  for (int i = 0; i < 4; ++i) BOOST_TEST_MESSAGE("k[" << i << "]=0x" << hex << k[i] << "(=" << dec << k[i] << ")");
  struct TestFunctor : public std::binary_function<int64_t,int64_t,bool> {
  public:
    const bool operator()(int64_t v1, int64_t v2) const {
      return (::memcmp (&v1, &v2, sizeof(int64_t)) < 0);
    }
  };
  std::sort (k + 0, k + 4, compareByMem);
  BOOST_TEST_MESSAGE("After sorting by memcmp:");
  for (int i = 0; i < 4; ++i) BOOST_TEST_MESSAGE("k[" << i << "]=0x" << hex << k[i] << "(=" << dec << k[i] << ")");
  BOOST_TEST_MESSAGE("===Tested alignment.");
}

BOOST_AUTO_TEST_CASE(ssb_query) {
  BOOST_TEST_MESSAGE("===Testing SSB Queries...");
  FEngine engine (TEST_DATA_FOLDER, string(TEST_DATA_FOLDER) + "_tinyssb.sig", 100);
  SSBQueryExecutor exec(&engine);

  // to add a few additional data
  FFamily *family[2];
  family[0] = engine.createNewFractureFamily(CSTORE_MV_FAMILY, MV_PROJECTION, true);
  family[1] = engine.createNewFractureFamily(BTREE_MV_FAMILY, MV_PROJECTION, false);
  FMainMemoryBTree fractureSorted (MV_PROJECTION, 10, true);
  FMainMemoryBTree fractureUnsorted (MV_PROJECTION, 10, false);
  const char* REGIONS[] = {"AFRICA", "MIDDLE EAST", "ASIA", "ASIA", "MIDDLE EAST"};
  const char* CATEGORIES[] = {"MFGR#12", "MFGR#15", "MFGR#12", "MFGR#22", "MFGR#25"};
  const char* BRANDS[] = {"MFGR#3330", "MFGR#220", "MFGR#225", "MFGR#320", "MFGR#3332"};
  int REV[10], PRI[10], DISC[10];
  for (int i = 0; i < 10; ++i) {
    MVProjection m;
    ::memset (&m, 0, sizeof(MVProjection));
    m.key.l_orderkey = i + 10000;
    ::memcpy (m.key.s_region, REGIONS[i % 5], ::strlen(REGIONS[i % 5]));
    m.key.d_year = 1993 + (i % 7);
    m.key.d_yearmonthnum = m.key.d_year * 100 + 12;
    m.l_quantity = i * 5;
    m.l_discount = i;
    m.l_extendedprice = 10 + i;
    m.l_revenue = 20 + i;
    m.l_supplycost = 50 + i;
    m.d_weeknuminyear = 5;
    ::memcpy (m.p_category, CATEGORIES[i % 5], ::strlen(CATEGORIES[i % 5]));
    ::memcpy (m.p_brand, BRANDS[i % 5], ::strlen(BRANDS[i % 5]));
    REV[i] = m.l_revenue;
    PRI[i] = m.l_extendedprice;
    DISC[i] = m.l_discount;
    fractureSorted.insert(&(m.key), &m);
    fractureUnsorted.insert(&(m.key), &m);
  }

  {
    BOOST_TEST_MESSAGE("- Q1.1");
    for (int i = 0; i < 2; ++i) {
      BOOST_TEST_MESSAGE("-- " << (i == 0 ? "cstore" : "rstore") << "");
      family[i]->setCurrentFracture(NULL);
      SSBQueryParam param;
      param.ints.push_back (1996);
      param.ints.push_back (1);
      param.ints.push_back (5);
      param.ints.push_back (30);
      boost::shared_ptr<SSBQueryResult> res = exec.query(11, i == 0, param);
      BOOST_TEST_MESSAGE("  result:" << res->toString());
      BOOST_CHECK_EQUAL (res->singleIntResult, 13456484);

      for (int sorted = 0; sorted < 2; ++sorted) {
        family[i]->setCurrentFracture(sorted == 0 ? &fractureUnsorted : &fractureSorted);
        res = exec.query(11, i == 0, param);
        BOOST_TEST_MESSAGE("  result:" << res->toString());
        BOOST_CHECK_EQUAL (res->singleIntResult, 13456484 + (DISC[3] * PRI[3])); // i=3 hits
      }
    }
  }

  {
    BOOST_TEST_MESSAGE("- Q1.2");
    for (int i = 0; i < 2; ++i) {
      BOOST_TEST_MESSAGE("-- " << (i == 0 ? "cstore" : "rstore") << "");
      family[i]->setCurrentFracture(NULL);
      SSBQueryParam param;
      param.ints.push_back (199312);
      param.ints.push_back (7);
      param.ints.push_back (10);
      param.ints.push_back (30);
      param.ints.push_back (50);
      boost::shared_ptr<SSBQueryResult> res = exec.query(12, i == 0, param);
      BOOST_TEST_MESSAGE("  result:" << res->toString());
      BOOST_CHECK_EQUAL (res->singleIntResult, 126913294);

      for (int sorted = 0; sorted < 2; ++sorted) {
        family[i]->setCurrentFracture(sorted == 0 ? &fractureUnsorted : &fractureSorted);
        res = exec.query(12, i == 0, param);
        BOOST_TEST_MESSAGE("  result:" << res->toString());
        BOOST_CHECK_EQUAL (res->singleIntResult, 126913294 + DISC[7] * PRI[7]); // i=7 hits 199312
      }
    }
  }

  {
    BOOST_TEST_MESSAGE("- Q1.3");
    for (int i = 0; i < 2; ++i) {
      BOOST_TEST_MESSAGE("-- " << (i == 0 ? "cstore" : "rstore") << "");
      family[i]->setCurrentFracture(NULL);
      SSBQueryParam param;
      param.ints.push_back (1996);
      param.ints.push_back (5);
      param.ints.push_back (0);
      param.ints.push_back (10);
      param.ints.push_back (30);
      param.ints.push_back (40);
      boost::shared_ptr<SSBQueryResult> res = exec.query(13, i == 0, param);
      BOOST_TEST_MESSAGE("  result:" << res->toString());
      BOOST_CHECK_EQUAL (res->singleIntResult, 82287540);

      for (int sorted = 0; sorted < 2; ++sorted) {
        family[i]->setCurrentFracture(sorted == 0 ? &fractureUnsorted : &fractureSorted);
        res = exec.query(13, i == 0, param);
        BOOST_TEST_MESSAGE("  result:" << res->toString());
        BOOST_CHECK_EQUAL (res->singleIntResult, 82287540 + (DISC[3] * PRI[3])); // i=3 hits
      }
    }
  }

  {
    BOOST_TEST_MESSAGE("- Q2.1");

    // L3-3 rev=3749742 , P-128449 : cat=#12, brand=#1239, S7045-ASIA, D1996
    // L3-4 rev=259257, P-29380 : cat=#12, brand=#123, S3709-EUROPE, D1996
    // L3-5 rev=3166705, P-183095 : cat=#12, brand=#127, S3709-EUROPE, D1996
    MVProjection mv;
    char p_brand1[sizeof(mv.p_brand)], p_brand2[sizeof(mv.p_brand)], p_brand3[sizeof(mv.p_brand)], p_brand4[sizeof(mv.p_brand)];
    ::memset (p_brand1, 0, sizeof(mv.p_brand));
    ::memcpy (p_brand1, "MFGR#1239", ::strlen("MFGR#1239"));
    ::memset (p_brand2, 0, sizeof(mv.p_brand));
    ::memcpy (p_brand2, "MFGR#123", ::strlen("MFGR#123"));
    ::memset (p_brand3, 0, sizeof(mv.p_brand));
    ::memcpy (p_brand3, "MFGR#127", ::strlen("MFGR#127"));
    ::memset (p_brand4, 0, sizeof(mv.p_brand));
    ::memcpy (p_brand4, "MFGR#225", ::strlen("MFGR#225"));

    for (int i = 0; i < 2; ++i) {
      BOOST_TEST_MESSAGE("-- " << (i == 0 ? "cstore" : "rstore") << "");
      family[i]->setCurrentFracture(NULL);
      SSBQueryParam param;
      param.strings.push_back ("MFGR#12");
      param.strings.push_back ("EUROPE");
      boost::shared_ptr<SSBQueryResult> res = exec.query(21, i == 0, param);
      BOOST_TEST_MESSAGE("  result:" << res->toString());
      BOOST_CHECK_EQUAL (res->groupedResults.size(), 2);
      for (SSBQueryResult::ResultMapIter it = res->groupedResults.begin(); it != res->groupedResults.end(); ++it) {
        const SSBQueryResult::ResultKey &key = it->first;
        int64_t sum = it->second;
        BOOST_CHECK_EQUAL (key.size(), 2);
        int16_t year = *reinterpret_cast<const int16_t*>(key[0].data());
        BOOST_CHECK_EQUAL (year, 1996);
        string brand = key[1];
        if (::memcmp(brand.data(), p_brand2, sizeof(mv.p_brand)) == 0) {
          BOOST_CHECK_EQUAL (sum, 259257);
        } else {
          BOOST_CHECK_EQUAL(::memcmp(brand.data(), p_brand3, sizeof(mv.p_brand)), 0);
          BOOST_CHECK_EQUAL (sum, 3166705);
        }
      }

      param.strings.clear();
      param.strings.push_back ("MFGR#12");
      param.strings.push_back ("ASIA");
      {
        res = exec.query(21, i == 0, param);
        BOOST_TEST_MESSAGE("  result:" << res->toString());
        BOOST_CHECK_EQUAL (res->groupedResults.size(), 1);
        SSBQueryResult::ResultMapIter it = res->groupedResults.begin();
        BOOST_REQUIRE (it != res->groupedResults.end());
        const SSBQueryResult::ResultKey &key = it->first;
        int64_t sum = it->second;
        BOOST_CHECK_EQUAL (key.size(), 2);
        int16_t year = *reinterpret_cast<const int16_t*>(key[0].data());
        BOOST_CHECK_EQUAL (year, 1996);
        string brand = key[1];
        BOOST_CHECK_EQUAL(::memcmp(brand.data(), p_brand1, sizeof(mv.p_brand)), 0);
        BOOST_CHECK_EQUAL (sum, 3749742);
      }

      for (int sorted = 0; sorted < 2; ++sorted) {
        family[i]->setCurrentFracture(sorted == 0 ? &fractureUnsorted : &fractureSorted);
        res = exec.query(21, i == 0, param);
        BOOST_TEST_MESSAGE("  result:" << res->toString());
        BOOST_CHECK_EQUAL (res->groupedResults.size(), 3);
        SSBQueryResult::ResultMapIter it = res->groupedResults.begin();
        // i=2,i=7 hit
        int correctYear[3] = {1993 + (7 % 7), 1993 + (2 % 7), 1996};
        int correctSum[3] = {REV[7], REV[2], 3749742};
        const char* correctBrand[3] = {BRANDS[7 % 5], BRANDS[2 % 5], p_brand1};
        for (int j = 0; j < 3; ++j) {
          BOOST_REQUIRE (it != res->groupedResults.end());
          const SSBQueryResult::ResultKey &key = it->first;
          int64_t sum = it->second;
          BOOST_CHECK_EQUAL (key.size(), 2);
          int16_t year = *reinterpret_cast<const int16_t*>(key[0].data());
          BOOST_CHECK_EQUAL (year, correctYear[j]);
          string brand = key[1];
          BOOST_CHECK_EQUAL(::memcmp(brand.data(), correctBrand[j], sizeof(mv.p_brand)), 0);
          BOOST_CHECK_EQUAL (sum, correctSum[j]);
          ++it;
        }
      }
    }
  }

  {
    BOOST_TEST_MESSAGE("- Q2.2");

    // L5-2 rev=4717324, P-123927: brand=#3332, S1295-MID, D1997
    // L6-4 rev=2925538, P-172758: brand=#3327, S9632-AFR, D1993
    MVProjection mv;
    char p_brand1[sizeof(mv.p_brand)], p_brand2[sizeof(mv.p_brand)];
    ::memset (p_brand1, 0, sizeof(mv.p_brand));
    ::memcpy (p_brand1, "MFGR#3332", ::strlen("MFGR#3332"));
    ::memset (p_brand2, 0, sizeof(mv.p_brand));
    ::memcpy (p_brand2, "MFGR#3327", ::strlen("MFGR#3327"));

    for (int i = 0; i < 2; ++i) {
      BOOST_TEST_MESSAGE("-- " << (i == 0 ? "cstore" : "rstore") << "");
      family[i]->setCurrentFracture(NULL);
      SSBQueryParam param;
      param.strings.push_back ("MFGR#3325");
      param.strings.push_back ("MFGR#3332");
      param.strings.push_back ("MIDDLE EAST");
      {
        boost::shared_ptr<SSBQueryResult> res = exec.query(22, i == 0, param);
        BOOST_TEST_MESSAGE("  result:" << res->toString());
        BOOST_CHECK_EQUAL (res->groupedResults.size(), 1);
        SSBQueryResult::ResultMapIter it = res->groupedResults.begin();
        BOOST_REQUIRE (it != res->groupedResults.end());
        const SSBQueryResult::ResultKey &key = it->first;
        int64_t sum = it->second;
        BOOST_CHECK_EQUAL (key.size(), 2);
        int16_t year = *reinterpret_cast<const int16_t*>(key[0].data());
        BOOST_CHECK_EQUAL (year, 1997);
        string brand = key[1];
        BOOST_CHECK_EQUAL(::memcmp(brand.data(), p_brand1, sizeof(mv.p_brand)), 0);
        BOOST_CHECK_EQUAL (sum, 4717324);
      }

      param.strings.clear();
      param.strings.push_back ("MFGR#3325");
      param.strings.push_back ("MFGR#3332");
      param.strings.push_back ("AFRICA");
      {
        boost::shared_ptr<SSBQueryResult> res = exec.query(22, i == 0, param);
        BOOST_TEST_MESSAGE("  result:" << res->toString());
        BOOST_CHECK_EQUAL (res->groupedResults.size(), 1);
        SSBQueryResult::ResultMapIter it = res->groupedResults.begin();
        BOOST_REQUIRE (it != res->groupedResults.end());
        const SSBQueryResult::ResultKey &key = it->first;
        int64_t sum = it->second;
        BOOST_CHECK_EQUAL (key.size(), 2);
        int16_t year = *reinterpret_cast<const int16_t*>(key[0].data());
        BOOST_CHECK_EQUAL (year, 1993);
        string brand = key[1];
        BOOST_CHECK_EQUAL(::memcmp(brand.data(), p_brand2, sizeof(mv.p_brand)), 0);
        BOOST_CHECK_EQUAL (sum, 2925538);
      }

      for (int sorted = 0; sorted < 2; ++sorted) {
        family[i]->setCurrentFracture(sorted == 0 ? &fractureUnsorted : &fractureSorted);
        boost::shared_ptr<SSBQueryResult> res = exec.query(22, i == 0, param);
        BOOST_TEST_MESSAGE("  result:" << res->toString());
        BOOST_CHECK_EQUAL (res->groupedResults.size(), 3);
        SSBQueryResult::ResultMapIter it = res->groupedResults.begin();
        // i=0,i=5 hit
        int correctYear[3] = {1993, 1993 + (0 % 7), 1993 + (5 % 7)};
        int correctSum[3] = {2925538, REV[0], REV[5]};
        const char* correctBrand[3] = {p_brand2, BRANDS[0 % 5], BRANDS[5 % 5]};
        for (int j = 0; j < 3; ++j) {
          BOOST_REQUIRE (it != res->groupedResults.end());
          const SSBQueryResult::ResultKey &key = it->first;
          int64_t sum = it->second;
          BOOST_CHECK_EQUAL (key.size(), 2);
          int16_t year = *reinterpret_cast<const int16_t*>(key[0].data());
          BOOST_CHECK_EQUAL (year, correctYear[j]);
          string brand = key[1];
          BOOST_CHECK_EQUAL(::memcmp(brand.data(), correctBrand[j], sizeof(mv.p_brand)), 0);
          BOOST_CHECK_EQUAL (sum, correctSum[j]);
          ++it;
        }
      }
    }
  }

  {
    BOOST_TEST_MESSAGE("- Q2.3");

    // L3-3 rev=3749742 , P-128449 : brand=#1239, S7045-ASIA, D1996
    // L5-2 rev=4717324, P-123927: brand=#3332, S1295-MID, D1997
    MVProjection mv;

    for (int i = 0; i < 2; ++i) {
      BOOST_TEST_MESSAGE("-- " << (i == 0 ? "cstore" : "rstore") << "");
      family[i]->setCurrentFracture(NULL);
      SSBQueryParam param;
      param.strings.push_back ("MFGR#1239");
      param.strings.push_back ("ASIA");
      {
        boost::shared_ptr<SSBQueryResult> res = exec.query(23, i == 0, param);
        BOOST_TEST_MESSAGE("  result:" << res->toString());
        BOOST_CHECK_EQUAL (res->groupedResults.size(), 1);
        SSBQueryResult::ResultMapIter it = res->groupedResults.begin();
        BOOST_REQUIRE (it != res->groupedResults.end());
        const SSBQueryResult::ResultKey &key = it->first;
        int64_t sum = it->second;
        BOOST_CHECK_EQUAL (key.size(), 1);
        int16_t year = *reinterpret_cast<const int16_t*>(key[0].data());
        BOOST_CHECK_EQUAL (year, 1996);
        BOOST_CHECK_EQUAL (sum, 3749742);
      }

      param.strings.clear();
      param.strings.push_back ("MFGR#3332");
      param.strings.push_back ("MIDDLE EAST");
      {
        boost::shared_ptr<SSBQueryResult> res = exec.query(23, i == 0, param);
        BOOST_TEST_MESSAGE("  result:" << res->toString());
        BOOST_CHECK_EQUAL (res->groupedResults.size(), 1);
        SSBQueryResult::ResultMapIter it = res->groupedResults.begin();
        BOOST_REQUIRE (it != res->groupedResults.end());
        const SSBQueryResult::ResultKey &key = it->first;
        int64_t sum = it->second;
        BOOST_CHECK_EQUAL (key.size(), 1);
        int16_t year = *reinterpret_cast<const int16_t*>(key[0].data());
        BOOST_CHECK_EQUAL (year, 1997);
        BOOST_CHECK_EQUAL (sum, 4717324);
      }

      for (int sorted = 0; sorted < 2; ++sorted) {
        family[i]->setCurrentFracture(sorted == 0 ? &fractureUnsorted : &fractureSorted);
        boost::shared_ptr<SSBQueryResult> res = exec.query(23, i == 0, param);
        BOOST_TEST_MESSAGE("  result:" << res->toString());
        BOOST_CHECK_EQUAL (res->groupedResults.size(), 2);
        SSBQueryResult::ResultMapIter it = res->groupedResults.begin();
        // i=4(y=1997,add up to existing group),i=9 (y=1995,new group) hit
        int correctYear[2] = {1993 + (9 % 7), 1997};
        int correctSum[2] = {REV[9], 4717324 + REV[4]};
        for (int j = 0; j < 2; ++j) {
          BOOST_REQUIRE (it != res->groupedResults.end());
          const SSBQueryResult::ResultKey &key = it->first;
          int64_t sum = it->second;
          BOOST_CHECK_EQUAL (key.size(), 1);
          int16_t year = *reinterpret_cast<const int16_t*>(key[0].data());
          BOOST_CHECK_EQUAL (year, correctYear[j]);
          BOOST_CHECK_EQUAL (sum, correctSum[j]);
          ++it;
        }
      }
    }
  }
  BOOST_TEST_MESSAGE("===Tested SSB Queries.");
}

BOOST_AUTO_TEST_CASE(ssb_dbgen) {
  BOOST_TEST_MESSAGE("===Testing SSB customized dbgen...");
  {
    DBGen gen ("../../data/tinyssb/", 100);
    for (size_t i = 0; i < 5; ++i) {
      if (i == 3) gen.setLastOrderKey(1000000);
      gen.generateNextBatch();
      size_t batchSize = gen.getCurrentBatchSize();
      BOOST_CHECK (batchSize >= 100);
      BOOST_CHECK (batchSize < 120);
      Lineorder *lb = gen.getLineorderBuffer();
      MVProjection *mb = gen.getMVBuffer();
      int32_t prevOrderKey = -1;
      int8_t prevLinenum = -1;
      for (size_t j = 0; j < batchSize; ++j) {
        Lineorder &l = lb[j];
        MVProjection &m = mb[j];
        if (i >= 3) {
          BOOST_CHECK (l.orderkey >= 1000000);
        }
        BOOST_CHECK (l.orderkey >= prevOrderKey);
        BOOST_CHECK (l.linenumber > 0);
        if (l.orderkey == prevOrderKey) {
          BOOST_CHECK_EQUAL (l.linenumber, prevLinenum + 1);
        }
        prevOrderKey = l.orderkey;
        prevLinenum = l.linenumber;

        BOOST_CHECK_EQUAL (l.linenumber, m.key.l_linenumber);
        BOOST_CHECK_EQUAL (l.orderkey, m.key.l_orderkey);
        BOOST_CHECK_EQUAL (l.revenue, m.l_revenue);
        BOOST_CHECK_EQUAL (l.quantity, m.l_quantity);
        BOOST_CHECK_EQUAL (l.extendedprice, m.l_extendedprice);
        BOOST_CHECK_EQUAL (l.discount, m.l_discount);
        BOOST_CHECK_EQUAL (l.supplycost, m.l_supplycost);
      }
    }
  }
  BOOST_TEST_MESSAGE("===Tested SSB customized dbgen.");
}

BOOST_AUTO_TEST_CASE(ssb_random_query) {
  BOOST_TEST_MESSAGE("===Testing SSB Random Queries...");
  FEngine engine (TEST_DATA_FOLDER, string(TEST_DATA_FOLDER) + "_tinyssb.sig", 100);
  SSBQueryExecutor exec(&engine);

  int seed = 1223345;
  SSBQueryParam param;
  map<int, int> m;
  for (int i = 0; i < 50; ++i) {
    int query = generateRandomQuery(seed);
    map<int, int>::iterator it = m.find(query);
    if (it == m.end()) m[query] = 1;
    else ++(it->second);
    BOOST_TEST_MESSAGE("Q" << query);
    param.generateRandomParam(query, seed);
    boost::shared_ptr<SSBQueryResult> res = exec.query(query, false, param);
    BOOST_TEST_MESSAGE("  result:" << res->toString());
  }
  for (map<int, int>::const_iterator it = m.begin(); it != m.end(); ++it) {
    BOOST_TEST_MESSAGE("Q" << it->first << ":" << it->second);
  }
  BOOST_TEST_MESSAGE("===Tested SSB Random Queries.");
}

*/
BOOST_AUTO_TEST_CASE(engine_family_merge_btree) {
  BOOST_TEST_MESSAGE("===Testing Fracture Family Merging for BTree...");
  {
    std::remove((TEST_DATA_FOLDER + string("_btreemerge.sig")).c_str());
    FEngine engine (TEST_DATA_FOLDER, string(TEST_DATA_FOLDER) + "_btreemerge.sig", 100);
    std::string TEST_FAMILY ("test_btree_family");
    FFamily *family = engine.createNewFractureFamily(TEST_FAMILY, MV_PROJECTION, false);
  
    DBGen gen ("../../data/tinyssb/", 100);
    std::vector<std::string> names;
    std::vector<int> counts;
    int totalCount = 0;
    for (size_t i = 0; i < 2 * 3; ++i) {
      gen.generateNextBatch();
      size_t batchSize = gen.getCurrentBatchSize();
      FMainMemoryBTree fracture (MV_PROJECTION, 200, false);
      MVProjection *mb = gen.getMVBuffer();
      for (size_t j = 0; j < batchSize; ++j) {
        const MVProjection &m = mb[j];
        fracture.insert(&(m.key), &m);
      }
      fracture.finishInserts();
      stringstream str;
      str << "btree_formerge_" << i << ".db";
      FFileSignature sig = engine.getSignatureSet().dumpToNewRowStoreFile(TEST_DATA_FOLDER, str.str(), fracture);
      family->addOnDiskFracture(sig.getFilepath());
      names.push_back (sig.getFilepath());
      BOOST_CHECK_EQUAL (sig.totalTupleCount, batchSize);
      counts.push_back(batchSize);
      BOOST_TEST_MESSAGE("-tuples[" << i << "]=" << batchSize);
      totalCount += batchSize;
    }
    BOOST_TEST_MESSAGE("-made fractures");
    BOOST_CHECK_EQUAL (family->getOnDiskFractures().size(), 2 * 3);
  
    BOOST_TEST_MESSAGE("-going to do 2 way merges");
    std::vector<std::string> newNames;
    for (int i = 0; i < 3; ++i) {
      std::vector<std::string> merged;
      merged.push_back (names[i * 2]);
      merged.push_back (names[i * 2 + 1]);
      std::string newName = family->mergeFractures(&engine, merged, true, 1 << 20);
      const FFileSignature &sig = engine.getSignatureSet().getFileSignature(newName);
      BOOST_CHECK_EQUAL (sig.totalTupleCount, counts[i * 2] + counts[i * 2 + 1]);
      newNames.push_back (newName);
    }
    BOOST_CHECK_EQUAL (family->getOnDiskFractures().size(), 3);
    BOOST_TEST_MESSAGE("-going to do 3 way merge");
    std::string newName = family->mergeFractures(&engine, newNames, false, 1 << 21);
    const FFileSignature &sig = engine.getSignatureSet().getFileSignature(newName);
    BOOST_CHECK_EQUAL (family->getOnDiskFractures().size(), 1);
    BOOST_CHECK_EQUAL (sig.totalTupleCount, totalCount);
  }

  BOOST_TEST_MESSAGE("===Tested Fracture Family Merging for BTree.");
}
BOOST_AUTO_TEST_CASE(engine_family_merge_cstore) {
  BOOST_TEST_MESSAGE("===Testing Fracture Family Merging for CStore...");
  {
    std::remove((TEST_DATA_FOLDER + string("_cstoremerge.sig")).c_str());
    FEngine engine (TEST_DATA_FOLDER, string(TEST_DATA_FOLDER) + "_cstoremerge.sig", 100);
    std::string TEST_FAMILY ("test_cstore_family");
    FFamily *family = engine.createNewFractureFamily(TEST_FAMILY, MV_PROJECTION, true);
  
    std::vector<FCStoreColumn> columns = FCStoreUtil::getPhysicalDesignsOf(MV_PROJECTION);
  
    DBGen gen ("../../data/tinyssb/", 50);
    std::vector<std::string> names;
    std::vector<int> counts;
    int totalCount = 0;
    for (size_t i = 0; i < 2 * 3; ++i) {
      gen.generateNextBatch();
      size_t batchSize = gen.getCurrentBatchSize();
      FMainMemoryBTree fracture (MV_PROJECTION, 100, false);
      MVProjection *mb = gen.getMVBuffer();
      for (size_t j = 0; j < batchSize; ++j) {
        const MVProjection &m = mb[j];
        fracture.insert(&(m.key), &m);
      }
      fracture.finishInserts();
      stringstream str;
      str << "cstore_formerge_" << i;
      std::vector<FFileSignature> signatures = engine.getSignatureSet().dumpToNewCStoreFiles(TEST_DATA_FOLDER, str.str(), fracture);
      BOOST_CHECK_EQUAL (signatures.size(), columns.size());
      for (size_t j = 0; j < signatures.size(); ++j) {
        BOOST_CHECK_EQUAL (signatures[j].totalTupleCount, batchSize);
      }
      family->addOnDiskFracture(str.str());
      names.push_back (str.str());
      counts.push_back(batchSize);
      BOOST_TEST_MESSAGE("-tuples[" << i << "]=" << batchSize);
      totalCount += batchSize;
    }
    BOOST_TEST_MESSAGE("-made fractures");
    BOOST_CHECK_EQUAL (family->getOnDiskFractures().size(), 2 * 3);
  
    BOOST_TEST_MESSAGE("-going to do 2 way merges");
    std::vector<std::string> newNames;
    for (int i = 0; i < 3; ++i) {
      std::vector<std::string> merged;
      merged.push_back (names[i * 2]);
      merged.push_back (names[i * 2 + 1]);
      std::string newName = family->mergeFractures(&engine, merged, true, 1 << 20);
      std::vector<FFileSignature> signatures = engine.getSignatureSet().getCStoreFileSignatures(TEST_DATA_FOLDER, columns, newName);
      for (size_t j = 0; j < signatures.size(); ++j) {
        BOOST_CHECK_EQUAL (signatures[j].totalTupleCount, counts[i * 2] + counts[i * 2 + 1]);
      }
      newNames.push_back (newName);
    }
    BOOST_CHECK_EQUAL (family->getOnDiskFractures().size(), 3);
    BOOST_TEST_MESSAGE("-going to do 3 way merge");
    std::string newName = family->mergeFractures(&engine, newNames, false, 1 << 21);
    BOOST_CHECK_EQUAL (family->getOnDiskFractures().size(), 1);
    std::vector<FFileSignature> signatures = engine.getSignatureSet().getCStoreFileSignatures(TEST_DATA_FOLDER, columns, newName);
    for (size_t j = 0; j < signatures.size(); ++j) {
      BOOST_CHECK_EQUAL (signatures[j].totalTupleCount, totalCount);
    }
  }

  BOOST_TEST_MESSAGE("===Tested Fracture Family Merging for CStore.");
}
