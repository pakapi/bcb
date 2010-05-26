#include "fcstore.h"
#include "fcstoreimpl.h"
#include "fbtree.h"
#include "fbufferpool.h"
#include "ffile.h"
#include "../io/fis.h"
#include "../ssb/ssb.h"
#include "../util/stopwatch.h"
#include "../util/hashmap.h"
#include <stdint.h>
#include <string.h>
#include <cstdio>
#include <sstream>
#include <boost/scoped_ptr.hpp>
#include <glog/logging.h>

using namespace std;
using namespace boost;

namespace fdb {

// ==========================================================================
//  CStore Physical Designs
// ==========================================================================
int calculateOffset (void *propertyPointer, void *basePointer) {
  int offset = reinterpret_cast<char*>(propertyPointer) - reinterpret_cast<char*>(basePointer);
  assert (offset >= 0);
  return offset;
}

std::vector<FCStoreColumn> FCStoreUtil::getPhysicalDesignsOf(TableType table) {
  std::vector<FCStoreColumn> ret;

  // objects to calculate an offset of each column. kinda dirty hack...
  Lineorder l; void *lp = &l;
  Customer c; void *cp = &c;
  Supplier s; void *sp = &s;
  Part p; void *pp = &p;
  Date d; void *dp = &d;
  MVProjection m; void *mp = &m;
  size_t totalSize = 0;
  switch (table) {
    case  LINEORDER_PK_SORT:
      ret.push_back (FCStoreColumn("orderkey", COLUMN_INT32, calculateOffset(&(l.orderkey), lp), RLE_COMPRESSED));
      ret.push_back (FCStoreColumn("linenumber", COLUMN_INT8, calculateOffset(&(l.linenumber), lp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("custkey", COLUMN_INT32, calculateOffset(&(l.custkey), lp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("partkey", COLUMN_INT32, calculateOffset(&(l.partkey), lp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("suppkey", COLUMN_INT32, calculateOffset(&(l.suppkey), lp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("orderdate", COLUMN_INT32, calculateOffset(&(l.orderdate), lp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("orderpriority", COLUMN_CHAR, sizeof(l.orderpriority), calculateOffset(&(l.orderpriority), lp), DICTIONARY_COMPRESSED));
      ret.push_back (FCStoreColumn("shippriority", COLUMN_CHAR, sizeof(l.shippriority), calculateOffset(&(l.shippriority), lp), DICTIONARY_COMPRESSED));
      ret.push_back (FCStoreColumn("quantity", COLUMN_INT8, calculateOffset(&(l.quantity), lp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("extendedprice", COLUMN_INT32, calculateOffset(&(l.extendedprice), lp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("ordertotalprice", COLUMN_INT32, calculateOffset(&(l.ordertotalprice), lp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("discount", COLUMN_INT8, calculateOffset(&(l.discount), lp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("revenue", COLUMN_INT32, calculateOffset(&(l.revenue), lp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("supplycost", COLUMN_INT32, calculateOffset(&(l.supplycost), lp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("tax", COLUMN_INT8, calculateOffset(&(l.tax), lp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("commitdate", COLUMN_INT32, calculateOffset(&(l.commitdate), lp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("shipmode", COLUMN_CHAR, sizeof(l.shipmode), calculateOffset(&(l.shipmode), lp), DICTIONARY_COMPRESSED));
      totalSize = sizeof(Lineorder);
      break;
    case  CUSTOMER_PK_SORT:
      ret.push_back (FCStoreColumn("custkey", COLUMN_INT32, calculateOffset(&(c.custkey), cp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("name", COLUMN_CHAR, sizeof(c.name), calculateOffset(&(c.name), cp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("address", COLUMN_CHAR, sizeof(c.address), calculateOffset(&(c.address), cp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("city", COLUMN_CHAR, sizeof(c.city), calculateOffset(&(c.city), cp), DICTIONARY_COMPRESSED));
      ret.push_back (FCStoreColumn("nation", COLUMN_CHAR, sizeof(c.nation), calculateOffset(&(c.nation), cp), DICTIONARY_COMPRESSED));
      ret.push_back (FCStoreColumn("region", COLUMN_CHAR, sizeof(c.region), calculateOffset(&(c.region), cp), DICTIONARY_COMPRESSED));
      ret.push_back (FCStoreColumn("phone", COLUMN_CHAR, sizeof(c.phone), calculateOffset(&(c.phone), cp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("mktsegment", COLUMN_CHAR, sizeof(c.mktsegment), calculateOffset(&(c.mktsegment), cp), DICTIONARY_COMPRESSED));
      totalSize = sizeof(Customer);
      break;
    case  SUPPLIER_PK_SORT:
      ret.push_back (FCStoreColumn("suppkey", COLUMN_INT32, calculateOffset(&(s.suppkey), sp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("name", COLUMN_CHAR, sizeof(s.name), calculateOffset(&(s.name), sp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("address", COLUMN_CHAR, sizeof(s.address), calculateOffset(&(s.address), sp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("city", COLUMN_CHAR, sizeof(s.city), calculateOffset(&(s.city), sp), DICTIONARY_COMPRESSED));
      ret.push_back (FCStoreColumn("nation", COLUMN_CHAR, sizeof(s.nation), calculateOffset(&(s.nation), sp), DICTIONARY_COMPRESSED));
      ret.push_back (FCStoreColumn("region", COLUMN_CHAR, sizeof(s.region), calculateOffset(&(s.region), sp), DICTIONARY_COMPRESSED));
      ret.push_back (FCStoreColumn("phone", COLUMN_CHAR, sizeof(s.phone), calculateOffset(&(s.phone), sp), UNCOMPRESSED));
      totalSize = sizeof(Supplier);
      break;
    case  PART_PK_SORT:
      ret.push_back (FCStoreColumn("partkey", COLUMN_INT32, calculateOffset(&(p.partkey), pp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("name", COLUMN_CHAR, sizeof(p.name), calculateOffset(&(p.name), pp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("mfgr", COLUMN_CHAR, sizeof(p.mfgr), calculateOffset(&(p.mfgr), pp), DICTIONARY_COMPRESSED));
      ret.push_back (FCStoreColumn("category", COLUMN_CHAR, sizeof(p.category), calculateOffset(&(p.category), pp), DICTIONARY_COMPRESSED));
      ret.push_back (FCStoreColumn("brand", COLUMN_CHAR, sizeof(p.brand), calculateOffset(&(p.brand), pp), DICTIONARY_COMPRESSED));
      ret.push_back (FCStoreColumn("color", COLUMN_CHAR, sizeof(p.color), calculateOffset(&(p.color), pp), DICTIONARY_COMPRESSED));
      ret.push_back (FCStoreColumn("type", COLUMN_CHAR, sizeof(p.type), calculateOffset(&(p.type), pp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("size", COLUMN_INT8, calculateOffset(&(p.size), pp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("container", COLUMN_CHAR, sizeof(p.container), calculateOffset(&(p.container), pp), DICTIONARY_COMPRESSED));
      totalSize = sizeof(Part);
      break;
    case  DATE_PK_SORT:
      ret.push_back (FCStoreColumn("datekey", COLUMN_INT32, calculateOffset(&(d.datekey), dp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("date", COLUMN_CHAR, sizeof(d.date), calculateOffset(&(d.date), dp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("dayofweek", COLUMN_CHAR, sizeof(d.dayofweek), calculateOffset(&(d.dayofweek), dp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("month", COLUMN_CHAR, sizeof(d.month), calculateOffset(&(d.month), dp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("year", COLUMN_INT16, calculateOffset(&(d.year), dp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("yearmonthnum", COLUMN_INT32, calculateOffset(&(d.yearmonthnum), dp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("yearmonth", COLUMN_CHAR, sizeof(d.yearmonth), calculateOffset(&(d.yearmonth), dp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("daynuminweek", COLUMN_INT8, calculateOffset(&(d.daynuminweek), dp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("daynuminmonth", COLUMN_INT8, calculateOffset(&(d.daynuminmonth), dp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("daynuminyear", COLUMN_INT16, calculateOffset(&(d.daynuminyear), dp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("monthnuminyear", COLUMN_INT8, calculateOffset(&(d.monthnuminyear), dp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("weeknuminyear", COLUMN_INT8, calculateOffset(&(d.weeknuminyear), dp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("sellingseason", COLUMN_CHAR, sizeof(d.sellingseason), calculateOffset(&(d.sellingseason), dp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("lastdayinweekfl", COLUMN_INT8, calculateOffset(&(d.lastdayinweekfl), dp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("lastdayinmonthfl", COLUMN_INT8, calculateOffset(&(d.lastdayinmonthfl), dp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("holidayfl", COLUMN_INT8, calculateOffset(&(d.holidayfl), dp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("weekdayfl", COLUMN_INT8, calculateOffset(&(d.weekdayfl), dp), UNCOMPRESSED));
      totalSize = sizeof(Date);
      break;

    case  MV_PROJECTION:
      ret.push_back (FCStoreColumn("s_region", COLUMN_CHAR, sizeof(m.key.s_region), calculateOffset(&(m.key.s_region), mp), RLE_COMPRESSED));
      ret.push_back (FCStoreColumn("d_year", COLUMN_INT16, calculateOffset(&(m.key.d_year), mp), RLE_COMPRESSED));
      ret.push_back (FCStoreColumn("c_region", COLUMN_CHAR, sizeof(m.key.c_region), calculateOffset(&(m.key.c_region), mp), RLE_COMPRESSED));
      ret.push_back (FCStoreColumn("s_nation", COLUMN_CHAR, sizeof(m.key.s_nation), calculateOffset(&(m.key.s_nation), mp), RLE_COMPRESSED));
      ret.push_back (FCStoreColumn("c_nation", COLUMN_CHAR, sizeof(m.key.c_nation), calculateOffset(&(m.key.c_nation), mp), RLE_COMPRESSED));
      ret.push_back (FCStoreColumn("s_city", COLUMN_CHAR, sizeof(m.key.s_city), calculateOffset(&(m.key.s_city), mp), RLE_COMPRESSED));
      ret.push_back (FCStoreColumn("c_city", COLUMN_CHAR, sizeof(m.key.c_city), calculateOffset(&(m.key.c_city), mp), RLE_COMPRESSED));
      ret.push_back (FCStoreColumn("d_yearmonthnum", COLUMN_INT32, calculateOffset(&(m.key.d_yearmonthnum), mp), RLE_COMPRESSED));
      ret.push_back (FCStoreColumn("d_yearmonth", COLUMN_CHAR, sizeof(m.key.d_yearmonth), calculateOffset(&(m.key.d_yearmonth), mp), RLE_COMPRESSED));
      ret.push_back (FCStoreColumn("l_orderkey", COLUMN_INT32, calculateOffset(&(m.key.l_orderkey), mp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("l_linenumber", COLUMN_INT8, calculateOffset(&(m.key.l_linenumber), mp), UNCOMPRESSED));

      ret.push_back (FCStoreColumn("l_quantity", COLUMN_INT8, calculateOffset(&(m.l_quantity), mp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("l_extendedprice", COLUMN_INT32, calculateOffset(&(m.l_extendedprice), mp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("l_discount", COLUMN_INT8, calculateOffset(&(m.l_discount), mp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("l_revenue", COLUMN_INT32, calculateOffset(&(m.l_revenue), mp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("l_supplycost", COLUMN_INT32, calculateOffset(&(m.l_supplycost), mp), UNCOMPRESSED));
      ret.push_back (FCStoreColumn("p_mfgr", COLUMN_CHAR, sizeof(m.p_mfgr), calculateOffset(&(m.p_mfgr), mp), DICTIONARY_COMPRESSED));
      ret.push_back (FCStoreColumn("p_category", COLUMN_CHAR, sizeof(m.p_category), calculateOffset(&(m.p_category), mp), DICTIONARY_COMPRESSED));
      ret.push_back (FCStoreColumn("p_brand", COLUMN_CHAR, sizeof(m.p_brand), calculateOffset(&(m.p_brand), mp), DICTIONARY_COMPRESSED));
      ret.push_back (FCStoreColumn("d_weeknuminyear", COLUMN_INT8, calculateOffset(&(m.d_weeknuminyear), mp), UNCOMPRESSED));
      totalSize = sizeof(MVProjection);
      break;
    default:
      // unsupported type
      assert (false);
      throw std::exception();
  }
#ifdef DEBUG
  assert (totalSize > 0);
  for (size_t i = 0; i < ret.size(); ++i) {
    if (i == 0) {
      assert (ret[i].offset == 0); // not 100% sure this is true in every environment...
    } else {
      assert (ret[i].offset > ret[i - 1].offset);
    }
    assert (ret[i].offset < (int) totalSize);
  }
#endif //DEBUG
  return ret;
}
std::vector<SortOrder> FCStoreUtil::getSortOrdersOf(TableType table) {
  std::vector<SortOrder> orders;
  switch (table) {
    case  LINEORDER_PK_SORT: // orderkey/linenumber
      orders.push_back (SortOrder(0, true));
      orders.push_back (SortOrder(1, true));
      break;
    case  CUSTOMER_PK_SORT: // PK in each table
    case  SUPPLIER_PK_SORT:
    case  PART_PK_SORT:
    case  DATE_PK_SORT:
      orders.push_back (SortOrder(0, true));
      break;
    case  MV_PROJECTION:
      for (int i = 0; i < 11; ++i) orders.push_back (SortOrder(i, true));
      break;
    default:
      // unsupported type
      assert (false);
      throw std::exception();
  }
  return orders;
}

// ==========================================================================
//  Dump method for RowStore temporary table to CStore file
// ==========================================================================
void dumpUncompressedColumn(CStoreDumpContext &context, const FMainMemoryBTree &btree);
void dumpRLECompressedColumn(CStoreDumpContext &context, const FMainMemoryBTree &btree);
void dumpDictionaryCompressedColumn(CStoreDumpContext &context, const FMainMemoryBTree &btree);

void FCStoreUtil::dumpToNewCStoreFile (
  std::vector<FFileSignature> &signatures, const FMainMemoryBTree &btree) {
  std::vector<FCStoreColumn> columns = getPhysicalDesignsOf(btree.getTableType());
  std::vector<SortOrder> orders = getSortOrdersOf(btree.getTableType());
  assert (columns.size() == signatures.size());
  LOG(INFO) << "dumping an on-memory btree to new CStore files...";

  StopWatch watch;
  watch.init();

  assert (FDB_PAGE_SIZE % FDB_DIRECT_IO_ALIGNMENT == 0);
  ScopedMemoryForIO bufferPtr(FDB_DISK_WRITE_BUFFER_PAGES * FDB_PAGE_SIZE, FDB_DIRECT_IO_ALIGNMENT, FDB_USE_DIRECT_IO);
  void *buffer = bufferPtr.get();

  const size_t count = signatures.size();
  for (size_t i = 0; i < count; ++i) {
    FFileSignature &signature = signatures[i];
    const FCStoreColumn &column = columns[i];

#ifdef DEBUG
    StopWatch watchColumn;
    watchColumn.init();
#endif //DEBUG

    assert (signature.fileId > 0);
    assert (signature.filepath != NULL);
    std::string filepath (signature.filepath);
    if (std::remove((filepath).c_str()) == 0) {
      VLOG(1) << "deleted existing file " << (filepath) << ".";
    }
    VLOG(1) << "dumping an on-memory btree to a new CStore file " << filepath << " (compression=" << toCompressionSchemeName(column.compression) << ")...";

    scoped_ptr<DirectFileOutputStream> fd(new DirectFileOutputStream(filepath, FDB_USE_DIRECT_IO));
    CStoreDumpContext context(signature.fileId, fd.get(), (char*) buffer, column, btree);

    ::memset (buffer, 0, FDB_DISK_WRITE_BUFFER_PAGES * FDB_PAGE_SIZE);
    if (column.compression == UNCOMPRESSED) {
      dumpUncompressedColumn (context, btree);
    } else if (column.compression == RLE_COMPRESSED) {
      dumpRLECompressedColumn (context, btree);
    } else if (column.compression == DICTIONARY_COMPRESSED) {
      dumpDictionaryCompressedColumn(context, btree);
    } else {
      LOG(ERROR) << "Unsupported compression type:" << column.compression;
      throw std::exception();
    }
    // done. flush and close
    fd->sync();
    fd->close();
    context.updateFileSignature(signature, btree.getTableType(), i);
#ifdef DEBUG
    watchColumn.stop();
    VLOG(1) << "finished writing " << signature.pageCount << " pages in " << watchColumn.getElapsed() << " microsec.";
#endif //DEBUG
  }
  watch.stop();
  LOG(INFO) << "completed all dumping. " << watch.getElapsed() << " micsosec";
}

CStoreDumpContext::CStoreDumpContext(int fileId_, DirectFileOutputStream *fd_, char *buffer_, const FCStoreColumn &column_, const FMainMemoryBTree &btree) {
  fileId = fileId_;
  fd = fd_;
  buffer = buffer_;
  bufferedPages = 0;
  currentPageId = 0;
  currentPageOffset = 0;
  currentTuple = 0;
  tupleCount = btree.size();
  column = column_;
  dictionaryBits = 0;
  dictionaryHashmap = NULL;
  switch (column.compression) {
  case UNCOMPRESSED:
    leafEntrySize = column.maxLength;
    entryPerLeafPage = (FDB_PAGE_SIZE - sizeof (FPageHeader)) / leafEntrySize;
    break;
  case RLE_COMPRESSED:
    leafEntrySize = column.maxLength + sizeof(int); // value + runlength
    entryPerLeafPage = (FDB_PAGE_SIZE - sizeof (FPageHeader)) / leafEntrySize;
    break;
  case DICTIONARY_COMPRESSED:
    leafEntrySize = 0; // determined later
    entryPerLeafPage = 0; // determined later
    dictionaryHashmap = new StringHashMap<uint16_t>(column.maxLength, 16);
    break;
  default:
      // unsupported type
      assert (false);
      throw std::exception();
  }
  entryInCurrentPage = 0;

  runTotal = 0;
  currentRunCount = 0;
  currentRunBeginningPos = 0;
  currentRunValue = NULL;

  dictionarySize = 0;
  currentPackedByte = 0;
  currentBitOffset = 0;
  rootPageStart = 0;
  rootPageCount = 0;
  rootPageLevel = 0;
  leafPageCount = 0;
}
CStoreDumpContext::~CStoreDumpContext() {
  if (dictionaryHashmap != NULL) delete dictionaryHashmap;
}

void CStoreDumpContext::updateFileSignature(FFileSignature &signature, TableType tableType, int columnIndex) const {
  signature.columnFile = true;
  signature.columnType = column.type;
  signature.columnMaxLength = column.maxLength;
  signature.columnIndex = columnIndex;
  signature.columnOffset = column.offset;
  signature.columnCompression = column.compression;

  signature.totalTupleCount = tupleCount;
  signature.keyEntrySize = 0; // no meaning in column store
  signature.leafEntrySize = 0; // no meaning in column store
  signature.keyCompareFuncType = KEY_CMP_INVALID;
  signature.rootPageStart = rootPageStart;
  signature.rootPageCount = rootPageCount;
  signature.rootPageLevel = rootPageLevel;
  signature.tableType = tableType;
  signature.pageCount = currentPageId;
  signature.leafPageCount = leafPageCount;
  signature.dictionaryBits = dictionaryBits;
  signature.dictionaryEntryCount = dictionarySize;
}

bool CStoreDumpContext::flushBufferIfNeeded() {
  // check if we need to flush buffered pages
  assert (currentPageOffset == 0);
  assert (bufferedPages <= FDB_DISK_WRITE_BUFFER_PAGES);
  if (bufferedPages == FDB_DISK_WRITE_BUFFER_PAGES) {
    flushBuffer();
    return true;
  }
  return false;
}
void CStoreDumpContext::flushBuffer() {
  assert (currentPageOffset == 0);
  assert (currentBitOffset == 0);
  assert (bufferedPages <= FDB_DISK_WRITE_BUFFER_PAGES);
  if (bufferedPages > 0) {
    VLOG(2) << "flush!";
    fd->write (buffer, FDB_PAGE_SIZE * bufferedPages);
    bufferedPages = 0;
    ::memset (buffer, 0, FDB_DISK_WRITE_BUFFER_PAGES * FDB_PAGE_SIZE);
  }
}

bool CStoreDumpContext::flipPageIfNeeded() {
  assert (entryInCurrentPage <= entryPerLeafPage);
  if (entryPerLeafPage == entryInCurrentPage) {
    flipPage();
    return true;
  }
  return false;
}
void CStoreDumpContext::flipPage() {
  if (currentPageOffset > 0) {
    ++(currentPageId);
    currentPageOffset = 0;
    entryInCurrentPage = 0;
    ++(bufferedPages);
    assert (bufferedPages <= FDB_DISK_WRITE_BUFFER_PAGES);
  }
}

void CStoreDumpContext::writeLeafPageHeader(int countInThisPage, bool lastSibling, int64_t beginningPos) {
  writePageHeader (countInThisPage, lastSibling, beginningPos, 0, false, leafEntrySize);
}
void CStoreDumpContext::writePageHeader(int countInThisPage, bool lastSibling, int64_t beginningPos, int level, bool root, int entrySize) {
  FPageHeader *header = reinterpret_cast<FPageHeader*> (buffer + (FDB_PAGE_SIZE * bufferedPages));
  header->magicNumber = MAGIC_NUMBER;
  header->fileId = fileId;
  header->pageId = currentPageId;
  header->level = level;
  header->root = root;
  header->entrySize = entrySize;
  header->count = countInThisPage;
  header->lastSibling = lastSibling;
  header->beginningPos = beginningPos;
  if (lastSibling) {
    VLOG(2) << "last page!";
  } else {
    VLOG(2) << "more page!";
  }
  currentPageOffset = sizeof (FPageHeader);
}

void CStoreDumpContext::writeLeafEntry(const char *entryData) {
  ::memcpy(buffer + (FDB_PAGE_SIZE * bufferedPages) + currentPageOffset, entryData, leafEntrySize);
  currentPageOffset += leafEntrySize;
  ++entryInCurrentPage;
  ++currentTuple;
}

void CStoreDumpContext::writeLeafEntryRLE(int runLength, const char *entryData) {
  assert ((int) sizeof(int) + column.maxLength == leafEntrySize);
  ::memcpy(buffer + (FDB_PAGE_SIZE * bufferedPages) + currentPageOffset, &runLength, sizeof(int));
  ::memcpy(buffer + (FDB_PAGE_SIZE * bufferedPages) + currentPageOffset + sizeof(int), entryData, column.maxLength);
  currentPageOffset += leafEntrySize;
  ++entryInCurrentPage;
}

void CStoreDumpContext::writeRootEntryRLE(int64_t beginningPos, int pageId) {
  ::memcpy(buffer + (FDB_PAGE_SIZE * bufferedPages) + currentPageOffset, &beginningPos, sizeof(int64_t));
  ::memcpy(buffer + (FDB_PAGE_SIZE * bufferedPages) + currentPageOffset + sizeof(int64_t), &pageId, sizeof(int));
  currentPageOffset += sizeof(int64_t) + sizeof(int);
  ++entryInCurrentPage;
}

void CStoreDumpContext::writeDictionary () {
  // flush last pages
  flipPage();
  flushBuffer();

  // then, write root (dictionary) pages
  int entriesInRootPage = (FDB_PAGE_SIZE - sizeof (FPageHeader)) / column.maxLength;
  rootPageStart = currentPageId;
  rootPageCount = (dictionarySize / entriesInRootPage) + (dictionarySize % entriesInRootPage == 0 ? 0 : 1);
  rootPageLevel = 1;
  for (int i = 0; i < rootPageCount; ++i) {
    bool lastSibling;
    int countInThisPage;
    if (i == rootPageCount - 1) {
      countInThisPage = dictionarySize - i * entriesInRootPage;
      assert (countInThisPage <= entriesInRootPage);
      assert (countInThisPage > 0);
      lastSibling = true;
    } else {
      countInThisPage = entriesInRootPage;
      lastSibling = false;
    }
    writePageHeader(countInThisPage, lastSibling, 0, 1, true, column.maxLength);

    char *p = buffer + (FDB_PAGE_SIZE * bufferedPages) + currentPageOffset;
    for (int j = 0; j < countInThisPage; ++j, p += column.maxLength) {
      ::memcpy(p, dictionaryEntries[j + i * entriesInRootPage], column.maxLength);
    }
    currentPageOffset += column.maxLength * countInThisPage;
    entryInCurrentPage += countInThisPage;
    flipPage();
    flushBufferIfNeeded();
  }
  VLOG(1) << "Wrote dictionary. " << dictionarySize << " entries. " << rootPageCount << " root pages";
  flipPage();
  flushBuffer();
}

void CStoreDumpContext::prepareForNewPageUniform () {
  flipPageIfNeeded();
  if (currentPageOffset == 0) {
    VLOG(2) << "new page!";
    flushBufferIfNeeded();
    int remainingCount = tupleCount - currentTuple;
    int countInThisPage;
    bool lastSibling;
    if (remainingCount >= entryPerLeafPage) {
      countInThisPage = entryPerLeafPage;
      lastSibling = false;
    } else {
      // uncompressed/dictionary file can simply calculate the remaining count
      countInThisPage = remainingCount;
      lastSibling = true;
    }
    writeLeafPageHeader(countInThisPage, lastSibling, currentTuple);
  }
}

// ========================================
//  Uncompressed Column
// ========================================
void dumpUncompressedColumnCallback (void *context, const void *key, const void *data) {
  CStoreDumpContext *dumpContext = reinterpret_cast<CStoreDumpContext*>(context);
  assert (dumpContext->currentTuple < dumpContext->tupleCount);
  dumpContext->prepareForNewPageUniform();
  dumpContext->writeLeafEntry (((char*) data) + dumpContext->column.offset);
}

void dumpUncompressedColumn(CStoreDumpContext &context, const FMainMemoryBTree &btree) {
  btree.traverse(dumpUncompressedColumnCallback, &context);

  // flush last pages
  context.flipPage();
  context.flushBuffer();

  context.leafPageCount = context.currentPageId;
}

// ========================================
//  RLE Compressed Column
// ========================================
void flushCurrentRun (CStoreDumpContext *dumpContext) {
  // flush the last run
  dumpContext->flipPageIfNeeded();
  if (dumpContext->currentPageOffset == 0) {
    VLOG(2) << "new page!";
    dumpContext->flushBufferIfNeeded();
    // determining countInThisPage and whether it's last or not is difficult in RLE
    // this will be overwritten when this page turns out to be the last sibling
    dumpContext->writeLeafPageHeader(dumpContext->entryPerLeafPage, false, dumpContext->currentRunBeginningPos);
    dumpContext->pageBeginningPositions.push_back (dumpContext->currentRunBeginningPos);
    assert ((int) dumpContext->pageBeginningPositions.size() == dumpContext->currentPageId + 1);
  }
  if (dumpContext->currentRunValue != NULL) {
    dumpContext->writeLeafEntryRLE(dumpContext->currentRunCount, dumpContext->currentRunValue);
  }
}
void dumpRLECompressedColumnCallback (void *context, const void *key, const void *data) {
  CStoreDumpContext *dumpContext = reinterpret_cast<CStoreDumpContext*>(context);
  assert (dumpContext->currentTuple < dumpContext->tupleCount);

  // write RLE data only when the value changes
  const char *currentValue = ((char*) data) + dumpContext->column.offset;
  if (dumpContext->currentRunValue == NULL || ::memcmp(dumpContext->currentRunValue, currentValue, dumpContext->column.maxLength) != 0) {
    VLOG(2) << "new run!";
    flushCurrentRun (dumpContext);

    // Start a new run
    dumpContext->currentRunBeginningPos = dumpContext->currentTuple;
    dumpContext->currentRunValue = currentValue;
    dumpContext->currentRunCount = 1;
    ++(dumpContext->runTotal);
  } else {
    ++(dumpContext->currentRunCount);
  }
  ++(dumpContext->currentTuple);
}

void dumpRLECompressedColumn(CStoreDumpContext &context, const FMainMemoryBTree &btree) {
  btree.traverse(dumpRLECompressedColumnCallback, &context);

  // flush last run
  flushCurrentRun (&context);
  // overwrite count/lastSibling of the last page
  {
    FPageHeader *header = reinterpret_cast<FPageHeader*>(context.buffer + (FDB_PAGE_SIZE * context.bufferedPages));
    assert (header->lastSibling == false);
    assert (header->count == context.entryPerLeafPage);
    header->lastSibling = true;
    header->count = context.entryInCurrentPage;
  }

  // flush last pages
  context.flipPage();
  context.flushBuffer();

  context.leafPageCount = context.currentPageId;

  VLOG(1) << "in total " << context.runTotal << " runs";

  // then, write root pages for position search
  size_t rootEntrySize = sizeof(int64_t) + sizeof (int);
  int leafPageCount = context.pageBeginningPositions.size();
  assert (leafPageCount == context.currentPageId);
  int entriesInRootPage = (FDB_PAGE_SIZE - sizeof (FPageHeader)) / rootEntrySize;
  context.rootPageStart = context.currentPageId;
  context.rootPageCount = (leafPageCount / entriesInRootPage) + (leafPageCount % entriesInRootPage == 0 ? 0 : 1);
  context.rootPageLevel = 1;
  for (int i = 0; i < context.rootPageCount; ++i) {
    bool lastSibling;
    int countInThisPage;
    if (i == context.rootPageCount - 1) {
      countInThisPage = leafPageCount - i * entriesInRootPage;
      assert (countInThisPage <= entriesInRootPage);
      assert (countInThisPage > 0);
      lastSibling = true;
    } else {
      countInThisPage = entriesInRootPage;
      lastSibling = false;
    }
    context.writePageHeader(countInThisPage, lastSibling, context.pageBeginningPositions[i * entriesInRootPage], 1, true, rootEntrySize);
    for (int j = 0; j < countInThisPage; ++j) {
      int pageId = i * entriesInRootPage + j;
      int64_t beginningPos = context.pageBeginningPositions[pageId];
      context.writeRootEntryRLE(beginningPos, pageId);
    }
    context.flipPage();
    context.flushBufferIfNeeded();
  }
  VLOG(1) << "RLE " << context.rootPageCount << " root pages";
  context.flipPage();
  context.flushBuffer();
}

// ========================================
//  Dictionary Encoded Column
// ========================================
void flushCurrentPackedByte(CStoreDumpContext *dumpContext) {
  assert (dumpContext->currentBitOffset >= 0);
  assert (dumpContext->currentBitOffset <= 8);
  dumpContext->currentBitOffset = 0;
  *reinterpret_cast<unsigned char*>(dumpContext->buffer + (FDB_PAGE_SIZE * dumpContext->bufferedPages) + dumpContext->currentPageOffset) = dumpContext->currentPackedByte;
  dumpContext->currentPackedByte = 0;
  ++(dumpContext->currentPageOffset);
}

void dumpSmallDictionaryCompressedColumnCallback (void *context, const void *key, const void *data) {
  CStoreDumpContext *dumpContext = reinterpret_cast<CStoreDumpContext*>(context);
  assert (dumpContext->dictionaryBits <= 4);
  assert (dumpContext->currentTuple < dumpContext->tupleCount);

  if (dumpContext->currentBitOffset == 0) {
    dumpContext->prepareForNewPageUniform();
  }

  const char* value = ((char*) data) + dumpContext->column.offset;
  uint8_t foundIndex = dumpContext->dictionaryHashmap->find(value);

  // <=4bits have to pack to a byte.
  dumpContext->currentPackedByte |= (foundIndex << dumpContext->currentBitOffset);
  ++(dumpContext->entryInCurrentPage);
  (dumpContext->currentBitOffset) += dumpContext->dictionaryBits;
  if (dumpContext->currentBitOffset == 8) {
    flushCurrentPackedByte(dumpContext);
  }
  ++(dumpContext->currentTuple);
}

template <typename INT_TYPE>
void dumpLargeDictionaryCompressedColumnCallback (void *context, const void *key, const void *data) {
  CStoreDumpContext *dumpContext = reinterpret_cast<CStoreDumpContext*>(context);
  assert (dumpContext->dictionaryBits >= 8);
  assert (dumpContext->currentTuple < dumpContext->tupleCount);
  assert (sizeof(INT_TYPE) == dumpContext->leafEntrySize);

  dumpContext->prepareForNewPageUniform();
  const char* value = ((char*) data) + dumpContext->column.offset;
  // simply write the current data, but in given length of int
  INT_TYPE foundIndex = dumpContext->dictionaryHashmap->find(value);
  dumpContext->writeLeafEntry(reinterpret_cast<char*>(&foundIndex));
}

struct CompFunctor {
  CompFunctor (int columnLength) : _columnLength (columnLength) {}
  bool operator() (const char *k1, const char *k2) {
    return ::memcmp (k1, k2, _columnLength) < 0;
  }
  int _columnLength;
};

void dumpDictionaryCompressedColumn(CStoreDumpContext &context, const FMainMemoryBTree &btree) {
#ifndef NDEBUG
  StopWatch watch;
  watch.init();
#endif // NDEBUG

  // at most 2^16 entries. so far.
  StringHashSet hashset (context.column.maxLength, 16);
  assert (context.dictionarySize == 0);

  // this is to just bulid dictionaries. order doesn't matter.
  // so, unsorted buffer is enough
  const char *value = reinterpret_cast<const char *>(btree.getUnsortedBuffer());
  value += context.column.offset;
  const int columnLength = context.column.maxLength;
  const int dataSize = btree.getDataSize();
  const int64_t tuples = btree.size();

  for (int64_t i = 0; i < tuples; ++i, value += dataSize) {
    if (hashset.find(value) == NULL) {
      // new value!
      if (context.dictionarySize >= (1 << 16)) {
        LOG (ERROR) << "more than 2^16 distinct values. too many for dictionary encoding.";
        assert (false);
      }
      // note that the data resides until we erase the on-memory table.
      // we don't have to copy the data.
      hashset.insert (value);
      context.dictionaryEntries.push_back (value);
      ++(context.dictionarySize);
    }
  }
  assert ((int) context.dictionaryEntries.size() == context.dictionarySize);

  // to assure that dictionary is equivalent in comparison (<,>)
  // sort the entries and assign IDs.
  std::sort (context.dictionaryEntries.begin(), context.dictionaryEntries.end(), CompFunctor(columnLength));
  for (int i = 0; i < context.dictionarySize; ++i) {
    context.dictionaryHashmap->insert(context.dictionaryEntries[i], i);
  }

#ifndef NDEBUG
  watch.stop();
  VLOG(2) << "Dictionary built. " << context.dictionarySize << " entries. " << watch.getElapsed() << " microsec";
#endif // NDEBUG

  assert (context.currentBitOffset == 0);

  // decide the bits to store one value, and then traverse the BTree in key sort order.
  if (context.dictionarySize <= (1 << 1)) {
    context.dictionaryBits = 1;
    context.leafEntrySize = 1;
    context.entryPerLeafPage = (FDB_PAGE_SIZE - sizeof (FPageHeader)) * 8;
    btree.traverse(dumpSmallDictionaryCompressedColumnCallback, &context);
  } else if (context.dictionarySize <= (1 << 2)) {
    context.dictionaryBits = 2;
    context.leafEntrySize = 1;
    context.entryPerLeafPage = (FDB_PAGE_SIZE - sizeof (FPageHeader)) * 4;
    btree.traverse(dumpSmallDictionaryCompressedColumnCallback, &context);
  } else if (context.dictionarySize <= (1 << 4)) {
    context.dictionaryBits = 4;
    context.leafEntrySize = 1;
    context.entryPerLeafPage = (FDB_PAGE_SIZE - sizeof (FPageHeader)) * 2;
    btree.traverse(dumpSmallDictionaryCompressedColumnCallback, &context);
  } else if (context.dictionarySize <= (1 << 8)) {
    context.dictionaryBits = 8;
    context.leafEntrySize = 1;
    context.entryPerLeafPage = (FDB_PAGE_SIZE - sizeof (FPageHeader));
    btree.traverse(dumpLargeDictionaryCompressedColumnCallback<uint8_t>, &context);
  } else {
    assert (context.dictionarySize <= (1 << 16));
    context.dictionaryBits = 16;
    context.leafEntrySize = 2;
    context.entryPerLeafPage = (FDB_PAGE_SIZE - sizeof (FPageHeader)) / 2;
    btree.traverse(dumpLargeDictionaryCompressedColumnCallback<uint16_t>, &context);
  }

  // flush last bits
  if (context.currentBitOffset != 0) {
    flushCurrentPackedByte(&context);
  }
  context.leafPageCount = context.currentPageId;
  context.writeDictionary();

  VLOG(2) << "Dumped Dictionary Compressed column";
}

// ==========================================================================
//  CStore temporary table implementation
// ==========================================================================
// TODO not implemented so far
FMainMemoryCStoreImpl::FMainMemoryCStoreImpl (TableType type, int maxTuples)
  : _type (type), _maxTuples(maxTuples) {
}
FMainMemoryCStoreImpl::~FMainMemoryCStoreImpl() {
}


// ==========================================================================
//  Read-Only disk-based CStore table implementation
// ==========================================================================


FReadOnlyCStore::FReadOnlyCStore (FBufferPool *bufferpool, TableType type, const FSignatureSet &signatureSet, const std::string &dataFolder, const std::string &filenamePrefix) : _bufferpool (bufferpool), _type(type) {
  _columns = FCStoreUtil::getPhysicalDesignsOf(type);
  vector<FFileSignature> signatures = signatureSet.getCStoreFileSignatures(dataFolder, _columns, filenamePrefix);
  assert (_columns.size() == signatures.size());
  for (size_t i = 0; i < _columns.size(); ++i) {
    const FCStoreColumn &column = _columns[i];
    const FFileSignature &signature = signatures[i];
    boost::shared_ptr<FColumnReader> reader;
    switch (column.compression) {
    case UNCOMPRESSED:
      reader = boost::shared_ptr<FColumnReader>(new FColumnReaderImplUncompressed(bufferpool, column, signature));
      break;
    case RLE_COMPRESSED:
      reader = boost::shared_ptr<FColumnReader>(new FColumnReaderImplRLE(bufferpool, column, signature));
      break;
    case DICTIONARY_COMPRESSED:
      reader = boost::shared_ptr<FColumnReader>(new FColumnReaderImplDictionary(bufferpool, column, signature));
      break;
    default:
      assert (false);
      throw std::exception();
    }
    _columnReaders.push_back(reader);
  }
}

FColumnReader* FReadOnlyCStore::getColumnReader(const std::string &colname) {
  assert (_columnReaders.size() == _columns.size());
  for (size_t i = 0; i < _columnReaders.size(); ++i) {
    if (_columns[i].name == colname) {
      return _columnReaders[i].get();
    }
  }
  assert (false);
  return NULL;
}
FColumnReader* FReadOnlyCStore::getColumnReader(size_t colIndex) {
  assert (colIndex < _columnReaders.size());
  return _columnReaders[colIndex].get();
}

std::string FColumnReaderImpl::normalize(const std::string &str) const {
  assert (str.size() <= (size_t) _column.maxLength);
  std::string ret (_column.maxLength, '\0');
  ret.replace (0, str.size(), str);
  return ret;
}

FColumnReaderImpl::FColumnReaderImpl(FBufferPool *bufferpool, const FCStoreColumn &column, const FFileSignature &signature)
  : _bufferpool(bufferpool), _column(column), _signature(signature), _searchRangeSet(false) {
}
std::string FColumnReaderImpl::toDebugStr (const void *key) const {
  if (_column.type == COLUMN_CHAR) {
    string ret(reinterpret_cast<const char*>(key), _column.maxLength);
    std::replace (ret.begin(), ret.end(), '\0', ' '); // replace padding NULLs with space.
    return "'" + ret + "'";
  } else {
    int64_t val = 0;
    switch (_column.type) {
    case  COLUMN_INT8:
      val = *reinterpret_cast<const int8_t*>(key);
      break;
    case  COLUMN_INT16:
      val = *reinterpret_cast<const int16_t*>(key);
      break;
    case  COLUMN_INT32:
      val = *reinterpret_cast<const int32_t*>(key);
      break;
    case  COLUMN_INT64:
      val = *reinterpret_cast<const int64_t*>(key);
      break;
    default:
      assert (false);
      throw std::exception();
    }
    stringstream str;
    str << val;
    return str.str();
  }
}
void FColumnReaderImpl::logSearchCond (const SearchCond &cond) const {
#ifndef NDEBUG
  switch (cond.type) {
  case SCT_EQUAL:
  case SCT_LT:
  case SCT_GT:
  case SCT_LTEQ:
  case SCT_GTEQ:
    VLOG(1) << "Searching " << toDebugStr(cond.key) << " (" << toSearchCondOp(cond.type) << ") in " << _searchRanges.size() << " ranges (type=" << (_searchRangeSet ? "range set" : "full scan") << ")...";
    break;
  case SCT_BETWEEN:
    VLOG(1) << "Searching " << toDebugStr(cond.key) << " to " << toDebugStr(cond.key2) << " in " << _searchRanges.size() << " ranges (type=" << (_searchRangeSet ? "range set" : "full scan") << ")...";
    break;
  case SCT_IN:
    VLOG(1) << "Searching " << cond.keys.size() << " values for IN clause in " << _searchRanges.size() << " ranges (type=" << (_searchRangeSet ? "range set" : "full scan") << ")...";
    break;
  }
#endif // NDEBUG
}

// ============================
//  Uncompressed Columns
// ============================
FColumnReaderImplUncompressed::FColumnReaderImplUncompressed(
  FBufferPool *bufferpool, const FCStoreColumn &column, const FFileSignature &signature)
: FColumnReaderImpl(bufferpool, column, signature)  {
  _entriesPerPage = (FDB_PAGE_SIZE - sizeof(FPageHeader)) / _column.maxLength;
}

int FColumnReaderImplUncompressed::processPageString(const SearchCond &cond, const char *cursor, size_t tuplesToRead, PositionBitmap *bitmap, int64_t bitmapPageOffset) {
  switch (cond.type) {
  case SCT_EQUAL:
    return processPageStringBinary (std::equal_to<int>(), cond, cursor, tuplesToRead, bitmap, bitmapPageOffset);
  case SCT_LT:
    return processPageStringBinary (std::less<int>(), cond, cursor, tuplesToRead, bitmap, bitmapPageOffset);
  case SCT_GT:
    return processPageStringBinary (std::greater<int>(), cond, cursor, tuplesToRead, bitmap, bitmapPageOffset);
  case SCT_LTEQ:
    return processPageStringBinary (std::less_equal<int>(), cond, cursor, tuplesToRead, bitmap, bitmapPageOffset);
  case SCT_GTEQ:
    return processPageStringBinary (std::greater_equal<int>(), cond, cursor, tuplesToRead, bitmap, bitmapPageOffset);
  case SCT_BETWEEN: return processPageStringBetween (cond, cursor, tuplesToRead, bitmap, bitmapPageOffset);
  case SCT_IN: return processPageStringIn (cond, cursor, tuplesToRead, bitmap, bitmapPageOffset);
  default:
    assert (false);
    return 0;
  }
}
int FColumnReaderImplUncompressed::processPageStringBetween(const SearchCond &cond, const char *cursor, size_t tuplesToRead, PositionBitmap *bitmap, int64_t bitmapPageOffset) {
  int matchCount = 0;
  const int length = _column.maxLength;
  for (size_t i = 0; i < tuplesToRead; ++i, cursor += length) {
    int ret = ::memcmp (cursor, cond.key, length);
    int ret2 = ::memcmp (cursor, cond.key2, length);
    if (ret >= 0 && ret2 <= 0) {
      bitmap->setBit(i + bitmapPageOffset);
      ++matchCount;
    }
  }
  return matchCount;
}
int FColumnReaderImplUncompressed::processPageStringIn(const SearchCond &cond, const char *cursor, size_t tuplesToRead, PositionBitmap *bitmap, int64_t bitmapPageOffset) {
  int matchCount = 0;
  const int length = _column.maxLength;
  for (size_t i = 0; i < tuplesToRead; ++i, cursor += length) {
    if (cond.matchStringIn(cursor, length)) {
      bitmap->setBit(i + bitmapPageOffset);
      ++matchCount;
    }
  }
  return matchCount;
}

void FColumnReaderImplUncompressed::getPositionBitmaps (const SearchCond &cond, std::vector<boost::shared_ptr<PositionBitmap> > &positions) {
#ifndef NDEBUG
  logSearchCond (cond);
  StopWatch watch;
  watch.init();
#endif // NDEBUG
  if (_searchRangeSet == false) {
    // this should not happen. very inefficient if happens
    assert (false);
    throw std::runtime_error ("not implemented yet!");
  }
  int totalMatchCount = 0;
  for (size_t i = 0; i < _searchRanges.size(); ++i) {
    const PositionRange &range = _searchRanges[i];
    size_t tupleCount = range.end - range.begin;
    boost::shared_ptr<PositionBitmap> bitmapPtr = PositionBitmap::newBitmap(range.begin, tupleCount);
    positions.push_back (bitmapPtr);
    PositionBitmap *bitmap = bitmapPtr.get();

    int firstPageId = range.begin / _entriesPerPage;
    int lastPageId = (range.end - 1) / _entriesPerPage;
    assert (firstPageId < _signature.pageCount);
    assert (lastPageId < _signature.pageCount);
    int matchCount = 0;
    for (int pageId = firstPageId; pageId <= lastPageId; ++pageId) {
      const int64_t tuplePageOffset = pageId * _entriesPerPage;
      const char *page = _bufferpool->readPage(_signature, pageId);
      const FPageHeader *header = reinterpret_cast<const FPageHeader*> (page);
      int64_t begin = 0;
      if (pageId == firstPageId) {
        begin = range.begin - tuplePageOffset;
      }
      if (begin >= header->count) {
        begin = header->count;
      }
      assert (begin >= 0);
      assert (begin < _entriesPerPage);
      assert (begin < header->count);
      int64_t end = _entriesPerPage;
      if (pageId == lastPageId) {
        end = range.end - tuplePageOffset;
      }
      if (end > header->count) {
        end = header->count;
      }
      assert (end >= 0);
      assert (end <= _entriesPerPage);
      assert (end <= header->count);
      assert (end >= begin);
      if (end == begin) continue;

      const char *cursor = page + sizeof (FPageHeader) + begin * _column.maxLength;
      size_t tupleToRead = end - begin;
      int64_t bitmapPageOffset = tuplePageOffset + begin - range.begin;
      // searching in one page is template-parameterized to boost performance.
      if (_column.type == COLUMN_CHAR) {
        matchCount += processPageString(cond, cursor, tupleToRead, bitmap, bitmapPageOffset);
      } else {
        switch  (_column.type) {
        case COLUMN_INT8:
          matchCount += processPageInts<int8_t>(cond, cursor, tupleToRead, bitmap, bitmapPageOffset);
          break;
        case COLUMN_INT16:
          matchCount += processPageInts<int16_t>(cond, cursor, tupleToRead, bitmap, bitmapPageOffset);
          break;
        case COLUMN_INT32:
          matchCount += processPageInts<int32_t>(cond, cursor, tupleToRead, bitmap, bitmapPageOffset);
          break;
        case COLUMN_INT64:
          matchCount += processPageInts<int64_t>(cond, cursor, tupleToRead, bitmap, bitmapPageOffset);
          break;
        default:
          assert (false);
          throw std::exception();
        }
      }
    }
    bitmap->matchedCount = matchCount;
    totalMatchCount += matchCount;
  }
#ifndef NDEBUG
  watch.stop();
  VLOG(2) << "Uncompressed::getPositionBitmaps Done. " << totalMatchCount << " entries matched. " << watch.getElapsed() << " microsec";
#endif // NDEBUG
}

void FColumnReaderImplUncompressed::getDecompressedData (const PositionRange &range, void *buffer, size_t bufferSize) {
  assert (range.begin >= 0);
  assert (range.end >= 0);
  assert (range.begin <= range.end);
#ifndef NDEBUG
  StopWatch watch;
  watch.init();
#endif // NDEBUG
  int64_t length = range.end - range.begin;
  assert (bufferSize >= length * _column.maxLength);

  int firstPageId = range.begin / _entriesPerPage;
  int lastPageId = (range.end - 1) / _entriesPerPage;
  assert (firstPageId < _signature.pageCount);
  assert (lastPageId < _signature.pageCount);
  size_t bytesOffset = 0;
  for (int pageId = firstPageId; pageId <= lastPageId; ++pageId) {
    const int64_t tuplePageOffset = pageId * _entriesPerPage;
    const char *page = _bufferpool->readPage(_signature, pageId);
    const FPageHeader *header = reinterpret_cast<const FPageHeader*> (page);
    int64_t begin = 0;
    if (pageId == firstPageId) {
      begin = range.begin - tuplePageOffset;
    }
    if (begin >= header->count) {
      begin = header->count;
    }
    assert (begin >= 0);
    assert (begin < _entriesPerPage);
    assert (begin < header->count);
    int64_t end = _entriesPerPage;
    if (pageId == lastPageId) {
      end = range.end - tuplePageOffset;
    }
    if (end > header->count) {
      end = header->count;
    }
    assert (end >= 0);
    assert (end <= _entriesPerPage);
    assert (end <= header->count);
    assert (end >= begin);
    if (end == begin) continue;

    const char *cursor = page + sizeof (FPageHeader) + begin * _column.maxLength;
    size_t bytesToRead = (end - begin) * _column.maxLength;
    ::memcpy (reinterpret_cast<char *>(buffer) + bytesOffset, cursor, bytesToRead);
    bytesOffset += bytesToRead;
  }

#ifndef NDEBUG
  watch.stop();
  VLOG(2) << "Uncompressed::getDecompressedData Done. " << length << " entries read. " << watch.getElapsed() << " microsec";
#endif // NDEBUG
}

// ============================
//  Dictionary Compressed Columns
// ============================
FColumnReaderImplDictionary::FColumnReaderImplDictionary(
  FBufferPool *bufferpool, const FCStoreColumn &column, const FFileSignature &signature)
: FColumnReaderImpl(bufferpool, column, signature)  {
  _dictionaryBits = signature.dictionaryBits;
  _entriesPerPage = (FDB_PAGE_SIZE - sizeof(FPageHeader)) * 8 / _dictionaryBits;
}

std::vector<int> FColumnReaderImplDictionary::searchDictionary (const SearchCond &cond) {
#ifndef NDEBUG
  StopWatch watch;
  watch.init();
#endif // NDEBUG
  std::vector<int> matchingIds;
  int currentEntryId = 0;
  for (int i = 0; i < _signature.rootPageCount; ++i) {
    const int pageId = i + _signature.rootPageStart;
    const char *page = _bufferpool->readPage(_signature, pageId);
    const FPageHeader *header = reinterpret_cast<const FPageHeader*> (page);
    assert (header->root);
    for (int j = 0; j < header->count; ++j, ++currentEntryId) {
      const char *cursor = page + sizeof (FPageHeader) + j * _column.maxLength;
      bool matched = false;
      if (_column.type == COLUMN_CHAR) {
        matched = cond.matchString(cursor, _column.maxLength);
      } else {
        matched = cond.matchInts(cursor, _column.maxLength);
      }
      if (matched) {
        matchingIds.push_back (currentEntryId);
      }
    }
  }
#ifndef NDEBUG
  watch.stop();
  VLOG(2) << "Searched Dictionary. " << matchingIds.size() << " entries matched. " << watch.getElapsed() << " microsec";
#endif // NDEBUG
  return matchingIds;
}

void FColumnReaderImplDictionary::getPositionBitmaps (const SearchCond &cond, std::vector<boost::shared_ptr<PositionBitmap> > &positions) {
#ifndef NDEBUG
  logSearchCond (cond);
  StopWatch watch;
  watch.init();
#endif // NDEBUG
  if (_searchRangeSet == false) {
    // this should not happen. very inefficient if happens
    assert (false);
    throw std::runtime_error ("not implemented yet!");
  }
  std::vector<int> matchingIds = searchDictionary(cond);
  if (matchingIds.size() == 0) {
    for (size_t i = 0; i < _searchRanges.size(); ++i) {
      const PositionRange &range = _searchRanges[i];
      size_t tupleCount = range.end - range.begin;
      boost::shared_ptr<PositionBitmap> bitmapPtr = PositionBitmap::newBitmap(range.begin, tupleCount);
      positions.push_back (bitmapPtr);
    }
    return;
  }
  int totalMatchCount = 0;
  for (size_t i = 0; i < _searchRanges.size(); ++i) {
    const PositionRange &range = _searchRanges[i];
    size_t tupleCount = range.end - range.begin;
    boost::shared_ptr<PositionBitmap> bitmapPtr = PositionBitmap::newBitmap(range.begin, tupleCount);
    positions.push_back (bitmapPtr);
    PositionBitmap *bitmap = bitmapPtr.get();

    int firstPageId = range.begin / _entriesPerPage;
    int lastPageId = (range.end - 1) / _entriesPerPage;
    assert (firstPageId < _signature.pageCount);
    assert (lastPageId < _signature.pageCount);
    int matchCount = 0;
    for (int pageId = firstPageId; pageId <= lastPageId; ++pageId) {
      const int64_t tuplePageOffset = pageId * _entriesPerPage;
      const char *page = _bufferpool->readPage(_signature, pageId);
      const FPageHeader *header = reinterpret_cast<const FPageHeader*> (page);
      int64_t begin = 0;
      if (pageId == firstPageId) {
        begin = range.begin - tuplePageOffset;
      }
      if (begin >= header->count) {
        begin = header->count;
      }
      assert (begin >= 0);
      assert (begin < _entriesPerPage);
      assert (begin < header->count);
      int64_t end = _entriesPerPage;
      if (pageId == lastPageId) {
        end = range.end - tuplePageOffset;
      }
      if (end > header->count) {
        end = header->count;
      }
      assert (end >= 0);
      assert (end <= _entriesPerPage);
      assert (end <= header->count);
      assert (end >= begin);
      if (end == begin) continue;

      size_t tupleToRead = end - begin;
      int64_t bitmapPageOffset = tuplePageOffset + begin - range.begin;
      const char *cursor = page + sizeof (FPageHeader) + begin * _dictionaryBits / 8;
      if (_dictionaryBits >= 8) {
        // no bit offset. simple
        assert (_dictionaryBits % 8 == 0);
        if (_dictionaryBits == 8) {
          matchCount += processPageNoBitOffset<uint8_t>(matchingIds, reinterpret_cast<const uint8_t*>(cursor), tupleToRead, bitmap, bitmapPageOffset);
        } else if (_dictionaryBits == 16) {
          matchCount += processPageNoBitOffset<uint16_t>(matchingIds, reinterpret_cast<const uint16_t*>(cursor), tupleToRead, bitmap, bitmapPageOffset);
        } else {
          assert (false); // not supported
        }
      } else {
        int bitOffset = begin * _dictionaryBits % 8;
        matchCount += processPageBitOffset(matchingIds, reinterpret_cast<const uint8_t*>(cursor), bitOffset, tupleToRead, bitmap, bitmapPageOffset);
      }
    }
    bitmap->matchedCount = matchCount;
    totalMatchCount += matchCount;
  }
#ifndef NDEBUG
  watch.stop();
  VLOG(2) << "Dictionary::getPositionBitmaps Done. " << totalMatchCount << " entries matched. " << watch.getElapsed() << " microsec";
#endif // NDEBUG
}

int FColumnReaderImplDictionary::processPageBitOffset(const std::vector<int> &matchingIds, const uint8_t *cursor, int bitOffset, size_t tuplesToRead, PositionBitmap *bitmap, int64_t bitmapPageOffset) {
  assert (matchingIds.size() > 0);
  int matchCount = 0;
  uint8_t mask;
  if (_dictionaryBits == 1) {
    mask = 0x1;
  } else if (_dictionaryBits == 2) {
    mask = 0x3;
  } else if (_dictionaryBits == 4) {
    mask = 0xf;
  } else {
    assert (false);
    return 0;
  }
  // branch to improve performance when matchingIds.size() == 1, which is often.
  if (matchingIds.size() == 1) {
    const uint8_t key = matchingIds[0];
    uint8_t curByte = *cursor;
    for (size_t i = 0; i < tuplesToRead; ++i, bitOffset += _dictionaryBits) {
      assert (bitOffset <= 8);
      if (bitOffset == 8) {
        bitOffset = 0;
        ++cursor;
        curByte = *cursor;
      }
      uint8_t curKey = (curByte >> bitOffset) & mask;
      if (curKey == key) {
        bitmap->setBit(i + bitmapPageOffset);
        ++matchCount;
      }
    }
  } else {
    const size_t s = matchingIds.size();
    uint8_t curByte = *cursor;
    for (size_t i = 0; i < tuplesToRead; ++i, bitOffset += _dictionaryBits) {
      assert (bitOffset <= 8);
      if (bitOffset == 8) {
        bitOffset = 0;
        ++cursor;
        curByte = *cursor;
      }
      uint8_t curKey = (curByte >> bitOffset) & mask;
      for (size_t j = 0; j < s; ++j) {
        if (curKey == matchingIds[j]) {
          bitmap->setBit(i + bitmapPageOffset);
          ++matchCount;
          break;
        }
      }
    }
  }
  return matchCount;
}

void FColumnReaderImplDictionary::getDictionaryCompressedData (const PositionRange &range, void *buffer, size_t bufferSize, int &bitOffset) {
#ifndef NDEBUG
  StopWatch watch;
  watch.init();
#endif // NDEBUG
  size_t tupleCount = range.end - range.begin;
  if (bufferSize < (tupleCount * _dictionaryBits / 8) + (_dictionaryBits % 8 == 0 ? 0 : 2)) {
    assert (false);
    throw std::exception();
  }

  int firstPageId = range.begin / _entriesPerPage;
  int lastPageId = (range.end - 1) / _entriesPerPage;
  bitOffset = 0;
  size_t bufferOffset = 0;
  for (int pageId = firstPageId; pageId <= lastPageId; ++pageId) {
    const int64_t tuplePageOffset = pageId * _entriesPerPage;
    const char *page = _bufferpool->readPage(_signature, pageId);
    const FPageHeader *header = reinterpret_cast<const FPageHeader*> (page);
    int64_t begin = 0;
    if (pageId == firstPageId) {
      begin = range.begin - tuplePageOffset;
    }
    if (begin >= header->count) {
      begin = header->count;
    }
    assert (begin >= 0);
    assert (begin < _entriesPerPage);
    assert (begin < header->count);
    int64_t end = _entriesPerPage;
    if (pageId == lastPageId) {
      end = range.end - tuplePageOffset;
    }
    if (end > header->count) {
      end = header->count;
    }
    assert (end >= 0);
    assert (end <= _entriesPerPage);
    assert (end <= header->count);
    assert (end >= begin);
    if (end == begin) continue;

    if (_dictionaryBits % 8 == 0) {
      size_t tupleToRead = end - begin;
      const char *cursor = page + sizeof (FPageHeader) + begin * _dictionaryBits / 8;
      size_t length = tupleToRead * _dictionaryBits / 8;
      ::memcpy (reinterpret_cast<char*>(buffer) + bufferOffset, cursor, length);
      bufferOffset += length;
    } else {
      const char *copyFrom = page + sizeof (FPageHeader) + begin * _dictionaryBits / 8;
      int copyFromBitOffset = begin * _dictionaryBits % 8;
      if (copyFromBitOffset != 0) {
        // first page could have extra bits before the real data
        assert (pageId == firstPageId);
        bitOffset = copyFromBitOffset;
      }

      const char *copyTo = page + sizeof (FPageHeader) + end * _dictionaryBits / 8;
      int copyToBitOffset = end * _dictionaryBits % 8;
      if (copyToBitOffset != 0) {
        // last page could have extra bits too
        assert (pageId == lastPageId);
        ++copyTo; // copy the last byte
      }

      ::memcpy (reinterpret_cast<char*>(buffer) + bufferOffset, copyFrom, copyTo - copyFrom);
      bufferOffset += (copyTo - copyFrom);
    }
  }
#ifndef NDEBUG
  watch.stop();
  VLOG(2) << "Done. " << tupleCount << " entries read. " << watch.getElapsed() << " microsec";
#endif // NDEBUG
}
int FColumnReaderImplDictionary::getDictionaryEntryId (const void *value) {
  vector<int> entries = searchDictionary (SearchCond(SCT_EQUAL, value));
  if (entries.size() == 0) return -1;
  return entries[0];
}
int FColumnReaderImplDictionary::getDictionaryEntrySizeInBits () {
  return _dictionaryBits;
}
int FColumnReaderImplDictionary::getDictionaryEntryCount () {
  return _signature.dictionaryEntryCount;
}
void FColumnReaderImplDictionary::getAllDictionaryEntries (std::vector<string> &entries) {
  assert (_column.type == COLUMN_CHAR);
#ifndef NDEBUG
  StopWatch watch;
  watch.init();
#endif // NDEBUG
  for (int i = 0; i < _signature.rootPageCount; ++i) {
    const int pageId = i + _signature.rootPageStart;
    const char *page = _bufferpool->readPage(_signature, pageId);
    const FPageHeader *header = reinterpret_cast<const FPageHeader*> (page);
    assert (header->root);
    for (int j = 0; j < header->count; ++j) {
      const char *cursor = page + sizeof (FPageHeader) + j * _column.maxLength;
      entries.push_back (string (cursor, _column.maxLength));
    }
  }
#ifndef NDEBUG
  watch.stop();
  VLOG(2) << "Done. all dictionary entries copied. " << watch.getElapsed() << " microsec";
#endif // NDEBUG
}

// ============================
//  RLE columns
// ============================

FColumnReaderImplRLE::FColumnReaderImplRLE (FBufferPool *bufferpool, const FCStoreColumn &column, const FFileSignature &signature)
  : FColumnReaderImpl(bufferpool, column, signature)  {
}

void FColumnReaderImplRLE::getPositionRanges (const SearchCond &cond, std::vector<PositionRange> &positions) {
  logSearchCond(cond);
#ifndef NDEBUG
  StopWatch watch;
  watch.init();
#endif // NDEBUG
  if (!_searchRangeSet) {
    getPositionRangesFullscan (cond, positions);
  } else {
    for (size_t i = 0; i < _searchRanges.size(); ++i) {
      getPositionRangesPartialScan (cond, positions, _searchRanges[i]);
    }
  }
#ifndef NDEBUG
  watch.stop();
  VLOG(2) << "RLE::getPositionRanges Done. " << positions.size() << " ranges matched. " << watch.getElapsed() << " microsec";
#endif // NDEBUG
}

pair<int, int> FColumnReaderImplRLE::getPageRange (const PositionRange &scanRange) {
  int beginPageId = -1; // the first page which has some tuple in scanRange
  int endPageId = -1; // the first page after beginPage which has no tuple in scanRange
  for (size_t i = 0; i < (size_t) _signature.rootPageCount && endPageId < 0; ++i) {
    int rootPageId = i + _signature.rootPageStart;
    const char *page = _bufferpool->readPage(_signature, rootPageId);
    const FPageHeader *header = reinterpret_cast<const FPageHeader*> (page);
    assert (header->root);
    const char *cursor = page + sizeof(FPageHeader);
    for (int j = 0; j < header->count; ++j, cursor += (sizeof(int64_t) + sizeof(int))) {
      int64_t beginningPos = *reinterpret_cast<const int64_t*> (cursor);
      int pageId = *reinterpret_cast<const int*> (cursor + sizeof(int64_t));
      if (beginningPos <= scanRange.begin) {
        beginPageId = pageId;
      } else if (beginningPos >= scanRange.end) {
        endPageId = pageId;
        break;
      }
    }
  }
  if (beginPageId >= 0 && endPageId < 0) {
    endPageId = beginPageId + 1;
  }
  return pair<int, int>(beginPageId, endPageId);
}
void FColumnReaderImplRLE::getPositionRangesPartialScan (const SearchCond &cond, std::vector<PositionRange> &positions, const PositionRange &scanRange) {
  // first, we look for ranges of pages where
  // beginPage.beginningPos <= scanRange.begin  AND endPage.beginningPos >= scanRange.end
  pair<int, int> pageRange = getPageRange(scanRange);
  int beginPageId = pageRange.first;
  int endPageId = pageRange.second;
  if (beginPageId < 0) {
    return;
  }
  if (endPageId < 0) {
    endPageId = beginPageId + 1; //until the end
  }
  assert (endPageId <= _signature.leafPageCount);

  // then, we read the RLE compressed pages
  PositionRange prevRange;
  bool hasPrevRange = false;
  for (int pageId = beginPageId; pageId < endPageId; ++pageId) {
    const char *page = _bufferpool->readPage(_signature, pageId);
    const FPageHeader *header = reinterpret_cast<const FPageHeader*> (page);
    const char *cursor = page + sizeof(FPageHeader);
    int64_t pos = header->beginningPos;
    for (int j = 0; j < header->count; ++j, cursor += (_column.maxLength + sizeof(int))) {
      int runLength = *reinterpret_cast<const int*> (cursor);
      if (pos + runLength <= scanRange.begin) {
        assert (pageId == beginPageId);
        pos += runLength;
        continue;
      }
      if (pos >= scanRange.end) {
        assert (pageId == endPageId - 1);
        break;
      }
      bool matched;
      if (_column.type == COLUMN_CHAR) {
        matched = cond.matchString(cursor + sizeof(int), _column.maxLength);
      } else {
        matched = cond.matchInts(cursor + sizeof(int), _column.maxLength);
      }
      if (matched) {
        if (hasPrevRange) {
          assert (prevRange.end <= pos);
          if (prevRange.end == pos) {
            prevRange.end = min (pos + runLength, scanRange.end); // connect to previous range
          } else {
            positions.push_back(prevRange);
            prevRange.begin = max (pos, scanRange.begin);
            prevRange.end = min (pos + runLength, scanRange.end);
          }
        } else {
          prevRange.begin = max (pos, scanRange.begin);
          prevRange.end = min (pos + runLength, scanRange.end);
          hasPrevRange = true;
        }
      }
      pos += runLength;
    }
  }
  if (hasPrevRange) {
    positions.push_back(prevRange);
  }
}

void FColumnReaderImplRLE::getPositionRangesFullscan (const SearchCond &cond, std::vector<PositionRange> &positions) {
  // don't need to read root pages in this case. simpler
  PositionRange prevRange;
  bool hasPrevRange = false;
  for (int pageId = 0; pageId < _signature.leafPageCount; ++pageId) {
    const char *page = _bufferpool->readPage(_signature, pageId);
    const FPageHeader *header = reinterpret_cast<const FPageHeader*> (page);
    const char *cursor = page + sizeof(FPageHeader);
    int64_t pos = header->beginningPos;
    for (int j = 0; j < header->count; ++j, cursor += (_column.maxLength + sizeof(int))) {
      int runLength = *reinterpret_cast<const int*> (cursor);
      bool matched;
      if (_column.type == COLUMN_CHAR) {
        matched = cond.matchString(cursor + sizeof(int), _column.maxLength);
      } else {
        matched = cond.matchInts(cursor + sizeof(int), _column.maxLength);
      }
      if (matched) {
        if (hasPrevRange) {
          assert (prevRange.end <= pos);
          if (prevRange.end == pos) {
            prevRange.end = pos + runLength; // connect to previous range
          } else {
            positions.push_back(prevRange);
            prevRange.begin = pos;
            prevRange.end = pos + runLength;
          }
        } else {
          prevRange.begin = pos;
          prevRange.end = pos + runLength;
          hasPrevRange = true;
        }
      }
      pos += runLength;
    }
  }
  if (hasPrevRange) {
    positions.push_back(prevRange);
  }
}

void FColumnReaderImplRLE::getRLECompressedData (const PositionRange &range, void *result) {
  assert (range.begin >= 0);
  assert (range.end >= 0);
  assert (range.begin <= range.end);
#ifndef NDEBUG
  StopWatch watch;
  watch.init();
#endif // NDEBUG

  pair<int, int> pageRange = getPageRange(range);
  int beginPageId = pageRange.first;
  int endPageId = pageRange.second;

  assert (beginPageId < _signature.leafPageCount);
  assert (endPageId <= _signature.leafPageCount);
  int count = 0;
  for (int pageId = beginPageId; pageId < endPageId; ++pageId) {
    const char *page = _bufferpool->readPage(_signature, pageId);
    const FPageHeader *header = reinterpret_cast<const FPageHeader*> (page);
    int64_t pos = header->beginningPos;
    for (int i = 0; i < header->count; ++i) {
      const char *cursor = page + sizeof (FPageHeader) + i * (_column.maxLength + sizeof(int));
      int runLength = *reinterpret_cast<const int*> (cursor);
      if (pos + runLength <= range.begin) {
        assert (pageId == beginPageId);
        pos += runLength;
        continue;
      }
      if (pos >= range.end) {
        assert (pageId == endPageId - 1);
        break;
      }
      ++count;
      const char *data = cursor + sizeof(int);
      PositionRange resultRange(max(range.begin, pos), min(range.end, pos + runLength));
      switch (_column.type) {
      case COLUMN_INT8:
        reinterpret_cast<vector<pair<PositionRange, int8_t> >*> (result)->push_back (pair<PositionRange, int8_t>(resultRange, *reinterpret_cast<const int8_t*>(data)));
        break;
      case COLUMN_INT16:
        reinterpret_cast<vector<pair<PositionRange, int16_t> >*> (result)->push_back (pair<PositionRange, int16_t>(resultRange, *reinterpret_cast<const int16_t*>(data)));
        break;
      case COLUMN_INT32:
        reinterpret_cast<vector<pair<PositionRange, int32_t> >*> (result)->push_back (pair<PositionRange, int32_t>(resultRange, *reinterpret_cast<const int32_t*>(data)));
        break;
      case COLUMN_INT64:
        reinterpret_cast<vector<pair<PositionRange, int64_t> >*> (result)->push_back (pair<PositionRange, int64_t>(resultRange, *reinterpret_cast<const int64_t*>(data)));
        break;
      case COLUMN_CHAR:
        reinterpret_cast<vector<pair<PositionRange, string> >*> (result)->push_back (pair<PositionRange, string>(resultRange, string(data, _column.maxLength)));
        break;
      default:
        assert (false);
      }
      pos += runLength;
    }
  }

#ifndef NDEBUG
  watch.stop();
  VLOG(2) << "RLE::getRLECompressedData Done. " << count << " runs read. " << watch.getElapsed() << " microsec";
#endif // NDEBUG
}

// ============================
//  Bitmap operations
// ============================
PositionBitmap::PositionBitmap (int64_t beginPosition_, size_t bitLength_) {
  beginPosition = beginPosition_;
  bitLength = bitLength_;
  byteLength = (bitLength / 8) + (bitLength % 8 == 0 ? 0 : 1);
  bitmap = new unsigned char[byteLength];
  ::memset (bitmap, 0, byteLength);
  matchedCount = 0;
}
PositionBitmap::~PositionBitmap() {
  delete[] bitmap;
}
boost::shared_ptr<PositionBitmap> PositionBitmap::newBitmap (int64_t beginPosition_, size_t bitLength_) {
  return boost::shared_ptr<PositionBitmap>(new PositionBitmap(beginPosition_, bitLength_));
}

} // fdb
