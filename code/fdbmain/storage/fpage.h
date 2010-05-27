#ifndef STORAGE_FPAGE_H
#define STORAGE_FPAGE_H

#define MAGIC_NUMBER (0x287f4ac5)

namespace fdb {

// the fixed-size header placed at the beginning of every page.
// the format of data page is
// btree leaf page: <page header><tuple><tuple><tuple>...
// btree non-leaf page (could be multi-level): <page header><firstkey><its pageid><firstkey><its pageid>...
// cstore uncompressed leaf page: <page header><value><value><value><value>... (no root pages)
// cstore RLE leaf page: <page header><count><value><count><value>...
// cstore RLE root page (always one-level): <page header><beginpos><pageid><beginpos><pageid>...
// cstore Dict leaf page: <page header><valueid><valueid><valueid>...
// cstore Dict root page (always one-level): <page header><value><value><value>...
struct FPageHeader {
  int magicNumber; // to check sanity
  int fileId;
  int pageId;
  int64_t beginningPos; // accumulated position (tuple id) of the first entry in this page
  int level; // 0=leaf
  bool root; // true if this is a root node.
  int entrySize; // byte size of one entry in this page (tuplesize in BTree leaf, key+sizeof(int) in BTree non-leaf)
  int count; // number of tuples/keys in this page
  bool lastSibling; // true if this page is the last of this level
};
}
#endif //STORAGE_FPAGE_H