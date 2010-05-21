#ifndef SSB_QUERYSSBIMPL_H
#define SSB_QUERYSSBIMPL_H

#include "queryssb.h"
#include <log4cxx/logger.h>

namespace fdb {

#define BTREE_MV_MAIN_FILENAME "mvprojection.db"
#define BTREE_MV_FAMILY "mvprojection.btree"
#define CSTORE_MV_MAIN_PREFIX "mvprojection"
#define CSTORE_MV_FAMILY "mvprojection.cstore"

class FEngine;
class FBufferPool;
class FSignatureSet;
class MVProjection;
typedef void (*BtreeMVSearchCallback) (void *context, const MVProjection *tuple);
class SSBQueryExecutorImpl {
public:
  SSBQueryExecutorImpl (FEngine *engine);
  FEngine* getEngine();

  boost::shared_ptr<SSBQueryResult> query (int query, bool cstore, const SSBQueryParam &param);

  void btreeMVSearchYear (BtreeMVSearchCallback callback, void* context, int year);
  void btreeMVSearchYearMainMemory (const std::string &familyName, BtreeMVSearchCallback callback, void* childContext, int year);

  void btreeMVSearchSRegion (BtreeMVSearchCallback callback, void* context, const std::string &region);
  void btreeMVSearchSRegionMainMemory (const std::string &familyName, BtreeMVSearchCallback callback, void* childContext, const std::string &region);

  boost::shared_ptr<SSBQueryResult> query11B (const SSBQueryParam &param);
  boost::shared_ptr<SSBQueryResult> query11C (const SSBQueryParam &param);

  boost::shared_ptr<SSBQueryResult> query12B (const SSBQueryParam &param);
  boost::shared_ptr<SSBQueryResult> query12C (const SSBQueryParam &param);

  boost::shared_ptr<SSBQueryResult> query13B (const SSBQueryParam &param);
  boost::shared_ptr<SSBQueryResult> query13C (const SSBQueryParam &param);

  boost::shared_ptr<SSBQueryResult> query21B (const SSBQueryParam &param);
  boost::shared_ptr<SSBQueryResult> query21C (const SSBQueryParam &param);

  boost::shared_ptr<SSBQueryResult> query22B (const SSBQueryParam &param);
  boost::shared_ptr<SSBQueryResult> query22C (const SSBQueryParam &param);

  boost::shared_ptr<SSBQueryResult> query23B (const SSBQueryParam &param);
  boost::shared_ptr<SSBQueryResult> query23C (const SSBQueryParam &param);

  FEngine *_engine;
  std::string _dataFolder;
  FBufferPool *_bufferpool;
  const FSignatureSet &_signatures;
  log4cxx::LoggerPtr _logger;
};

} //fdb

#endif //SSB_QUERYSSBIMPL_H
