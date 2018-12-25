////===================================================================
 //
 // ob_log_binlog_record.cpp liboblog / Oceanbase
 //
 // Copyright (C) 2013 Alipay.com, Inc.
 //
 // Created on 2013-12-26 by XiuMing (wanhong.wwh@alibaba-inc.com)
 //
 // -------------------------------------------------------------------
 //
 // Description
 //   ObLogBinlogRecord implementation file
 //
 // -------------------------------------------------------------------
 //
 // Change Log
 //
////====================================================================

#include "ob_log_binlog_record.h"
#include "ob_log_dml_info.h"          // ObLogDmlStmt
#include "ob_log_filter.h"            // ObLogMutator
#include <MD.h>                       // ITableMeta

using namespace oceanbase::common;

namespace oceanbase
{
  namespace liboblog
  {
    const std::string ObLogBinlogRecord::BINLOG_RECORD_TYPE = DRCMessageFactory::DFT_BR;

    int ObLogBinlogRecord::init_heartbeat(int64_t timestamp, bool query_back)
    {
      int ret = OB_SUCCESS;

      if (OB_LOG_INVALID_TIMESTAMP == timestamp)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      if (NULL == data_)
      {
        TBSYS_LOG(WARN, "IBinlogRecord has not been created");
        ret = OB_NOT_INIT;
      }
      else
      {
        int src_category = get_src_category_(query_back);
        init_(0, HEARTBEAT, src_category, timestamp, 0, true, NULL, 0);
      }

      return ret;
    }

    int ObLogBinlogRecord::init_trans_barrier(const RecordType type,
        const ObLogMutator &mutator,
        bool query_back,
        uint64_t br_index_in_trans)
    {
      int ret = OB_SUCCESS;

      if (NULL == data_)
      {
        TBSYS_LOG(WARN, "IBinlogRecord has not been created");
        ret = OB_NOT_INIT;
      }
      else if (EUNKNOWN == type || ! mutator.is_dml_mutator())
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        uint64_t id = mutator.get_num();
        int src_category = get_src_category_(query_back);
        uint64_t checkpoint = mutator.get_log_id();
        int64_t timestamp = mutator.get_mutate_timestamp();
        bool first_in_log_event = true;
        ITableMeta *table_meta = NULL;

        init_(id, type, src_category, timestamp, checkpoint, first_in_log_event, table_meta, br_index_in_trans);
      }

      return ret;
    }

    int ObLogBinlogRecord::init_dml(const ObLogDmlStmt *dml_stmt, bool query_back)
    {
      int ret = OB_SUCCESS;

      if (NULL == dml_stmt || ! dml_stmt->is_inited())
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == data_)
      {
        TBSYS_LOG(WARN, "IBinlogRecord has not been created");
        ret = OB_NOT_INIT;
      }
      else
      {
        uint64_t id = dml_stmt->id_;
        RecordType type = get_record_type_(dml_stmt->dml_type_);
        int src_category = get_src_category_(query_back);
        int64_t timestamp = dml_stmt->timestamp_;
        uint64_t checkpoint = dml_stmt->checkpoint_;
        bool first_in_log_event = dml_stmt->get_first_in_log_event();
        ITableMeta *table_meta = dml_stmt->table_meta_;
        uint64_t br_index_in_trans = dml_stmt->stmt_index_in_trans_;

        init_(id, type, src_category, timestamp, checkpoint, first_in_log_event, table_meta, br_index_in_trans);
      }

      return ret;
    }

    void ObLogBinlogRecord::init_(uint64_t id,
        RecordType type,
        int src_category,
        int64_t timestamp,
        uint64_t checkpoint,
        bool first_in_log_event,
        ITableMeta *table_meta,
        uint64_t br_index_in_trans)
    {
      setSrcType(SRC_OCEANBASE);
      set_next(NULL);

      setId(id);
      setRecordType(type);
      setTableMeta(table_meta);
      setSrcCategory(src_category);
      setTimestamp(timestamp/1000000);
      setFirstInLogevent(first_in_log_event);

      setCheckpoint(checkpoint, br_index_in_trans);

      if (NULL != table_meta)
      {
        setDbname(table_meta->getDBMeta()->getName());
        setTbname(table_meta->getName());
      }

      // FIXME: Set precise timestamp for correctness testing
      set_precise_timestamp(timestamp);

      // TODO: Call setInstance() to set data source IP:PORT
    }

    RecordType ObLogBinlogRecord::get_record_type_(const ObDmlType dml_type) const
    {
      RecordType record_type = EUNKNOWN;

      // Set record type
      switch (dml_type)
      {
        case OB_DML_REPLACE:
          record_type = EREPLACE;
          break;

        case OB_DML_INSERT:
          record_type = EINSERT;
          break;

        case OB_DML_UPDATE:
          record_type = EUPDATE;
          break;

        case OB_DML_DELETE:
          record_type = EDELETE;
          break;

        default:
          record_type = EUNKNOWN;
          break;
      }

      return record_type;
    }
  } // end namespace liboblog
} // end namespace oceanbase
