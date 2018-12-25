/*
 * =====================================================================================
 *
 *       Filename:  OceanbaseDb.cpp
 *
 *        Version:  1.0
 *        Created:  04/13/2011 09:55:53 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */

#include "oceanbase_db.h"
#include "common/ob_packet_factory.h"
#include "common/ob_base_client.h"
#include "common/ob_server.h"
#include "common/ob_string.h"
#include "common/ob_scanner.h"
#include "common/ob_result.h"
#include "common/utility.h"
#include "common/ob_crc64.h"
#include "common/ob_define.h"
#include "common/serialization.h"
#include "common/ob_tsi_factory.h"
#include "common/ob_malloc.h"
#include "common/ob_errno.h"
#include "db_record_set.h"
#include "db_table_info.h"
#include "db_record.h"
#include "ob_api_util.h"
#include "sys/time.h"

#include <string>

const int64_t kMaxLogSize = 256 * 1024 * 1024;  /* log file size 256M */
const int64_t kMaxApplyRetries = 5;
const int kMaxColumns = 50;

//add by zcd :beg
const int32_t DefaultAPIVersion = 1;
const int32_t MAX_THREAD_BUFFER_SIZE = 2*1024*1024L; /* 线程私有最大缓冲区大小 2MB*/
const int64_t PACKET_TIMEOUT_US = 60000000; /* 发送一个数据包请求时设定的超时时间 60s */
const int kDefaultResultBufferSize = 2 * 1024 * 1024; //一个ob_packet的缓冲区大小
const int64_t MAX_TIMEOUT_US = /*25 * 60 * 1000 * 1000*/ 30L * 60 * 1000 * 1000; //add by zhuxh [timeout mechanism] 20170512 //Now it means all time for a tablet to export
const int64_t SPAN_TIMEOUT_US = 2 * 60 * 1000 * 1000; //2分钟,每次重试递增的时间

//:e

using namespace oceanbase::common;

namespace oceanbase {
  using namespace common;
  using namespace sql;
  namespace api {
    RowMutator::RowMutator()
    {
    }
    RowMutator::RowMutator(const std::string &table_name, const ObRowkey &rowkey, DbTranscation *tnx)
      :table_name_(table_name), rowkey_(rowkey), tnx_(tnx)
    {
      op_ = ObActionFlag::OP_UPDATE;
    }

    int RowMutator::delete_row()
    {
      ObMutatorCellInfo mutation;
      mutation.cell_info.table_name_.assign_ptr(const_cast<char *>(table_name_.c_str()), 
          static_cast<int32_t>(table_name_.length()));
      mutation.cell_info.row_key_ = rowkey_;
      mutation.op_type.set_ext(ObActionFlag::OP_UPDATE);
      mutation.cell_info.value_.set_ext(ObActionFlag::OP_DEL_ROW);
      int ret = tnx_->mutator_.add_cell(mutation);
      return ret;
    }

    int RowMutator::add(const char *column_name, const ObObj &val)
    {
      ObMutatorCellInfo mutation;

      mutation.cell_info.table_name_.assign_ptr(const_cast<char *>(table_name_.c_str()), 
          static_cast<int32_t>(table_name_.length()));
      mutation.cell_info.row_key_ = rowkey_;
      mutation.cell_info.column_name_.assign_ptr(const_cast<char *>(column_name), 
          static_cast<int32_t>(strlen(column_name)));

      mutation.op_type.set_ext(op_);
      mutation.cell_info.value_ = val;
      int ret = tnx_->mutator_.add_cell(mutation);

      return ret;
    }

    int RowMutator::add(const std::string &column_name, const ObObj &val)
    {
      return add(column_name.c_str(), val);
    }

    void RowMutator::set(const std::string &table_name, const ObRowkey &rowkey, 
            DbTranscation *tnx)
    {
      table_name_ = table_name;
      rowkey_ = rowkey;
      tnx_ = tnx;
    }

    DbTranscation::DbTranscation() : db_(NULL)
    {
    }

    DbTranscation::DbTranscation(OceanbaseDb *db) : db_(db)
    {
      assert(db_ != NULL);
    }

    int DbTranscation::reset()
    {
      return mutator_.reset();
    }

    int DbTranscation::insert_mutator(const char* table_name, const ObRowkey& rowkey, RowMutator *mutator)
    {
      int ret = OB_SUCCESS;
      std::string table = table_name;
      mutator->set(table_name, rowkey, this);
      mutator->set_op(ObActionFlag::OP_INSERT);
      return ret;
    }

    int DbTranscation::update_mutator(const char* table_name, const ObRowkey &rowkey, RowMutator *mutator)
    {
      int ret = OB_SUCCESS;
      std::string table = table_name;
      mutator->set(table_name, rowkey, this);
      return ret;
    }

    int DbTranscation::free_row_mutator(RowMutator *&mutator)
    {
      delete mutator;
      mutator = NULL;

      return OB_SUCCESS;
    }

    int DbTranscation::commit()
    {
      int ret = OB_SUCCESS;
      ObMemBuf *mbuff = GET_TSI_MULT(ObMemBuf, TSI_MBUF_ID);

#if 0
      {
        TBSYS_LOG(INFO, "dumping obmutator, mutator.size = %ld", mutator_.size());
        mutator_.reset_iter();
        while (mutator_.next_cell() == OB_SUCCESS)
        {
          ObMutatorCellInfo *cell = NULL;
          if (mutator_.get_cell(&cell) == OB_SUCCESS)
          {
            char buf[256];
            int64_t len = cell->cell_info.value_.to_string(buf, 256);
            buf[len] = 0;
            ObString &table_name = cell->cell_info.table_name_;
            ObString &column = cell->cell_info.column_name_;
            TBSYS_LOG(INFO, "table[%.*s] column[%.*s]: value: %s", table_name.length(), table_name.ptr(), 
                column.length(), column.ptr(), buf);
          }
        }
      }
#endif

      if (mbuff == NULL || mbuff->ensure_space(OB_MAX_PACKET_LENGTH) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "can't allocate from TSI");
        ret = OB_ERROR;
      }

      if (ret == OB_SUCCESS)
      {
        ObDataBuffer buffer(mbuff->get_buffer(), mbuff->get_buffer_size());

        ObDataBuffer tmp_buffer = buffer;
        ret = mutator_.serialize(buffer.get_data(), buffer.get_capacity(), buffer.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "can't serialize mutator, due to %d. mutator size[%ld]", ret, mutator_.size());
        }
        else
        {
          int64_t retries = 0;
          TBSYS_LOG(DEBUG, "mutator serialize size[%ld]", buffer.get_position());

          while (true) 
          {
            if (retries > 2) //retry 2 times
            {
              TBSYS_LOG(ERROR, "fail to write to ups[%d]", ret);//mod by liyongfeng: modify WARN into ERROR
              break;
            }
            int64_t pos = 0;
            ret = db_->do_server_cmd(db_->update_server_, OB_WRITE, buffer, pos);
            if (OB_ALLOCATE_MEMORY_FAILED == ret || OB_MEM_OVERFLOW == ret)//mod by liyongfeng for UPS memory overflow
			{
				sleep(60);// add by liyongfeng: if ups memory is not enough, ob_import sleep 60s and then retry, but this retry is not added into retries.
				TBSYS_LOG(WARN, "Updateserver memory is not enough, sleep 60s and wait ups release memory, then retry, ret=%d", ret);
				buffer = tmp_buffer;
				int err = mutator_.serialize(buffer.get_data(), buffer.get_capacity(), buffer.get_position());
				if(err != OB_SUCCESS) {
					TBSYS_LOG(ERROR, "can't serialize mutator, due to %d", err);
					ret = err;
					break;
				}
			}
			else if (ret != OB_SUCCESS)// add by liyongfeng: if other errors occur, then ob_import sleep 5s and retry, only retry 2 times.
			{
				retries++;
				TBSYS_LOG(WARN, "Write to OB failed, ret=%d retry_time=%ld", ret, retries); // mod by liyongfeng: modify ERROR into WARN.
				sleep(5);//mod:e
				buffer = tmp_buffer;
				int err = mutator_.serialize(buffer.get_data(), buffer.get_capacity(), buffer.get_position());
				if (err != OB_SUCCESS) {
					TBSYS_LOG(ERROR, "can't serialize mutator, due to %d", err);
					ret = err;
					break;
				}
            }
            else
            {
              break;
            }

            TBSYS_LOG(WARN, "retring write to database, buffer size = %ld", pos);
          }
        }

        if (ret != OB_SUCCESS)
        {
          char server_buf[128];
          db_->update_server_.to_string(server_buf, 128);
          TBSYS_LOG(ERROR, "can't write to update server, due to [%d], server:%s", ret, server_buf);
        }
      }

      return ret;
    }

    int DbTranscation::abort()
    {
      int ret = OB_SUCCESS;
      return ret;
    }

    int DbTranscation::add_cell(ObMutatorCellInfo &cell)
    {
      return mutator_.add_cell(cell);
    }

    OceanbaseDb::OceanbaseDb(const char *ip, unsigned short port, 
                             int64_t timeout, uint64_t tablet_timeout):
      arr_slave_client_length(0),//add by zhuxh:20160120
      rpc_buffer_(k2M) //add by zhuxh:20160712 [New way of fetching schema]
    {            
      inited_ = false;
      root_server_.set_ipv4_addr(ip, port);
      timeout_ = timeout;
      tablet_timeout_ = tablet_timeout;
      TBSYS_LOG(INFO, "%s:%d, timeout is %ld, tablet_timeout is %ld", ip, port, timeout_, tablet_timeout_);
      db_ref_ = 1;
      consistency_ = true;
      limit_count_ = limit_offset_ = 0;
      // add by zcd :b
      pthread_key_create(&send_sql_param_key_, destroy_ts_execute_sql_param_key);
      // :end
    }

    OceanbaseDb::~OceanbaseDb()
    {
      free_tablet_cache();

      assert(db_ref_ == 1);
    }

    int OceanbaseDb::set_limit_info(const int64_t offset, int64_t count)
    {
      int err = OB_SUCCESS;
      if (offset < 0 || count < 0)
      {
        TBSYS_LOG(WARN,"set limit info error [offset:%ld,count:%ld]", offset, count);
        err = OB_ERROR;
      }
      if (OB_SUCCESS == err)
      {
        limit_offset_ = offset;
        limit_count_ = count;
      }
      return err;
    }

    int OceanbaseDb::get(const std::vector<DbMutiGetRow> &rows, DbRecordSet &rs)
    {
      int ret = OB_SUCCESS;
      common::ObServer server;

      if (rows.empty()) {
        return ret;
      }

      ObRowkey rowkey = rows[0].rowkey;
      const std::string &table = rows[0].table;

      if (!rs.inited()) {
        TBSYS_LOG(INFO, "DbRecordSet Not init ,please init it first");
        ret = common::OB_ERROR;
      }

      int retries = kTabletDupNr;

      while (retries--) {
        ret = get_tablet_location(table, rowkey, server);
        if (ret != common::OB_SUCCESS) {
          TBSYS_LOG(ERROR, "No Mergeserver available");
          break;
        }
        char server_str[128];
        server.to_string(server_str, 128);

        ret = do_muti_get(server, rows, rs.get_scanner(), rs.get_buffer());
        if (ret == OB_SUCCESS) {
          TBSYS_LOG(DEBUG, "using merger server %s", server_str);
          break;
        } else {
          TBSYS_LOG(WARN, "failed when get data from %s", server_str);
          if (ret == OB_ERROR_OUT_OF_RANGE) {
            TBSYS_LOG(ERROR, "rowkey out of range");
            break;
          }

          mark_ms_failure(server, table, rowkey);
        } 
      }

      if (ret == OB_SUCCESS) {
        __sync_add_and_fetch(&db_stats_.total_succ_gets, 1);
      } else {
        __sync_add_and_fetch(&db_stats_.total_fail_gets, 1);
      }

      return ret;

    }

    int OceanbaseDb::get(const std::string &table,const std::vector<std::string> &columns, 
        const std::vector<ObRowkey> &rowkeys, DbRecordSet &rs)
    {
      int ret = OB_SUCCESS;
      common::ObServer server;
      ObRowkey rowkey;

      if (rowkeys.empty()) {
        return ret;
      }

      rowkey = rowkeys[0];

      if (!rs.inited()) {
        TBSYS_LOG(INFO, "DbRecordSet Not init ,please init it first");
        ret = common::OB_ERROR;
      }

      int retries = kTabletDupNr;

      std::vector<DbMutiGetRow> rows;
      for(size_t i = 0;i < rowkeys.size(); i++) {
        DbMutiGetRow row;
        row.table = table;
        row.rowkey = rowkeys[i];
        row.columns = &columns;

        rows.push_back(row);
      }

      while (retries--) {
        ret = get_tablet_location(table, rowkey, server);
        if (ret != common::OB_SUCCESS) {
          TBSYS_LOG(ERROR, "No Mergeserver available");
          break;
        }
        char server_str[128];
        server.to_string(server_str, 128);

        ret = do_muti_get(server, rows, rs.get_scanner(), rs.get_buffer());
        if (ret == OB_SUCCESS) {
          TBSYS_LOG(INFO, "using mergeserver %s", server_str);
          break;
        } else {
          TBSYS_LOG(WARN, "failed when get data from %s", server_str);
          if (ret == OB_ERROR_OUT_OF_RANGE) {
            TBSYS_LOG(ERROR, "rowkey out of range");
            break;
          }

          mark_ms_failure(server, table, rowkey);
        } 
      }

      if (ret == OB_SUCCESS) {
        __sync_add_and_fetch(&db_stats_.total_succ_gets, 1);
      } else {
        __sync_add_and_fetch(&db_stats_.total_fail_gets, 1);
      }

      return ret;

    }

    int OceanbaseDb::get(const std::string &table,const std::vector<std::string> &columns, 
        const ObRowkey &rowkey, DbRecordSet &rs)
    {
      int ret = OB_SUCCESS;
      common::ObServer server;

      if (!rs.inited()) {
        TBSYS_LOG(INFO, "DbRecordSet Not init ,please init it first");
        ret = common::OB_ERROR;
      }

      int retries = kTabletDupNr;

      while (retries--) {
        ret = get_tablet_location(table, rowkey, server);
        if (ret != common::OB_SUCCESS) {
          TBSYS_LOG(ERROR, "No Mergeserver available");
          break;
        }

        ret = do_server_get(server, rowkey, rs.get_scanner(), rs.get_buffer(), table, columns);
        if (ret == OB_SUCCESS) {
          break;
        } else {
          char err_msg[128];

          server.to_string(err_msg, 128);
          TBSYS_LOG(WARN, "failed when get data from %s", err_msg);
          if (ret == OB_ERROR_OUT_OF_RANGE) {
            TBSYS_LOG(ERROR, "rowkey out of range");
            break;
          }

          mark_ms_failure(server, table, rowkey);
        } 
      }

      if (ret == OB_SUCCESS) {
        __sync_add_and_fetch(&db_stats_.total_succ_gets, 1);
      } else {
        __sync_add_and_fetch(&db_stats_.total_fail_gets, 1);
      }

      return ret;
    }

    //mod by zhuxh:20160118
    //add checking merge state of slave cluster
    int OceanbaseDb::init()
    {
      client_.initialize(root_server_);
      inited_ = true;

      ObServer server;
      int ret = OB_SUCCESS;
      if ( OB_SUCCESS != ( ret = get_update_server(server) ) )
      {
        TBSYS_LOG(ERROR, "can't get update server");
      }
      //add by zhuxh:20160122 :b
      else if ( OB_SUCCESS != (ret = init_slave_cluster_info() ) )
      {
        TBSYS_LOG(ERROR, "Can't get slave clusters' info.");
      }
      //add zhuxh:e
      return ret;
    }

    int OceanbaseDb::global_init(const char *log_dir, const char * level)
    {
      UNUSED(log_dir);
      ob_init_memory_pool();
      //TBSYS_LOGGER.setFileName(log_dir);
      TBSYS_LOGGER.setMaxFileSize(kMaxLogSize);
      TBSYS_LOGGER.setLogLevel(level); 
      ob_init_crc64_table(OB_DEFAULT_CRC64_POLYNOM);
      TBSYS_LOG(INFO, "init oceanbase global config");
      return OB_SUCCESS;
    }

    int OceanbaseDb::search_tablet_cache(const std::string &table, const ObRowkey &rowkey, TabletInfo &loc)
    {
      tbsys::CThreadGuard guard(&cache_lock_);
      CacheSet::iterator itr = cache_set_.find(table);
      if (itr == cache_set_.end())
        return OB_ERROR;

      CacheRow::iterator row_itr = itr->second.lower_bound(rowkey);
      if (row_itr == itr->second.end())
        return OB_ERROR;

      //remove timeout tablet
      if (row_itr->second.expired(tablet_timeout_)) {
        itr->second.erase(row_itr);
        return OB_ERROR; 
      }

      loc = row_itr->second;
      return OB_SUCCESS;
    }

    void OceanbaseDb::try_mark_server_fail(TabletInfo &tablet_info, ObServer &server, bool &do_erase_tablet)
    {
      int i;
      for(i = 0; i < kTabletDupNr; i++) {
        if (tablet_info.slice_[i].ip_v4 == static_cast<int32_t>(server.get_ipv4()) && 
            tablet_info.slice_[i].ms_port == server.get_port()) {
          tablet_info.slice_[i].server_avail = false;
          break;
        }
      }

      //check wether tablet is available, if not delete it from set
      for(i = 0; i < kTabletDupNr ; i++) {
        if (tablet_info.slice_[i].server_avail == true) {
          break; 
        }
      }

      if (i == kTabletDupNr) {
        do_erase_tablet = true;
      } else {
        do_erase_tablet = false;
      }
    }

    void OceanbaseDb::mark_ms_failure(ObServer &server, const std::string &table, const ObRowkey &rowkey)
    {
      int ret = OB_SUCCESS;
      CacheRow::iterator row_itr;

      tbsys::CThreadGuard guard(&cache_lock_);

      CacheSet::iterator itr = cache_set_.find(table);
      if (itr == cache_set_.end())
        ret = OB_ERROR;

      if (ret == OB_SUCCESS) {
        row_itr = itr->second.lower_bound(rowkey);
        if (row_itr == itr->second.end())
          ret = OB_ERROR;
      }

      if (ret == OB_SUCCESS) {
        TabletInfo &tablet_info = row_itr->second;
        bool do_erase_tablet = false;

        try_mark_server_fail(tablet_info, server, do_erase_tablet);
        if (do_erase_tablet)
          itr->second.erase(row_itr);
      } else {
        TBSYS_LOG(WARN, "tablet updated, no such rowkey");
      }
    }

    int OceanbaseDb::get_tablet_location(const std::string &table, const ObRowkey &rowkey, 
        common::ObServer &server)
    {
      int ret = OB_SUCCESS;
      TabletInfo tablet_info;
      TabletSliceLocation loc;

      ret = search_tablet_cache(table, rowkey, tablet_info);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(DEBUG, "Table %s not find in tablet cache, do root server get", table.c_str());

        ret = get_ms_location(rowkey, table);
        if (ret == OB_SUCCESS)
          ret = search_tablet_cache(table, rowkey, tablet_info);
        else {
          TBSYS_LOG(WARN, "get_ms_location faild , retcode:[%d]", ret);
        }
      }

      if (ret == common::OB_SUCCESS) {
        ret = tablet_info.get_one_avail_slice(loc, rowkey);
        if (ret == common::OB_SUCCESS) {
          server.set_ipv4_addr(loc.ip_v4, loc.ms_port);
        } else {
          TBSYS_LOG(ERROR, "No available Merger Server");
        }
      }

      return ret;
    }

    int OceanbaseDb::get_ms_location(const ObRowkey &row_key, const std::string &table_name)
    {
      int ret = OB_SUCCESS;
      ObScanner scanner;
      std::vector<std::string> columns;
      columns.push_back("*");
      DbRecordSet ds;

      if (!inited_) {
        TBSYS_LOG(ERROR, "OceanbaseDb is not Inited, plesase Initialize it first");
        ret = OB_ERROR;
      } else if ((ret = ds.init()) != common::OB_SUCCESS) {
        TBSYS_LOG(INFO, "DbRecordSet Init error");
      }

      if (ret == OB_SUCCESS) {
        int retires = kTabletDupNr;

        while(retires--) {
          ret = do_server_get(root_server_, row_key, ds.get_scanner(), 
              ds.get_buffer(), table_name, columns);
          if (ret == OB_SUCCESS)
            break;
        }

        if (ret != OB_SUCCESS) {
          TBSYS_LOG(ERROR, "do server get failed, ret=[%d]", ret);
        }
      }

      if (ret == OB_SUCCESS) {
        //        dump_scanner(ds.get_scanner());
        DbRecordSet::Iterator itr = ds.begin();
        while (itr != ds.end()) {
          DbRecord *recp;
          TabletInfo tablet_info;

          itr.get_record(&recp);
          if (recp == NULL) {
            TBSYS_LOG(WARN, "NULL record skip line");
            itr++;
            continue;
          }

          ret = tablet_info.parse_from_record(recp);
          if (ret != OB_SUCCESS) {
            TBSYS_LOG(ERROR, "pase from record failed");
            break;
          }

          insert_tablet_cache(table_name, tablet_info.get_end_key(), tablet_info);
          itr++;
        }
      }

      return ret;
    }


    void OceanbaseDb::insert_tablet_cache (const std::string &table, const ObRowkey &rowkey, TabletInfo &tablet)
    {
      tbsys::CThreadGuard guard(&cache_lock_);

      CacheSet::iterator set_itr = cache_set_.find(table);
      if (set_itr == cache_set_.end()) {
        CacheRow row;

        row.insert(CacheRow::value_type(rowkey, tablet));
        cache_set_.insert(CacheSet::value_type(table, row));
      } else {
        CacheRow::iterator row_itr = set_itr->second.find(rowkey);
        if (row_itr != set_itr->second.end()) {
          set_itr->second.erase(row_itr);
          TBSYS_LOG(DEBUG, "deleting cache table is %s", table.c_str());
        } 

        TBSYS_LOG(DEBUG, "insert cache table is %s", table.c_str());
        set_itr->second.insert(std::make_pair(rowkey, tablet));
      }
    }		/* -----  end of method OceanbaseDb::insert_tablet_cache  ----- */

    void OceanbaseDb::free_tablet_cache( )
    {
      CacheSet::iterator set_itr = cache_set_.begin();
      while (set_itr != cache_set_.end()) {
        CacheRow::iterator row_itr = set_itr->second.begin();
        while (row_itr != set_itr->second.end()) {
          row_itr++;
        }
        set_itr++;
      }
    }		/* -----  end of method OceanbaseDb::free  ----- */

    int OceanbaseDb::do_muti_get(common::ObServer &server, const std::vector<DbMutiGetRow>& rows, 
        ObScanner &scanner, ObDataBuffer& data_buff)
    {
      ObGetParam *get_param = NULL;
      int ret = init_get_param(get_param, rows);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(WARN, "can't init get_param");
      }

      if (ret == OB_SUCCESS) {
        data_buff.get_position() = 0;
        ret = get_param->serialize(data_buff.get_data(), data_buff.get_capacity(),
            data_buff.get_position());
        if (ret != OB_SUCCESS) {
          TBSYS_LOG(WARN, "can't deserialize get param");
        }
      }

#if 0
      for(int64_t i = 0;i < get_param->get_cell_size(); i++) {
        char buf[128];

        int len = hex_to_str((*get_param)[i]->row_key_.ptr(), (*get_param)[i]->row_key_.length(), buf ,128);
        buf[2 * len] = 0;

        std::string table_name = 
          std::string((*get_param)[i]->table_name_.ptr(), (*get_param)[i]->table_name_.length());
        std::string column_name =
          std::string((*get_param)[i]->column_name_.ptr(), (*get_param)[i]->column_name_.length());

        TBSYS_LOG(INFO, "tablet=%s,Column=%s, Rowkey=%s", table_name.c_str(), column_name.c_str(), buf);
        (*get_param)[i]->value_.dump(TBSYS_LOG_LEVEL_INFO);
      }
#endif
#if 0
      {
        char buf[128];
        server.to_string(buf, 128);
        TBSYS_LOG(INFO, "Assessing Merge server, %s", buf);
      }
#endif

      if (ret == OB_SUCCESS) {
        int64_t pos = 0;

        ret = do_server_cmd(server, (int32_t)OB_GET_REQUEST, data_buff, pos);
        if (ret != OB_SUCCESS) {
          TBSYS_LOG(WARN, "can't do server cmd, ret=%d", ret);
        } else {
          scanner.clear();

          ret = scanner.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
          if (ret != OB_SUCCESS) {
            TBSYS_LOG(WARN, "ob scanner can't deserialize buffer, pos=%ld, ret=%d",pos, ret);
          }
        }
      }

      return ret;
    }

    int OceanbaseDb::init_get_param(ObGetParam *&param, const std::vector<DbMutiGetRow> &rows)
    {
      ObCellInfo cell;

      ObGetParam *get_param = GET_TSI_MULT(ObGetParam, TSI_GET_ID);
      if (get_param == NULL) {
        TBSYS_LOG(ERROR, "can't allocate memory from TSI");
        return OB_ERROR;
      }

      get_param->reset();

      ObBorderFlag border;
      border.set_inclusive_start();
      border.set_inclusive_end();
      border.set_max_value();
      border.set_min_value();

      ObVersionRange version_range;

      version_range.start_version_ = 0;
      version_range.end_version_ = 0;

      version_range.border_flag_ = border;
      get_param->set_version_range(version_range);
      param = get_param;
      param->set_is_read_consistency(consistency_);

      for(size_t i = 0;i < rows.size(); i++) {
        std::vector<std::string>::const_iterator itr = rows[i].columns->begin();
        cell.row_key_ = rows[i].rowkey;
        cell.table_name_.assign_ptr(const_cast<char *>(rows[i].table.c_str()), static_cast<int32_t>(rows[i].table.length()));

        while(itr != rows[i].columns->end()) {
          cell.column_name_.assign_ptr(const_cast<char *>(itr->c_str()), static_cast<int32_t>(itr->length()));

          int ret = get_param->add_cell(cell);
          if (ret != OB_SUCCESS) {
            TBSYS_LOG(ERROR, "add cell to get param failed:ret[%d]", ret);
            return ret;
          }

          itr++;
        }
      }

      return OB_SUCCESS;
    }

    int OceanbaseDb::do_server_get(common::ObServer &server, const ObRowkey& row_key, 
        ObScanner &scanner, 
        ObDataBuffer& data_buff, const std::string &table_name, 
        const std::vector<std::string> &columns)
    {
      int ret;
      ObCellInfo cell;
      ObGetParam *get_param = GET_TSI_MULT(ObGetParam, TSI_GET_ID);
      if (get_param == NULL) {
        TBSYS_LOG(ERROR, "can't allocate memory from TSI");
        return OB_ERROR;
      }
      get_param->reset();

      ObBorderFlag border;
      border.set_inclusive_start();
      border.set_inclusive_end();
      border.set_max_value();
      border.set_min_value();

      ObVersionRange version_range;

      version_range.start_version_ = 0;
      version_range.end_version_ = 0;

      version_range.border_flag_ = border;
      get_param->set_version_range(version_range);
      get_param->set_is_read_consistency(consistency_);

      cell.row_key_ = row_key;
      cell.table_name_.assign_ptr(const_cast<char *>(table_name.c_str()), static_cast<int32_t>(table_name.length()));

      std::vector<std::string>::const_iterator itr = columns.begin();
      while(itr != columns.end()) {
        cell.column_name_.assign_ptr(const_cast<char *>(itr->c_str()), static_cast<int32_t>(itr->length()));
        int ret = get_param->add_cell(cell);
        if (ret != OB_SUCCESS) {
          TBSYS_LOG(ERROR, "add cell to get param failed:ret[%d]", ret);
          return ret;
        }
        itr++;
      }

      data_buff.get_position() = 0;
      ret = get_param->serialize(data_buff.get_data(), data_buff.get_capacity(),
          data_buff.get_position());
      if (OB_SUCCESS == ret) {
        /* update send bytes */
        __sync_add_and_fetch(&db_stats_.total_send_bytes, data_buff.get_position());

        //add pangtianze [for Paxos]
        int32_t cluster_id = 1;
        if (OB_SUCCESS == ret)
        {
          if(OB_SUCCESS != (ret = serialization::encode_vi32(data_buff.get_data(), data_buff.get_capacity(),
                                                                  data_buff.get_position(), cluster_id)))
          {
            TBSYS_LOG(WARN, "failed to encode cluster_id, ret=%d", ret);
          }
        }
        //add:e

        //mod by qianzm 20160629:b
        //ret = client_.get_client_mgr().send_request(server, OB_GET_REQUEST, 1, timeout_, data_buff);
        ret = client_.get_client_mgr().send_request(server, OB_GET_REQUEST, 1,
            60000000, data_buff);
        //mod 20160629:e

        /* update recv bytes */
        if (ret == OB_SUCCESS) {
          __sync_add_and_fetch(&db_stats_.total_recv_bytes, data_buff.get_position());
        }
      } else {
        TBSYS_LOG(WARN, "serialzie get param failed, ret=%d", ret);
      }

      int64_t pos = 0;

      char ip_buf[25];
      server.ip_to_string(ip_buf, 25);
      TBSYS_LOG(DEBUG, "Merger server ip is %s, port is %d", ip_buf, server.get_port());

      ObResultCode result_code;
      if (OB_SUCCESS == ret) 
        ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);

      if (OB_SUCCESS != ret)
        TBSYS_LOG(ERROR, "do_server_get deserialize result failed:pos[%ld], ret[%d]", pos, ret);
      else
        ret = result_code.result_code_;

      if (OB_SUCCESS == ret) {
        scanner.clear();
        ret = scanner.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != ret)
          TBSYS_LOG(ERROR, "deserialize scanner from buff failed:pos[%ld], ret[%d]", pos, ret);
      }

      if (ret != OB_SUCCESS)
        TBSYS_LOG(ERROR, "do server get failed:%d, server=%s, table_name=%s", ret, ip_buf, table_name.c_str());

      return ret;
    }

    int OceanbaseDb::scan(const std::string &table, const std::vector<std::string> &columns, 
        const ObRowkey &start_key, const ObRowkey &end_key, DbRecordSet &rs, int64_t version, 
        bool inclusive_start, bool inclusive_end)
    {
      int ret = OB_SUCCESS;
      TabletInfo tablet_info;

      if (end_key.ptr() == NULL || end_key.length() == 0) {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "scan with null end key");
      }

      if (ret == OB_SUCCESS) {
        ret = search_tablet_cache(table, end_key, tablet_info);
        if (ret != OB_SUCCESS) {
          TBSYS_LOG(DEBUG, "Table %s not find in tablet cache, do root server get", table.c_str());

          ret = get_ms_location(end_key, table);
          if (ret == OB_SUCCESS)
            ret = search_tablet_cache(table, end_key, tablet_info);
          else {
            TBSYS_LOG(WARN, "get_ms_location faild , retcode:[%d]", ret);
          }
        }
      }

      if (ret == OB_SUCCESS) {
        ret = scan(tablet_info, table, columns, start_key, end_key, rs, version, inclusive_start, inclusive_end);
      }

      return ret;
    }


    int OceanbaseDb::scan(const TabletInfo &tablets, const std::string &table, const std::vector<std::string> &columns, 
        const ObRowkey &start_key, const ObRowkey &end_key, DbRecordSet &rs, int64_t version, 
        bool inclusive_start, bool inclusive_end)
    {
      int ret = OB_SUCCESS;
      ObScanParam *param = get_scan_param(table, columns, start_key, 
          end_key, inclusive_start, inclusive_end, version);
      if (NULL == param) {
        ret = OB_ERROR;
      } else {
        ObServer server;

        for(size_t i = 0;i < (size_t)kTabletDupNr; i++) {
          if (tablets.slice_[i].server_avail == true) {
            char server_str[128];

            server.set_ipv4_addr(tablets.slice_[i].ip_v4, tablets.slice_[i].ms_port);
            server.to_string(server_str, 128);

            ObDataBuffer data_buff = rs.get_buffer();

            ret = param->serialize(data_buff.get_data(), data_buff.get_capacity(),
                data_buff.get_position());
            if (ret != OB_SUCCESS) {
              TBSYS_LOG(WARN, "can't serialize scan param to databuf, ret=%d", ret);
            } else {
              int64_t pos = 0;

              ret = do_server_cmd(server, (int64_t)OB_SCAN_REQUEST, data_buff, pos);
              if (ret == OB_SUCCESS) {
                ObScanner &scanner = rs.get_scanner();
                scanner.clear();

                ret = scanner.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
                if (ret != OB_SUCCESS) {
                  TBSYS_LOG(WARN, "can't deserialize obscanner, ret=%d", ret);
                }
              } else {
                TBSYS_LOG(WARN, "error when calling do_server_cmd, ret=[%d], server:%s", ret, server_str);
              }
            }

            if (ret == OB_SUCCESS) {
              TBSYS_LOG(DEBUG, "scan requesting server, %s", server_str);
              break;
            }

            if (i < (kTabletDupNr - 1))
              usleep(5000);
          }
        }
      }

      return ret;
    }

    common::ObScanParam *OceanbaseDb::get_scan_param(const std::string &table, const std::vector<std::string>& columns,
        const ObRowkey &start_key, const ObRowkey &end_key,
        bool inclusive_start, bool inclusive_end,
        int64_t data_version)
    {
      ObScanParam *param = GET_TSI_MULT(ObScanParam, TSI_SCAN_ID);
      if (param == NULL) {
        TBSYS_LOG(ERROR, "can't allocate memory from TSI");
        return NULL;
      }

      TBSYS_LOG(DEBUG, "scan param version is %ld", data_version);
      param->reset();

      ObBorderFlag border;

      /* setup version */
      border.set_inclusive_start();
      border.set_inclusive_end();
      border.set_min_value();

      if (data_version == 0) {
        border.set_max_value();
      }

      ObVersionRange version_range;
      version_range.end_version_ = data_version;
      version_range.border_flag_ = border;
      param->set_version_range(version_range);

      /* set consistency */
      param->set_is_read_consistency(consistency_);

      /* do not cache the result */
      param->set_is_result_cached(false);

      /* setup scan range */
      ObBorderFlag scan_border;
      if (inclusive_start)
        scan_border.set_inclusive_start();
      else
        scan_border.unset_inclusive_start();

      if (inclusive_end)
        scan_border.set_inclusive_end();
      else
        scan_border.unset_inclusive_end();

      if (start_key.ptr() == NULL || start_key.length() == 0)
        scan_border.set_min_value();

      ObNewRange range;
      range.start_key_ = start_key;
      range.end_key_ = end_key;
      range.border_flag_ = scan_border;

      ObString table_name;
      table_name.assign(const_cast<char *>(table.c_str()), static_cast<int32_t>(table.length()));

      param->set(OB_INVALID_ID, table_name, range);
      ObString column;
      for(size_t i = 0;i < columns.size(); i++) {
        column.assign_ptr(const_cast<char *>(columns[i].c_str()), static_cast<int32_t>(columns[i].length()));

        if (param->add_column(column) != OB_SUCCESS) {
          param = NULL;
          TBSYS_LOG(ERROR, "can't add column to scan param");
          break;
        }
      }
      param->set_limit_info(limit_offset_, limit_count_);

      return param;
    }

    //del by zhuxh:20160712 [New way of fetching schema]
#if 0
    int OceanbaseDb::fetch_schema(common::ObSchemaManagerV2& schema_manager)
    {
      int ret = OB_SUCCESS;

      char *buff = new(std::nothrow) char[k2M];
      if (buff == NULL) {
        TBSYS_LOG(ERROR, "Fetch schema faild, due to memory lack");
        return common::OB_MEM_OVERFLOW;
      }

      ObDataBuffer data_buff;
      data_buff.set_data(buff, k2M);
      data_buff.get_position() = 0;

      serialization::encode_vi64(data_buff.get_data(), 
          data_buff.get_capacity(), data_buff.get_position(), 1);

       //mod by qianzm 20160629:b
      //ret = client_.get_client_mgr().send_request(root_server_, OB_FETCH_SCHEMA, 1, timeout_, data_buff);
      ret = client_.get_client_mgr().send_request(root_server_, OB_FETCH_SCHEMA, 1,
                    60000000, data_buff);
      //mod 20160629:e

      int64_t pos = 0;
      ObResultCode result_code;
      if (OB_SUCCESS == ret) 
        ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);

      if (OB_SUCCESS != ret)
        TBSYS_LOG(ERROR, "deserialize result failed:pos[%ld], ret[%d]", pos, ret);
      else
        ret = result_code.result_code_;

      if (OB_SUCCESS == ret) {
        ret = schema_manager.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "deserialize schema from buff failed:"
              "version[%d], pos[%ld], ret[%d]", 1, pos, ret);
        }
        else
        {
          TBSYS_LOG(DEBUG, "fetch schema succ:version[%ld]", schema_manager.get_version());
        }
      }

      delete [] buff;
      return ret;
    }
#endif

    //add by zhuxh:20160712 [New way of fetching schema]
    //reference: ob_backup_server.cpp:507
    int OceanbaseDb::fetch_schema(common::ObSchemaManagerV2& schema_manager)
    {
      int ret = OB_SUCCESS;

      common::ObClientManager & client_manager_ = client_.get_client_mgr();
      common::ObGeneralRpcStub rpc_stub_;

      if (OB_SUCCESS == ret)
      {
        ret = rpc_stub_.init(&rpc_buffer_, &client_manager_);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "init rpc stub failed, err=%d", ret);
        }
      }

      //fetch the latest schema
      if (OB_SUCCESS == ret)
      {
        //timeout = config_.network_timeout;
        int64_t retry_times = 0;
        int64_t retry_times_sum = 3;
        while ((ret == OB_SUCCESS) )//&& !stoped_)
        {
          // fetch table schema for startup.
          ret = rpc_stub_.fetch_schema(timeout_, root_server_,
                                          0, false, schema_manager);
          if (OB_SUCCESS == ret || retry_times > retry_times_sum) break;
          //usleep(RETRY_INTERVAL_TIME);
          sleep(1);
          TBSYS_LOG(INFO, "retry to fetch schema:retry_times[%ld]", retry_times ++);
        }
        if (ret != OB_SUCCESS && ret != OB_ALLOCATE_MEMORY_FAILED)
        {
          ret = OB_INNER_STAT_ERROR;
        }
      }

      return ret;
    }

	//add by liyongfeng, 20141014, get state of daily merge from rootserver
	//state值有三种情况:1(当前不在合并),0(当前正在合并),-1(获取合并状态失败)
	//对应的返回值有两种情况:OB_SUCCESS(对应state=1或state=0), 非OB_SUCCESS(state=-1)

    //mod by zhuxh:20160118 :b
    //additionally check merge state of slave cluster
	int OceanbaseDb::get_daily_merge_state(int32_t &state)
	{
		int ret = OB_SUCCESS;
        state = -1;

        bool not_merge_done=false;

		char *buff = new(std::nothrow) char[k2M];
		if (buff == NULL) {
			TBSYS_LOG(ERROR, "get merge state failed, due to memory lack");
			return common::OB_MEM_OVERFLOW;
		}
		static const int32_t MY_VERSION = 1;

        common::ObBaseClient * aoc[MAX_CLUSTER_NUM+1]={&client_};
        int i=0;
        for(i=0;i<arr_slave_client_length;i++)
        {
          aoc[i+1]=arr_slave_client_+i;
        }
        for( i=0 ; OB_SUCCESS == ret && i<arr_slave_client_length+1; i++ )
        {
          ObDataBuffer msgbuf;
          msgbuf.set_data(buff, k2M);
          msgbuf.get_position() = 0;

          TBSYS_LOG(INFO, "Checking merge state: %s, merge checking array index=%d", (i==0?"cluster master":"cluster slave"), i);
          if(OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), 32))) {
              TBSYS_LOG(ERROR, "failed to serialize ObDataBuffer, err=%d", ret);
          }
          else if (OB_SUCCESS != (ret = aoc[i]->send_recv(OB_RS_STAT, MY_VERSION, timeout_, msgbuf))) {
              TBSYS_LOG(ERROR, "failed to send request, pcode=%d, err=%d", OB_RS_STAT, ret);
          } else {
              ObResultCode result_code;
              msgbuf.get_position() = 0;
              if(OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position()))) {
                  TBSYS_LOG(ERROR, "failed to deserialize result from response, pos=%ld, err=%d", msgbuf.get_position(), ret);
              }
              else if (OB_SUCCESS != (ret = result_code.result_code_)) {
                  TBSYS_LOG(ERROR, "failed to get daily merge state, err=%d", result_code.result_code_);
              }else {
                  const char * merge_info = msgbuf.get_data() + msgbuf.get_position();
                  TBSYS_LOG(INFO, "current daily merge state [%s]", merge_info);
                  //compare msgbuf.get_data() + msgbuf.get_position() with "merge: DONE"
                  //three merge state -- merge: DONE / merge: DOING / merge: TIMEOUT

                  //ZXH TEST {
                  /*
                  const char * zxh_merge_info="merge: DOING";
                  if( i>0 )
                  {
                    merge_info = zxh_merge_info;
                    TBSYS_LOG(INFO, "cluster %d is thought to merge for testing by zhuxh on 20160119.",i+1);
                  }
                  */
                  //TBSYS_LOG(INFO, "cluster %d",i);
                  //ZXH TEST }

                  if (0 != strcmp(merge_info, "merge: DONE")) //msgbuf.get_data() + msgbuf.get_position() == "merge: DONE" means daily merge has completed, otherwise daily merge is doing.
                    //del by zhuxh:20160122 :b
#if 0
                  state = 1;
                } else {
                    state = 0;
#endif
                    //del :e

                    //add by zhx:20160122 :b
                  {
                    not_merge_done = true;
                    break;
                    //add :e
                  }
              }
          }
        }

		if (buff != NULL) {
			delete [] buff;
		}

        //add by zhuxh:20160122 :b
        if(OB_SUCCESS == ret)//Success means getting merge state successfully. The variable state should be no longer -1.
        {
          if(not_merge_done)//At least 1 cluster, including master and slaves, is still merging.
          {
            state=0;
          }
          else//No cluster is still merging.
            state=1;
        }
        //add :e

		return ret;
	}
    //add zhuxh:e

    int OceanbaseDb::do_server_cmd(const ObServer &server, const int32_t opcode, 
        ObDataBuffer &inout_buffer, int64_t &pos)
    {
      int ret = OB_SUCCESS;
	  
      ret = client_.get_client_mgr().send_request(server, opcode, 1, timeout_, inout_buffer);

      pos = 0;
      ObResultCode result_code;
      if (OB_SUCCESS == ret) 
      {
        ret = result_code.deserialize(inout_buffer.get_data(), inout_buffer.get_position(), pos);
      }

      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "deserialize result failed:pos[%ld], ret[%d]", pos, ret);
      }
      else
      {
        ret = result_code.result_code_;
      }

      return ret;
    }

    int OceanbaseDb::get_memtable_version(int64_t &version)
    {
      int ret = OB_SUCCESS;

      char *buff = new(std::nothrow) char[k2M];
      if (buff == NULL) {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "can't allocate memory for get_memtable_version");
      }

      if (ret == OB_SUCCESS) {
        ObDataBuffer buffer(buff, OB_MAX_PACKET_LENGTH);
        int64_t pos = 0;

        ret = do_server_cmd(update_server_, (int32_t)OB_UPS_GET_LAST_FROZEN_VERSION, buffer, pos);
        if (ret != OB_SUCCESS) {
          TBSYS_LOG(ERROR, "can't get memtable version errorcode=%d", ret);
        } else {
          ret = serialization::decode_vi64(buffer.get_data(), 
              buffer.get_position(), pos, &version);
          if (ret != OB_SUCCESS) {
            TBSYS_LOG(ERROR, "can't decode version, errorcode=%d", ret);
          }
        }
      }

      return ret;
    }

    int OceanbaseDb::get_update_server(ObServer &server)
    {
      int ret = OB_SUCCESS;
      char *buff = new(std::nothrow) char[k2M];
      if (buff == NULL)
      {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "can't allocate memory from heap");
      }

      if (ret == OB_SUCCESS)
      {
        ObDataBuffer buffer(buff, OB_MAX_PACKET_LENGTH);
        int64_t pos = 0;

        ret = do_server_cmd(root_server_, (int32_t)OB_GET_UPDATE_SERVER_INFO, buffer, pos);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "can't get updateserver errorcode=%d", ret);
        }
        else
        {
          ret = update_server_.deserialize(buffer.get_data(), buffer.get_position(), pos);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "can't get updateserver");
          }
          else
          {
            server = update_server_;
          }
        }
      }

      if (buff != NULL)
      {
        delete [] buff;
      }

      return ret;
    }

	//add by liyongfeng:20141020 获取最新的主UPS,并与第一次获得主UPS进行比较,相同,表示导入正确;否则,UPS发生切换,导入有问题
	int OceanbaseDb::get_latest_update_server(int32_t &ups_switch)
	{
		int ret = OB_SUCCESS;
		ups_switch = 0;
		char *buff = new(std::nothrow) char[k2M];
		ObServer latest_update_server;
		if(NULL == buff) {
			ret = OB_MEM_OVERFLOW;
			TBSYS_LOG(ERROR, "can't allocate memory from heap, err=%d", ret);
		}

		if (OB_SUCCESS == ret) {
			ObDataBuffer buffer(buff, OB_MAX_PACKET_LENGTH);
			int64_t pos = 0;

			ret = do_server_cmd(root_server_, (int32_t)OB_GET_UPDATE_SERVER_INFO, buffer, pos);
			if (ret != OB_SUCCESS) {
				TBSYS_LOG(ERROR, "send request to get latest update server failed, err=%d", ret);
			} else {
				ret = latest_update_server.deserialize(buffer.get_data(), buffer.get_position(), pos);
				if (ret != OB_SUCCESS) {
					TBSYS_LOG(ERROR, "failed to deserialize to get latest update server, err=%d", ret);
				} else {
					//比较前后两次获取的UPS,包括成为主UPS的时间
					if (latest_update_server == update_server_) {//如果前后两次UPS相同,需要判断前后两次成为主UPS的时间戳
						ups_switch = 0;
					} else {
						TBSYS_LOG(ERROR, "master update server has switched from server[%s] to server[%s]", update_server_.to_cstring(), latest_update_server.to_cstring());
						ups_switch = 1;
					}
				}
			}
		}

		if (buff != NULL) {
			delete [] buff;
		}

		return ret;
	}
	//add:end

    int OceanbaseDb::start_tnx(DbTranscation *&tnx)
    {
      int ret = OB_SUCCESS;
      tnx = new(std::nothrow) DbTranscation(this);
      if (tnx == NULL) {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "can't allocat transcation class");
      } else {
        ref();
      }

      return ret;
    }

    void OceanbaseDb::end_tnx(DbTranscation *&tnx)
    {
      if (tnx != NULL) {
        delete tnx;
        tnx = NULL;
        unref();
      }
    }

    // add by zcd :b
    /* 
     * ===  FUNCTION  ======================================================================
     *         Name:  fetch_ms_list
     *  Description:  获取当前集群中可以用来导出工具使用的ms集合，存储在array中
     *                master_percent是从主集群中取出的ms百分比，在0-100之间的整数，
     *                slave_percent是从备集群中取出的ms百分比，在0-100之间的整数
     *                获取到的ms集合存储在array中
     * =====================================================================================
     */
    int OceanbaseDb::fetch_ms_list(const int master_percent, const int slave_percent, std::vector<ObServer> &array)
    {
      TBSYS_LOG(INFO, "enter fetch ms list");
      int ret = OB_SUCCESS;
      int64_t session_id = 0;
      ObSQLResultSet rs;
      bool has_next = false;
      ObRow row;
      int cell_idx = 0;
      const ObObj* cell = NULL;
      uint64_t tid = 0;
      uint64_t cid = 0;
      ObString ip;
      int64_t port = 0;
      ObServer server;
      char ip_buffer[OB_IP_STR_BUFF];

      ObDataBuffer out_buffer;
      char *data = new char[MAX_THREAD_BUFFER_SIZE];
      out_buffer.set_data(data, MAX_THREAD_BUFFER_SIZE);

      ObServer ms;
      if(OB_SUCCESS != (ret = get_a_ms_from_rs(ms)))
      {
        TBSYS_LOG(ERROR, "get the first ms from rs failed!");
        return ret;
      }

      std::vector<ObServer> all_ms;
      std::vector<std::string> select_sql_vec;

      //mod by zhuxh:20160504 [cluster role instead of id] :b
      //select_sql_vec.push_back("select svr_ip, inner_port from __all_server where svr_type='mergeserver' and cluster_id=1"); /* 主集群中所有ms */
      //select_sql_vec.push_back("select svr_ip, inner_port from __all_server where svr_type='mergeserver' and cluster_id=2"); /* 备集群中所有ms */
      select_sql_vec.push_back("select svr_ip, inner_port from __all_server a inner join __all_cluster b on a.cluster_id=b.cluster_id where svr_type='mergeserver' and cluster_role=1");
      select_sql_vec.push_back("select svr_ip, inner_port from __all_server a inner join __all_cluster b on a.cluster_id=b.cluster_id where svr_type='mergeserver' and cluster_role=2");
      //mod by zhuxh:20160504 :e

      for(unsigned int i = 0; i < select_sql_vec.size(); i++)
      {
        all_ms.clear();
        std::string select_sql = select_sql_vec[i];

        TBSYS_LOG(INFO, "begin send execute sql");
        TBSYS_LOG(INFO, "connect ms: %s", to_cstring(ms));
        if(OB_SUCCESS != (ret = send_execute_sql(ms, select_sql, 60000000, session_id, rs, out_buffer, has_next)))
        {
          TBSYS_LOG(ERROR, "send execute sql failed, ret=%d", ret);
          return ret;
        }
        while(true)
        {
          TBSYS_LOG(INFO, "has next=%d", has_next);
          ObNewScanner &scanner = rs.get_new_scanner();
          //获取scanner里每条记录去填充到all_ms中
          while(OB_SUCCESS == (ret = scanner.get_next_row(row)))
          {
            cell_idx = 0;
            if(OB_SUCCESS != (ret = row.raw_get_cell(cell_idx++, cell, tid, cid)))
            {
              TBSYS_LOG(ERROR, "failed to get cell, ret=%d", ret);
              break;
            }
            else if(OB_SUCCESS != (ret = cell->get_varchar(ip)))
            {
              TBSYS_LOG(ERROR, "failed to get ip, ret=%d", ret);
              break;
            }
            else if(OB_SUCCESS != (ret = row.raw_get_cell(cell_idx++, cell, tid, cid)))
            {
              TBSYS_LOG(ERROR, "failed to get cell, ret=%d", ret);
              break;
            }
            else if(OB_SUCCESS != (ret = cell->get_int(port)))
            {
              TBSYS_LOG(ERROR, "failed to get port, ret=%d", ret);
              break;
            }
            else
            {
              char *p = ip.ptr();
              while(p < ip.length() + ip.ptr() && *p == ' ')
              {
                ++p;
              }
              int32_t length = static_cast<int32_t>(ip.length() - (p - ip.ptr()));
              int n = snprintf(ip_buffer, sizeof(ip_buffer), "%.*s", length, p);
              if (n<0 || n >= static_cast<int64_t>(sizeof(ip_buffer)))
              {
                ret = OB_BUF_NOT_ENOUGH;
                TBSYS_LOG(WARN, "ip buffer not enough, ip=%.*s", ip.length(), ip.ptr());
              }
              else if (!server.set_ipv4_addr(ip_buffer, static_cast<int32_t>(port)))
              {
                ret = OB_ERR_SYS;
                TBSYS_LOG(WARN, "failed to parse ip=%.*s", ip.length(), ip.ptr());
              }
              else if (server.get_ipv4() == 0)
              {
                ret = OB_INVALID_ARGUMENT;
                TBSYS_LOG(ERROR, "wrong proxy ip input ip=[%.*s] port=%ld, after parsed server=%s",
                    ip.length(), ip.ptr(), port, to_cstring(server));
              }
              else
              {
                all_ms.push_back(server);
              }
              if(OB_SUCCESS != ret)
              {
                TBSYS_LOG(ERROR, "failed to construct a record! ret=%d", ret);
                break;
              }
            }
          }
  
          if(OB_ITER_END == ret)
          {
            ret = OB_SUCCESS;
          }
          //根据has_next标志和返回码判断是否应该获取下一个数据包
          if(has_next && OB_SUCCESS == ret)
          {
            if(OB_SUCCESS != (ret = get_next_result(ms, PACKET_TIMEOUT_US, rs, out_buffer, session_id, has_next)))
            {
              TBSYS_LOG(ERROR, "get next result failed, ret=%d", ret);
              return ret;
            }
            TBSYS_LOG(INFO, "get next result success! ret=%d", ret);
          }
          else if(OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "get next result failed, ret=%d", ret);
            break;
          }
          else if(!has_next)
          {
            TBSYS_LOG(INFO, "the result is the last!");
            break;
          }
        }

        unsigned int count = 0;
        if(0 == i)
        {
          count = (unsigned int)(master_percent * all_ms.size() / 100);
          TBSYS_LOG(INFO, "zcd.master_count:%u", count);
        }
        else if(1 == i)
        {
          count = (unsigned int)(slave_percent * all_ms.size() / 100);
          TBSYS_LOG(INFO, "zcd.slave_count:%u", count);
        }

        for(unsigned int j = 0; j < count; j++)
        {
          array.push_back(all_ms[j]);
        }
      }

      if(select_sql_vec.empty() && array.empty())
      {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "no available ms exist!");
      }

      for(unsigned int i = 0; i < array.size(); i++)
      {
        TBSYS_LOG(INFO, "selected ms use:%s", to_cstring(array[i]));
      }

      return ret;
    }

    // add by zhangcd 20150814:b
    int OceanbaseDb::fetch_mysql_ms_list(const int percent, std::vector<ObServer> &array)
    {
      TBSYS_LOG(INFO, "enter fetch_mysql_ms_list");
      int ret = OB_SUCCESS;
      int64_t session_id = 0;
      ObSQLResultSet rs;
      bool has_next = false;
      ObRow row;
      int cell_idx = 0;
      const ObObj* cell = NULL;
      uint64_t tid = 0;
      uint64_t cid = 0;
      ObString ip;
      int64_t port = 0;
      ObServer server;
      char ip_buffer[OB_IP_STR_BUFF];

      ObDataBuffer out_buffer;
      char *data = new char[MAX_THREAD_BUFFER_SIZE];
      out_buffer.set_data(data, MAX_THREAD_BUFFER_SIZE);

      ObServer ms;
      if(OB_SUCCESS != (ret = get_a_ms_from_rs(ms)))
      {
        TBSYS_LOG(ERROR, "get the first ms from rs failed!");
        return ret;
      }

      std::vector<ObServer> all_ms;
      std::string select_sql = "select svr_ip, svr_port from __all_server a inner join __all_cluster b on a.cluster_id=b.cluster_id where cluster_role=1 and svr_type='mergeserver'";
      //mod by zhangcd:20150925
      //"select svr_ip, svr_port from __all_server where svr_type='mergeserver' and cluster_id=1"; /* 主集群中所有ms */

      TBSYS_LOG(INFO, "begin send execute sql");
      TBSYS_LOG(INFO, "connect ms: %s", to_cstring(ms));
      if(OB_SUCCESS != (ret = send_execute_sql(ms, select_sql, 60000000, session_id, rs, out_buffer, has_next)))
      {
        TBSYS_LOG(ERROR, "send execute sql failed, ret=%d", ret);
        return ret;
      }
      while(true)
      {
        TBSYS_LOG(INFO, "has next=%d", has_next);
        ObNewScanner &scanner = rs.get_new_scanner();
        //获取scanner里每条记录去填充到all_ms中
        while(OB_SUCCESS == (ret = scanner.get_next_row(row)))
        {
          cell_idx = 0;
          if(OB_SUCCESS != (ret = row.raw_get_cell(cell_idx++, cell, tid, cid)))
          {
            TBSYS_LOG(ERROR, "failed to get cell, ret=%d", ret);
            break;
          }
          else if(OB_SUCCESS != (ret = cell->get_varchar(ip)))
          {
            TBSYS_LOG(ERROR, "failed to get ip, ret=%d", ret);
            break;
          }
          else if(OB_SUCCESS != (ret = row.raw_get_cell(cell_idx++, cell, tid, cid)))
          {
            TBSYS_LOG(ERROR, "failed to get cell, ret=%d", ret);
            break;
          }
          else if(OB_SUCCESS != (ret = cell->get_int(port)))
          {
            TBSYS_LOG(ERROR, "failed to get port, ret=%d", ret);
            break;
          }
          else
          {
            char *p = ip.ptr();
            while(p < ip.length() + ip.ptr() && *p == ' ')
            {
              ++p;
            }
            int32_t length = static_cast<int32_t>(ip.length() - (p - ip.ptr()));
            int n = snprintf(ip_buffer, sizeof(ip_buffer), "%.*s", length, p);
            if (n<0 || n >= static_cast<int64_t>(sizeof(ip_buffer)))
            {
              ret = OB_BUF_NOT_ENOUGH;
              TBSYS_LOG(WARN, "ip buffer not enough, ip=%.*s", ip.length(), ip.ptr());
            }
            else if (!server.set_ipv4_addr(ip_buffer, static_cast<int32_t>(port)))
            {
              ret = OB_ERR_SYS;
              TBSYS_LOG(WARN, "failed to parse ip=%.*s", ip.length(), ip.ptr());
            }
            else if (server.get_ipv4() == 0)
            {
              ret = OB_INVALID_ARGUMENT;
              TBSYS_LOG(ERROR, "wrong proxy ip input ip=[%.*s] port=%ld, after parsed server=%s",
                  ip.length(), ip.ptr(), port, to_cstring(server));
            }
            else
            {
              all_ms.push_back(server);
            }
            if(OB_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR, "failed to construct a record! ret=%d", ret);
              break;
            }
          }
        }

        if(OB_ITER_END == ret)
        {
          ret = OB_SUCCESS;
        }
        //根据has_next标志和返回码判断是否应该获取下一个数据包
        if(has_next && OB_SUCCESS == ret)
        {
          if(OB_SUCCESS != (ret = get_next_result(ms, PACKET_TIMEOUT_US, rs, out_buffer, session_id, has_next)))
          {
            TBSYS_LOG(ERROR, "get next result failed, ret=%d", ret);
            return ret;
          }
          TBSYS_LOG(INFO, "get next result success! ret=%d", ret);
        }
        else if(OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "get next result failed, ret=%d", ret);
          break;
        }
        else if(!has_next)
        {
          TBSYS_LOG(INFO, "the result is the last!");
          break;
        }
      }

      unsigned int count = 0;
      count = (unsigned int)(percent * all_ms.size() / 100);
      TBSYS_LOG(INFO, "zcd.master_count:%u", count);

      for(unsigned int j = 0; j < count; j++)
      {
        array.push_back(all_ms[j]);
      }

      if(array.empty())
      {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "no available ms exist!");
      }

      for(unsigned int i = 0; i < array.size(); i++)
      {
        TBSYS_LOG(INFO, "selected ms use:%s", to_cstring(array[i]));
      }

      return ret;
    }
    //add:e

    /* 
     * ===  FUNCTION  ======================================================================
     *         Name:  get_result
     *  Description:  获取一个查询的结果数据包，反序列化到rs中返回
     *                并拷贝一份到out_buffer中,has_next作为返回参数标记是否还有下一个
     * =====================================================================================
     */
    int OceanbaseDb::get_result(ObSQLResultSet &rs, ObDataBuffer &out_buffer, bool &has_next)
    {
      int ret = OB_SUCCESS;
      struct ExecuteSqlParam *param = NULL;
      TBSYS_LOG(INFO, "enter get_result!");
      // 使用线程私有单例的ExecuteSqlParam对象
      if(OB_SUCCESS != (ret = get_ts_execute_sql_param(param)))
      {
        TBSYS_LOG(ERROR, "get execute sql param failed!");
        return ret;
      }

      TBSYS_LOG(TRACE, "current exe sql client_timeout left is %ld", param->client_timeout_timestamp - tbsys::CTimeUtil::getTime());
      // 判断是否超时，此超时时间是当前客户端的超时时间
      if(param->client_timeout_timestamp <= tbsys::CTimeUtil::getTime())
      {
        TBSYS_LOG(ERROR, "current exe sql is timeout!");
        return OB_RESPONSE_TIME_OUT;
      }

      // 判断是不是拿第一个数据包，如果是的话就需要调用send_execute_sql函数
      if(param->is_first)
      {
        param->is_first = false;
        if(OB_SUCCESS != (ret = send_execute_sql(param->ms, param->sql, param->ms_timeout, param->session_id, rs, out_buffer, has_next)))
        {
          TBSYS_LOG(ERROR, "first send execute sql failed! ret=%d", ret);
          return ret;
        }
        else
        {
          TBSYS_LOG(INFO, "first send execute sql success!");
        }
      }
      // 如果不是拿第一个数据包（即拿后续的数据包），则调用get_next_result函数
      else
      {
        if(OB_SUCCESS != (ret = get_next_result(param->ms, PACKET_TIMEOUT_US, rs, out_buffer, param->session_id, has_next)))
        {
          TBSYS_LOG(ERROR, "next get_result failed! ret=%d", ret);
          return ret;
        }
        else
        {
          TBSYS_LOG(INFO, "next get_result success!");
        }
      }
      return ret;
    }

    /* 
     * ===  FUNCTION  ======================================================================
     *         Name:  send_execute_sql
     *  Description:  向ms发送sql请求, 
     *                ms_timeout_us为整个执行过程加上数据包传输过程的总的超时时间
     *                session_id为传回的参数，作为后续请求没有接收完的所有数据包的参数
     *                out_buffer为得到的数据包
     *                has_next标记是否还有数据没有传送
     * =====================================================================================
     */
    int OceanbaseDb::send_execute_sql(const ObServer& ms, const std::string &sql, const int64_t ms_timeout_us, int64_t &session_id, ObSQLResultSet &rs, ObDataBuffer &out_buffer, bool &has_next)
    {
      int ret = OB_SUCCESS;
      ObDataBuffer databuf;
      ObString ob_sql = ObString::make_string(sql.c_str());
      // 使用线程私有单例对象ObDataBuffer，作为数据存储区
      if(OB_SUCCESS != (ret = get_thread_buffer(databuf)))
      {
        TBSYS_LOG(ERROR, "failed to get thread buffer. err=%d", ret);
      }
      else if(databuf.get_capacity() > out_buffer.get_capacity() || NULL == out_buffer.get_data())
      {
        TBSYS_LOG(ERROR, "out buffer is not correct!");
        ret = OB_ERROR;
      }
      else if(OB_SUCCESS != (ret = ob_sql.serialize(databuf.get_data(), databuf.get_capacity(), databuf.get_position())))
      {
        TBSYS_LOG(ERROR, "failed to serialize the sql into buf. ret=%d", ret);
      }
      // 发起sql查询，向ms发送带有sql的请求包，此函数是同步函数，返回结果包含结果集中的第一个数据包，会存储在输出缓存中
      else if(OB_SUCCESS != (ret = client_.get_client_mgr().send_request(ms, OB_SQL_EXECUTE, DefaultAPIVersion, ms_timeout_us, databuf, session_id)))
      {
        TBSYS_LOG(ERROR, "failed to send request. ret=%d, ms_timeout_us=%ld", ret, ms_timeout_us);
      }
      if(OB_SUCCESS != ret)
      {
        return ret;
      }
      databuf.get_position() = 0;
      if(OB_SUCCESS != (ret = rs.deserialize(databuf.get_data(), databuf.get_capacity(), databuf.get_position())))
      {
        TBSYS_LOG(ERROR, "failed to deserialize result buffer, ret=%d", ret);
      }
	  else if(OB_SUCCESS != (ret = rs.get_errno()))
	  {
	    TBSYS_LOG(ERROR, "execute query sql failed, sql=[%s], errno=[%d], errinfo=[%s]", sql.c_str(), rs.get_errno(), ob_strerror(rs.get_errno()));
	  }
      else
      {
        // 将输出结果的未反序列化的内存块拷贝到out_buffer中
        memcpy(out_buffer.get_data(), databuf.get_data(), databuf.get_capacity());
        bool fullfilled = true;
        rs.get_fullfilled(fullfilled);
        if(rs.get_new_scanner().is_empty() || fullfilled)
        {
          has_next = false;
        }
        else
        {
          has_next = true;
        }
      }
      return ret;
    }

    /* 
     * ===  FUNCTION  ======================================================================
     *         Name:  get_next_result
     *  Description:  获取查询结果的下一个包
     *                packet_timeout为接受一个数据包的超时时间
     *                rs为反序列化的数据集
     *                out_buffer为未序列化的数据
     *                session_id为传入作为参数的标记
     *                has_next为是否还有下一个数据包
     * =====================================================================================
     */
    int OceanbaseDb::get_next_result(const ObServer& ms, const int64_t packet_timeout, ObSQLResultSet &rs, common::ObDataBuffer &out_buffer, const int64_t session_id, bool &has_next)
    {
      int ret = OB_SUCCESS;
      ObDataBuffer databuf;
      if(OB_SUCCESS != (ret = get_thread_buffer(databuf)))
      {
        TBSYS_LOG(ERROR, "failed to get thread buffer. err=%d", ret);
      }
      else if(databuf.get_capacity() > out_buffer.get_capacity() || NULL == out_buffer.get_data())
      {
        TBSYS_LOG(ERROR, "out buffer is not correct!");
        ret = OB_ERROR;
      }
      // 发送请求获取下一个数据包
      else if(OB_SUCCESS != (ret = client_.get_client_mgr().get_next(ms, session_id, packet_timeout, databuf, databuf)))
      {
        TBSYS_LOG(ERROR, "failed to get next packet. err=%d", ret);
      }
      if(OB_SUCCESS != ret)
      {
        return ret;
      }
      databuf.get_position() = 0;
      if(OB_SUCCESS != (ret = rs.deserialize(databuf.get_data(), databuf.get_capacity(), databuf.get_position())))
      {
        TBSYS_LOG(ERROR, "failed to deserialize result buffer, ret=%d", ret);
      }
	  else if(OB_SUCCESS != (ret = rs.get_errno()))
	  {
	    TBSYS_LOG(ERROR, "fetch next packet failed, sql=[%s], errno=[%d], errinfo=[%s]", rs.get_sql_str().ptr(), rs.get_errno(), ob_strerror(rs.get_errno()));
	  }
      else
      {
        memcpy(out_buffer.get_data(), databuf.get_data(), databuf.get_capacity());
        bool fullfilled = true;
        rs.get_fullfilled(fullfilled);
        if(rs.get_new_scanner().is_empty() || fullfilled)
        {
          has_next = false;
        }
        else
        {
          has_next = true;
        }
      }
      return ret;
    }
    
    /* 
     * ===  FUNCTION  ======================================================================
     *         Name:  get_thread_buffer
     *  Description:  获取线程内的公共内存
     *                data_buffer作为结果返回
     * =====================================================================================
     */
    int OceanbaseDb::get_thread_buffer(common::ObDataBuffer& data_buffer)
    {
      int ret = OB_SUCCESS;
      common::ThreadSpecificBuffer::Buffer* buff = tsbuffer_.get_buffer();
      if (NULL == buff)
      {
        TBSYS_LOG(ERROR, "thread_buffer_ = NULL");
        ret = OB_ERROR;
      }
      else
      {
        buff->reset();
        data_buffer.set_data(buff->current(), buff->remain());
      }
      return ret;
    }

    
    /* 
     * ===  FUNCTION  ======================================================================
     *         Name:  init_execute_sql
     *  Description:  初始化一次查询的参数
     *                ms为请求查询的server
     *                ms_timeout为查询执行的过程和数据传输过程的总超时时间
     *                client_timeout为客户端设定的超时时间，如果客户端检查发现从发送第一次
     *                查询请求到接收到某个数据包的时间已经超过了client_timeout，客户端会自
     *                动放弃请求下一个数据包
     *                通常client_timeout需要设定比ms_timeout少10000000us~20000000us
     * =====================================================================================
     */
    int OceanbaseDb::init_execute_sql(const ObServer& ms, const std::string& sql, int64_t ms_timeout, int64_t client_timeout)
    {
      int ret = OB_SUCCESS;
      void *ptr = pthread_getspecific(send_sql_param_key_);
      if(NULL == ptr)
      {
        struct ExecuteSqlParam * param = new struct ExecuteSqlParam;
        if(NULL == param)
        {
          TBSYS_LOG(ERROR, "malloc memory failed!");
          ret = OB_ERROR;
        }
        else
        {
          ptr = param;
          ret = pthread_setspecific(send_sql_param_key_, ptr);
          if(0 != ret)
          {
            TBSYS_LOG(ERROR, "pthread_setspecific failed:%d", ret);
            ret = OB_ERROR;
            delete param;
            param = NULL;
          }
        }
      }
      if(OB_SUCCESS == ret)
      {
        struct ExecuteSqlParam *param = (struct ExecuteSqlParam *)ptr;
        param->ms = ms;
        param->sql = sql;
        param->session_id = 0;
        param->client_timeout_timestamp = tbsys::CTimeUtil::getTime() + client_timeout;
        param->ms_timeout = ms_timeout;
        param->is_first = true;
      }
      return ret;
    }


    /* 
     * ===  FUNCTION  ======================================================================
     *         Name:  get_ts_execute_sql_param
     *  Description:  获取线程私有的ExecuteSqlParam单例对象
     * =====================================================================================
     */
    int OceanbaseDb::get_ts_execute_sql_param(struct ExecuteSqlParam *&param)
    {
      int ret = OB_SUCCESS;
      void *ptr = pthread_getspecific(send_sql_param_key_);
      if(NULL == ptr)
      {
        TBSYS_LOG(ERROR, "get thread specific sql param failed!");
        ret = OB_ERROR;
      }
      param = (struct ExecuteSqlParam *)ptr;
      return ret;
    }


    /* 
     * ===  FUNCTION  ======================================================================
     *         Name:  destroy_ts_execute_sql_param_key
     *  Description:  线程结束时调用的销毁线程私有ExecuteSqlParam单例对象的函数
     * =====================================================================================
     */
    void OceanbaseDb::destroy_ts_execute_sql_param_key(void *ptr)
    {
      TBSYS_LOG(INFO, "delete thread specific buffer, ptr=%p", ptr);
      if(NULL != ptr)
      {
        delete (struct ExecuteSqlParam*)ptr;
      }
    }
    // :end


    /* *
     * @author liyongfeng:20141110
     * @brief OceanbaseDb::get_tablet_info 获取tablet信息
     * @param table 表名
     * @param rowkey 指定主键
     * @param data_buff 用于序列化ObGetParam发包,并接受返回数据后反序列化到ObScanner
     * @return 成功,返回OB_SUCCESS;否则,返回非OB_SUCCESS
     */
    int OceanbaseDb::get_tablet_info(const std::string &table, const ObRowkey &rowkey, ObDataBuffer &data_buff, ObScanner &scanner)
    {
        int ret = OB_SUCCESS;
        std::vector<std::string> columns;
        columns.push_back("*");
    
        if (!inited_) {
            TBSYS_LOG(ERROR, "OceanbaseDb is not inited, please initialize it first");
            return OB_ERROR;
        }
    
        if (OB_SUCCESS == ret) {
            int retries = 3;
            while (retries--) {
                ret = do_server_get(root_server_, rowkey, scanner, data_buff, table, columns);
                if (OB_SUCCESS == ret) break;
                else
                    sleep(1);
            }
    
            if (ret != OB_SUCCESS) {
                TBSYS_LOG(ERROR, "do server get failed, server[%s], ret=[%d]", root_server_.to_cstring(), ret);
            }
        }
        return ret;
    }

    /* 
     * ===  FUNCTION  ======================================================================
     *         Name:  get_a_ms_from_rs
     *  Description:  从rs获取一个ms的地址到out_ms中
     * =====================================================================================
     */
    int OceanbaseDb::get_a_ms_from_rs(ObServer &out_ms)
    {
      int64_t pos = 0;
      int ret = OB_SUCCESS;
      int buffer_size = 1024*1024*2;
      char *buffer = new char[buffer_size];
      ObDataBuffer data_buff(buffer, buffer_size);
      std::vector<ObServer> ms_list;
      ObResultCode res;
      //add pangtianze [for Paxos]
      int32_t cluster_id = 0;
      if (OB_SUCCESS == ret)
      {
        if(OB_SUCCESS != (ret = serialization::encode_vi32(data_buff.get_data(), data_buff.get_capacity(),
                                                                data_buff.get_position(), cluster_id)))
        {
          TBSYS_LOG(WARN, "failed to encode cluster_id, ret=%d", ret);
        }
      }
      //add:e
      ret = do_server_cmd(root_server_, (int32_t)OB_GET_MS_LIST, data_buff, pos);
      if (OB_SUCCESS != ret) 
      {
        TBSYS_LOG(ERROR, "can't do server cmd, ret=%d", ret);
      }
      else 
      {
        data_buff.get_position() = 0;
        ret = res.deserialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "ObResultCode deserialize error, ret=%d", ret);
        }
        else
        {
          int32_t ms_num = 0;
          ret = serialization::decode_vi32(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position(), &ms_num);
          if(OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "decode ms num fail:ret[%d]", ret);
          }
          else if(ms_num <= 0)
          {
            ret = OB_ERROR;
            TBSYS_LOG(ERROR, "no ms exist!");
          }
          else
          {
            TBSYS_LOG(DEBUG, "ms server number[%d]", ms_num);
            ObServer ms;
            int64_t reserved = 0;
            for(int32_t i = 0;i < ms_num && OB_SUCCESS == ret; i++)
            {
              if (OB_SUCCESS != (ret = ms.deserialize(data_buff.get_data(),
                                                      data_buff.get_capacity(),
                                                      data_buff.get_position())))
              {
                TBSYS_LOG(ERROR, "deserialize merge server fail, ret: [%d]", ret);
              }
              else if (OB_SUCCESS !=
                       (ret = serialization::decode_vi64(data_buff.get_data(),
                                                         data_buff.get_capacity(),
                                                         data_buff.get_position(),
                                                         &reserved)))
              {
                TBSYS_LOG(ERROR, "deserializ merge server"
                          " reserver int64 fail, ret: [%d]", ret);
              }
              else
              {
                ms_list.push_back(ms);
              }
            }
            if(OB_SUCCESS == ret)
            {
              out_ms = ms_list[0];
            }
          }
        }
      }
      delete buffer;
      return ret;
    }


    /* 
     * ===  FUNCTION  ======================================================================
     *         Name:  get_thread_server_session
     *  Description:  获取当前线程正在执行的查询的sql对应的server和session_id参数
     *                结果存储在server和id中
     * =====================================================================================
     */
    int OceanbaseDb::get_thread_server_session(ObServer &server, int64_t& session_id)
    {
      int ret = OB_SUCCESS;
      struct ExecuteSqlParam *param = NULL;
      if(OB_SUCCESS != (ret = get_ts_execute_sql_param(param)))
      {
        TBSYS_LOG(ERROR, "get_ts_execute_sql_param failed! ret=%d", ret);
      }
      else
      {
        server = param->ms;
        session_id = param->session_id;
      }
      return ret;
    }

    /* 
     * ===  FUNCTION  ======================================================================
     *         Name:  post_end_next
     *  Description:  发送终止获取下一个数据包的请求到对应的server上
     *                输入参数为server和session_id
     * =====================================================================================
     */
    int OceanbaseDb::post_end_next(ObServer server, int64_t session_id)
    {
      int ret = OB_SUCCESS;
      ObDataBuffer buffer;
      if(OB_SUCCESS != get_thread_buffer(buffer))
      {
        TBSYS_LOG(ERROR, "get_thread_buffer failed! ret=%d", ret);
      }
      else if(OB_SUCCESS != (ret = client_.get_client_mgr().post_end_next(server, session_id, PACKET_TIMEOUT_US, buffer, NULL, NULL)))
      {
        TBSYS_LOG(ERROR, "post_end_next failed! server=%s, session_id=%ld ret=%d", to_cstring(server), session_id, ret);
      }
      return ret;
    }

    int OceanbaseDb::execute_dml_sql(const ObString& ob_sql, const ObServer &ms, int64_t& affected_row)
    {
      int ret = OB_SUCCESS;
      ObDataBuffer databuf;
      int64_t session_id = -1;
      ObSQLResultSet rs;
      // 使用线程私有单例对象ObDataBuffer，作为数据存储区
      if(OB_SUCCESS != (ret = get_thread_buffer(databuf)))
      {
        TBSYS_LOG(ERROR, "failed to get thread buffer. err=%d", ret);
      }
      else if(OB_SUCCESS != (ret = ob_sql.serialize(databuf.get_data(), databuf.get_capacity(), databuf.get_position())))
      {
        TBSYS_LOG(ERROR, "failed to serialize the sql into buf. ret=%d", ret);
      }
      // 发起sql查询，向ms发送带有sql的请求包，此函数是同步函数，返回结果包含结果集中的第一个数据包，会存储在输出缓存中
      else if(OB_SUCCESS != (ret = client_.get_client_mgr().send_request(ms, OB_SQL_EXECUTE, DefaultAPIVersion, 1000 * 1000 * 1000, databuf, session_id)))
      {
        TBSYS_LOG(ERROR, "failed to send request. ret=%d", ret);
      }
      if(OB_SUCCESS != ret)
      {
        return ret;
      }
      databuf.get_position() = 0;
      if(OB_SUCCESS != (ret = rs.deserialize(databuf.get_data(), databuf.get_capacity(), databuf.get_position())))
      {
        TBSYS_LOG(ERROR, "failed to deserialize result buffer, ret=%d", ret);
      }
      else if(OB_SUCCESS != (ret = rs.get_errno()))
      {
        TBSYS_LOG(ERROR, "execute query sql failed, sql=[%.*s], errno=[%d], errinfo=[%s]", ob_sql.length(), ob_sql.ptr(), rs.get_errno(), ob_strerror(rs.get_errno()));
      }
      else
      {
        if(rs.get_new_scanner().is_empty())
        {
          affected_row = rs.get_result_set().get_affected_rows();
        }
        else
        {
          TBSYS_LOG(ERROR, "this function should not execute query sql!");
        }
      }
      return ret;
    }

    //add by zhuxh:20160118
    //to check merge of slave cluster
    int OceanbaseDb::init_slave_cluster_info()
    {
      TBSYS_LOG(INFO, "enter fetch_mysql_ms_list");
      int ret = OB_SUCCESS;
      int64_t session_id = 0;
      ObSQLResultSet rs;
      bool has_next = false;
      ObRow row;
      int cell_idx = 0;
      const ObObj* cell = NULL;
      uint64_t tid = 0;
      uint64_t cid = 0;
      ObString ip;
      int64_t port = 0;
      ObServer server;
      char ip_buffer[OB_IP_STR_BUFF];

      ObDataBuffer out_buffer;
      char *data = new char[MAX_THREAD_BUFFER_SIZE];
      out_buffer.set_data(data, MAX_THREAD_BUFFER_SIZE);

      ObServer ms;
      if(OB_SUCCESS != (ret = get_a_ms_from_rs(ms)))
      {
        TBSYS_LOG(ERROR, "get the first ms from rs failed!");
        return ret;
      }
      std::string select_sql = "select cluster_vip,rootserver_port from __all_cluster where cluster_role=2 and cluster_flow_percent>0";

      char sql_buf[64]={0};
      sprintf(sql_buf,"%d",MAX_CLUSTER_NUM);
      select_sql += " limit ";
      select_sql += sql_buf;

      if( OB_SUCCESS != (ret = send_execute_sql(ms, select_sql, 60000000, session_id, rs, out_buffer, has_next)))
      {
        TBSYS_LOG(ERROR, "send execute sql failed, ret=%d", ret);
        return ret;
      }
      while(true)
      {
        TBSYS_LOG(INFO, "has next=%d", has_next);
        ObNewScanner &scanner = rs.get_new_scanner();
        //获取scanner里每条记录，以初始化arr_slave_client_的每一个元素
        while(OB_SUCCESS == (ret = scanner.get_next_row(row)))
        {
          cell_idx = 0;
          if(OB_SUCCESS != (ret = row.raw_get_cell(cell_idx++, cell, tid, cid)))
          {
            TBSYS_LOG(ERROR, "failed to get cell, ret=%d", ret);
            break;
          }
          else if(OB_SUCCESS != (ret = cell->get_varchar(ip)))
          {
            TBSYS_LOG(ERROR, "failed to get ip, ret=%d", ret);
            break;
          }
          else if(OB_SUCCESS != (ret = row.raw_get_cell(cell_idx++, cell, tid, cid)))
          {
            TBSYS_LOG(ERROR, "failed to get cell, ret=%d", ret);
            break;
          }
          else if(OB_SUCCESS != (ret = cell->get_int(port)))
          {
            TBSYS_LOG(ERROR, "failed to get port, ret=%d", ret);
            break;
          }
          else
          {
            char *p = ip.ptr();
            while(p < ip.length() + ip.ptr() && *p == ' ')
            {
              ++p;
            }
            int32_t length = static_cast<int32_t>(ip.length() - (p - ip.ptr()));
            int n = snprintf(ip_buffer, sizeof(ip_buffer), "%.*s", length, p);
            if (n<0 || n >= static_cast<int64_t>(sizeof(ip_buffer)))
            {
              ret = OB_BUF_NOT_ENOUGH;
              TBSYS_LOG(WARN, "ip buffer not enough, ip=%.*s", ip.length(), ip.ptr());
            }
            else if (!server.set_ipv4_addr(ip_buffer, static_cast<int32_t>(port)))
            {
              ret = OB_ERR_SYS;
              TBSYS_LOG(WARN, "failed to parse ip=%.*s", ip.length(), ip.ptr());
            }
            else if (server.get_ipv4() == 0)
            {
              ret = OB_INVALID_ARGUMENT;
              TBSYS_LOG(ERROR, "wrong proxy ip input ip=[%.*s] port=%ld, after parsed server=%s",
                  ip.length(), ip.ptr(), port, to_cstring(server));
            }
            else
            {
              TBSYS_LOG(INFO, "server=%s, %.*s:%ld, merge checking array index=%d, will be checked merge state later.",
                  to_cstring(server), ip.length(), ip.ptr(), port, arr_slave_client_length+1);
              arr_slave_client_[arr_slave_client_length++].initialize(server);
            }
            if(OB_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR, "failed to construct a record! ret=%d", ret);
              break;
            }
          }
        }

        if(OB_ITER_END == ret)
        {
          ret = OB_SUCCESS;
        }
        //根据has_next标志和返回码判断是否应该获取下一个数据包
        if(has_next && OB_SUCCESS == ret)
        {
          if(OB_SUCCESS != (ret = get_next_result(ms, PACKET_TIMEOUT_US, rs, out_buffer, session_id, has_next)))
          {
            TBSYS_LOG(ERROR, "get next result failed, ret=%d", ret);
            return ret;
          }
          TBSYS_LOG(INFO, "get next result success! ret=%d", ret);
        }
        else if(OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "get next result failed, ret=%d", ret);
          break;
        }
        else if(!has_next)
        {
          TBSYS_LOG(INFO, "the result is the last!");
          break;
        }
      }

      return ret;

    }
    //add zhuxh:e

//add by zhuxh:20160603 [Sequence] :b
//mod by zhuxh:20160907 [cb sequence]
    int OceanbaseDb::sequence(
        bool create, std::string seq_name,
        int64_t      start, int64_t      increment,
        bool default_start, bool default_increment )
    {
      int ret = OB_SUCCESS;
      ObServer ms;
      if(OB_SUCCESS != (ret = get_a_ms_from_rs(ms)))
      {
        TBSYS_LOG(ERROR, "get the first ms from rs failed!");
      }

      char * data = new (std::nothrow) char[MAX_THREAD_BUFFER_SIZE];
      if( NULL == data )
      {
        TBSYS_LOG(ERROR, "Allocating memory failed.");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        ObDataBuffer out_buffer;
        out_buffer.set_data(data, MAX_THREAD_BUFFER_SIZE);
        int64_t session_id = 0;
        ObSQLResultSet rs;
        bool has_next = false;

        //mod by zhuxh:20160907 [cb sequence] :b
        //char number [20] = {0};//Length of MAX_INT == Length(2147483647) = 10
        char number [30] = {0};//Length of LONG_LONG_MIN ~= Length(-10^18) == 20
        std::string sql;

        sql  = "select count(*) from __all_sequence where sequence_name='";
        sql += seq_name;
        sql += "'";
        int64_t sequence_has_existed = 0; //result of the sql above

        if(OB_SUCCESS != (ret = send_execute_sql(ms, sql, 60000000, session_id, rs, out_buffer, has_next)))
        {
          TBSYS_LOG(ERROR, "Sending execute sql fails, ret=%d", ret);
        }
        else
        {
          int cell_idx = 0;
          ObRow row;
          ObNewScanner & scanner = rs.get_new_scanner();
          uint64_t tid = 0;
          uint64_t cid = 0;
          const ObObj * cell = NULL;
          if ( OB_SUCCESS != (ret = scanner.get_next_row(row)) )
          {
            TBSYS_LOG(ERROR, "Getting next row fails, ret=%d", ret);
          }
          else if(OB_SUCCESS != (ret = row.raw_get_cell(cell_idx++, cell, tid, cid)))
          {
            TBSYS_LOG(ERROR, "Getting a cell fails, ret=%d", ret);
          }
          else if(OB_SUCCESS != (ret = cell->get_int(sequence_has_existed)))
          {
            TBSYS_LOG(ERROR, "Getting the value of cell fails, ret=%d", ret);
          }
        }

        if( OB_SUCCESS == ret )
        {
          if( ! create ) //create == false means dropping
          {
            sql = "drop sequence ";
            sql += seq_name;
          }
          else if( ! sequence_has_existed ) //The sequence doesn't exist. So create it.
          {
            int64_t _start = start;
            int64_t _increment = increment;
            if( default_start )
              _start = 0;
            if( default_increment )
              _increment = 1;

            sql = "create sequence \""; //mod by zhuxh:20161018 [sequence case bug]
            sql += seq_name;
            sql += "\" as bigint start with "; //mod by zhuxh:20161018 [sequence case bug]
            sprintf(number, "%ld", _start); sql += number;
            sql += " increment by ";
            sprintf(number, "%ld", _increment); sql += number;
          }
          else if( !( default_start && default_increment ) ) //It has existed and more than one parameter need update
          {
            sql = "alter sequence \""; //mod by zhuxh:20161018 [sequence case bug]
            sql += seq_name;
            sql += "\""; //add by zhuxh:20161018 [sequence case bug]
            if( ! default_start )
            {
              sql += " restart with ";
              sprintf(number, "%ld", start); sql += number;
            }
            if( ! default_increment )
            {
              sql += " increment by ";
              sprintf(number, "%ld", increment); sql += number;
            }
          }
        }
        //mod by zhuxh:20160907 [cb sequence] :e
        if(OB_SUCCESS != (ret = send_execute_sql(ms, sql, 60000000, session_id, rs, out_buffer, has_next)))
        {
          TBSYS_LOG(ERROR, "send execute sql failed, ret=%d", ret);
        }
      }

      delete []data;
      return ret;
    }
  }
}
//add :e
