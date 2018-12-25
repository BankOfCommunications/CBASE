/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*
*
*   Version: 0.1 2010-09-19
*
*   Authors:
*          ruohai(ruohai@taobao.com)
*
*
================================================================*/
#include <stdlib.h>
#include "common/ob_malloc.h"
#include "common/file_utils.h"
#include "rootserver/ob_root_log_worker.h"
#include "rootserver/ob_root_server2.h"
#include "rootserver/ob_root_log_manager.h"
#include "rootserver/ob_root_worker.h"
namespace oceanbase
{
  using namespace common;
  namespace rootserver
  {
    ObRootLogWorker::ObRootLogWorker()
    {
    }

    void ObRootLogWorker::set_root_server(ObRootServer2* root_server)
    {
      root_server_ = root_server;
    }

    void ObRootLogWorker::set_balancer(ObRootBalancer* balancer)
    {
      balancer_ = balancer;
    }

    void ObRootLogWorker::set_log_manager(ObRootLogManager* log_manager)
    {
      log_manager_ = log_manager;
    }

    int ObRootLogWorker::sync_schema(const int64_t timestamp)
    {
      int ret = OB_SUCCESS;
      char* log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH, ObModIds::OB_RS_LOG_WORKER));
      if (log_data == NULL)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "allocate memory failed, size: %ld", OB_MAX_PACKET_LENGTH);
      }

      int64_t pos = 0;
      // read schema conteng from schema file
      if (ret == OB_SUCCESS)
      {
        FileUtils fu;
        int32_t rc = fu.open(root_server_->config_.schema_filename, O_RDONLY);
        if (rc < 0)
        {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "open schema file failed when sync");
        }
        else
        {
          // read schema content from file
          char* tmp_buffer = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH, ObModIds::OB_RS_LOG_WORKER));
          if (tmp_buffer == NULL)
          {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            TBSYS_LOG(ERROR, "allocate memory failed, size: %ld", OB_MAX_PACKET_LENGTH);
          }

          int64_t rl = 0;
          if (ret == OB_SUCCESS)
          {
            rl = fu.read(tmp_buffer, OB_MAX_PACKET_LENGTH);
            if (rl < 0)
            {
              ret = OB_ERROR;
              TBSYS_LOG(ERROR, "read from schema file failed");
            }

            if (rl == OB_MAX_PACKET_LENGTH)
            {
              TBSYS_LOG(ERROR, "schema file too large, size: %ld", OB_MAX_PACKET_LENGTH);
            }
          }

          if (ret == OB_SUCCESS)
          {
            ret = serialization::encode_vstr(log_data, OB_MAX_PACKET_LENGTH, pos, tmp_buffer, rl);
          }

          if (tmp_buffer != NULL)
          {
            ob_free(tmp_buffer);
            tmp_buffer = NULL;
          }

          fu.close();
        }
      }

      if (ret == OB_SUCCESS)
      {
        ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos, timestamp);
      }

      if (ret == OB_SUCCESS)
      {
        ret = flush_log(OB_RT_SCHEMA_SYNC, log_data, pos);
      }

      if (log_data != NULL)
      {
        ob_free(log_data);
        log_data = NULL;
      }

      return ret;
    }

    int ObRootLogWorker::regist_cs(const ObServer& server, const char* server_version, const int64_t timestamp)
    {
      return log_server_with_ts(OB_RT_CS_REGIST, server, server_version, timestamp);
    }
int ObRootLogWorker::regist_lms(const ObServer& server, int32_t sql_port, const char* server_version, const int64_t timestamp)
    {
      int ret = OB_SUCCESS;

      char* log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH, ObModIds::OB_RS_LOG_WORKER));
      if (log_data == NULL)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "allocate memory failed");
      }

      int64_t pos = 0;
      if (ret == OB_SUCCESS)
      {
        ret = server.serialize(log_data, OB_MAX_PACKET_LENGTH, pos);
      }

      if (ret == OB_SUCCESS)
      {
        ret = serialization::encode_vi32(log_data, OB_MAX_PACKET_LENGTH, pos, sql_port);
      }

      if (ret == OB_SUCCESS)
      {
        ret = serialization::encode_vstr(log_data, OB_MAX_PACKET_LENGTH, pos, server_version);
      }

      if (ret == OB_SUCCESS)
      {
        ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos, timestamp);
      }

      if (ret == OB_SUCCESS)
      {
        ret = flush_log(OB_RT_LMS_REGIST, log_data, pos);
      }

      if (log_data != NULL)
      {
        ob_free(log_data);
        log_data = NULL;
      }

      return ret;
    }

    int ObRootLogWorker::regist_ms(const ObServer& server, int32_t sql_port, const char* server_version, const int64_t timestamp)
    {
      int ret = OB_SUCCESS;

      char* log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH, ObModIds::OB_RS_LOG_WORKER));
      if (log_data == NULL)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "allocate memory failed");
      }

      int64_t pos = 0;
      if (ret == OB_SUCCESS)
      {
        ret = server.serialize(log_data, OB_MAX_PACKET_LENGTH, pos);
      }

      if (ret == OB_SUCCESS)
      {
        ret = serialization::encode_vi32(log_data, OB_MAX_PACKET_LENGTH, pos, sql_port);
      }

      if (ret == OB_SUCCESS)
      {
        ret = serialization::encode_vstr(log_data, OB_MAX_PACKET_LENGTH, pos, server_version);
      }

      if (ret == OB_SUCCESS)
      {
        ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos, timestamp);
      }

      if (ret == OB_SUCCESS)
      {
        ret = flush_log(OB_RT_MS_REGIST, log_data, pos);
      }

      if (log_data != NULL)
      {
        ob_free(log_data);
        log_data = NULL;
      }

      return ret;
    }

    int ObRootLogWorker::server_is_down(const ObServer& server, const int64_t timestamp)
    {
      return log_server_with_ts(OB_RT_SERVER_DOWN, server, "", timestamp);
    }

    int ObRootLogWorker::log_server_with_ts(const LogCommand cmd, const ObServer& server, const char* server_version, const int64_t timestamp)
    {
      int ret = OB_SUCCESS;

      char* log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH, ObModIds::OB_RS_LOG_WORKER));
      if (log_data == NULL)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "allocate memory failed");
      }

      int64_t pos = 0;
      if (ret == OB_SUCCESS)
      {
        ret = server.serialize(log_data, OB_MAX_PACKET_LENGTH, pos);
      }

      if (ret == OB_SUCCESS)
      {
        ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos, timestamp);
      }

      if (ret == OB_SUCCESS)
      {
        ret = serialization::encode_vstr(log_data, OB_MAX_PACKET_LENGTH, pos, server_version);
      }


      if (ret == OB_SUCCESS)
      {
        ret = flush_log(cmd, log_data, pos);
      }

      if (log_data != NULL)
      {
        ob_free(log_data);
        log_data = NULL;
      }

      return ret;
    }

    int ObRootLogWorker::report_cs_load(const ObServer& server, const int64_t capacity, const int64_t used)
    {
      int ret = OB_SUCCESS;

      char* log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH, ObModIds::OB_RS_LOG_WORKER));
      if (log_data == NULL)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "allocate memory failed");
      }

      int64_t pos = 0;
      if (ret == OB_SUCCESS)
      {
        ret = server.serialize(log_data, OB_MAX_PACKET_LENGTH, pos);
      }

      if (ret == OB_SUCCESS)
      {
        ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos, capacity);
      }

      if (ret == OB_SUCCESS)
      {
        ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos, used);
      }

      if (ret == OB_SUCCESS)
      {
        ret = flush_log(OB_RT_CS_LOAD_REPORT, log_data, pos);
      }

      if (log_data != NULL)
      {
        ob_free(log_data);
        log_data = NULL;
      }

      return ret;
    }

    int ObRootLogWorker::cs_migrate_done(const int32_t result, const ObDataSourceDesc& desc,
        const int64_t occupy_size, const uint64_t crc_sum, const uint64_t row_checksum, const int64_t row_count)
    {
      int ret = OB_SUCCESS;

      char* log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH, ObModIds::OB_RS_LOG_WORKER));
      if (log_data == NULL)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "allocate memory failed");
      }

      int64_t pos = 0;
      if (OB_SUCCESS == ret
          && OB_SUCCESS == (ret = desc.range_.serialize(log_data, OB_MAX_PACKET_LENGTH, pos))
          && OB_SUCCESS == (ret = desc.src_server_.serialize(log_data, OB_MAX_PACKET_LENGTH, pos))
          && OB_SUCCESS == (ret = desc.dst_server_.serialize(log_data, OB_MAX_PACKET_LENGTH, pos))
          && OB_SUCCESS == (ret = serialization::encode_bool(log_data, OB_MAX_PACKET_LENGTH, pos, desc.keep_source_))
          && OB_SUCCESS == (ret = serialization::encode_i64(log_data, OB_MAX_PACKET_LENGTH, pos, desc.tablet_version_))
          && OB_SUCCESS == (ret = serialization::encode_i32(log_data, OB_MAX_PACKET_LENGTH, pos, static_cast<int32_t>(desc.type_)))
          && OB_SUCCESS == (ret = serialization::encode_i64(log_data, OB_MAX_PACKET_LENGTH, pos, desc.sstable_version_))
          && OB_SUCCESS == (ret = desc.uri_.serialize(log_data, OB_MAX_PACKET_LENGTH, pos))
          && OB_SUCCESS == (ret = serialization::encode_vi32(log_data, OB_MAX_PACKET_LENGTH, pos, result))
          && OB_SUCCESS == (ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos, occupy_size))
          && OB_SUCCESS == (ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos, crc_sum))
          && OB_SUCCESS == (ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos, row_checksum))
          && OB_SUCCESS == (ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos, row_count))
         )
      {
        //ok
      }
      else
      {
        TBSYS_LOG(WARN, "failed to serialize migrate done, ret=%d", ret);
      }


      if (ret == OB_SUCCESS)
      {
        ret = flush_log(OB_RT_CS_MIGRATE_DONE, log_data, pos);
      }

      if (log_data != NULL)
      {
        ob_free(log_data);
        log_data = NULL;
      }

      return ret;
    }
int ObRootLogWorker::add_range_for_load_data(const common::ObTabletReportInfoList& tablets)
    {
      int ret = OB_SUCCESS;

      char* log_data = NULL;
      log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH, ObModIds::OB_RS_LOG_WORKER));
      if (log_data == NULL)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "allocate memory failed");
      }

      int64_t pos = 0;
      if (ret == OB_SUCCESS)
      {
        ret = tablets.serialize(log_data, OB_MAX_PACKET_LENGTH, pos);
      }

      if (ret == OB_SUCCESS)
      {
        ret = flush_log(OB_RT_ADD_RANGE_FOR_LOAD_DATA, log_data, pos);
      }

      if (log_data != NULL)
      {
        ob_free(log_data);
        log_data = NULL;
      }

      return ret;
    }

    int ObRootLogWorker::report_tablets(const common::ObServer& server, const common::ObTabletReportInfoList& tablets,
        const int64_t timestamp)
    {
      int ret = OB_SUCCESS;
      char* log_data = NULL;
      log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH, ObModIds::OB_RS_LOG_WORKER));
      if (log_data == NULL)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "allocate memory failed");
      }

      int64_t pos = 0;
      if (ret == OB_SUCCESS)
      {
        ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos, timestamp);
      }

      if (ret == OB_SUCCESS)
      {
        ret = server.serialize(log_data, OB_MAX_PACKET_LENGTH, pos);
      }

      if (ret == OB_SUCCESS)
      {
        ret = tablets.serialize(log_data, OB_MAX_PACKET_LENGTH, pos);
      }

      if (ret == OB_SUCCESS)
      {
        ret = flush_log(OB_RT_REPORT_TABLETS, log_data, pos);
      }

      if (log_data != NULL)
      {
        ob_free(log_data);
        log_data = NULL;
      }

      return ret;
    }

    int ObRootLogWorker::delete_replicas(const ObServer& server, const ObTabletReportInfoList& replicas)
    {
      int ret = OB_SUCCESS;
      char* log_data = NULL;
      log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH, ObModIds::OB_RS_LOG_WORKER));
      if (log_data == NULL)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "allocate memory failed");
      }
      else
      {
        int64_t pos = 0;
        if (OB_SUCCESS != (ret = server.serialize(log_data, OB_MAX_PACKET_LENGTH, pos)))
        {
          TBSYS_LOG(WARN, "failed to serialize server info");
        }
        else if (OB_SUCCESS != (ret = replicas.serialize(log_data, OB_MAX_PACKET_LENGTH, pos)))
        {
          TBSYS_LOG(WARN, "failed to serialize replicas list");
        }
        else if (OB_SUCCESS != (ret = flush_log(OB_RT_CS_DELETE_REPLICAS, log_data, pos)))
        {
          TBSYS_LOG(WARN, "failed to flush log, err=%d", ret);
        }
      }
      if (NULL != log_data)
      {
        ob_free(log_data);
        log_data = NULL;
      }
      return ret;
    }

    int ObRootLogWorker::remove_replica(const ObTabletReportInfo &replica)
    {
      int ret = OB_SUCCESS;
      char* log_data = NULL;
      log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH, ObModIds::OB_RS_LOG_WORKER));
      if (log_data == NULL)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "allocate memory failed");
      }
      else
      {
        int64_t pos = 0;
        if (OB_SUCCESS != (ret = replica.serialize(log_data, OB_MAX_PACKET_LENGTH, pos)))
        {
          TBSYS_LOG(WARN, "failed to serialize");
        }
        else if (OB_SUCCESS != (ret = flush_log(OB_RT_REMOVE_REPLICA, log_data, pos)))
        {
          TBSYS_LOG(WARN, "failed to flush log, err=%d", ret);
        }
      }
      if (NULL != log_data)
      {
        ob_free(log_data);
        log_data = NULL;
      }
      return ret;
    }

    int ObRootLogWorker::remove_table(const common::ObArray<uint64_t> &deleted_tables)
    {
      int ret = OB_SUCCESS;
      char* log_data = NULL;
      log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH, ObModIds::OB_RS_LOG_WORKER));
      int64_t pos = 0;
      if (log_data == NULL)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "allocate memory failed");
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos, deleted_tables.count())))
      {
        TBSYS_LOG(WARN, "failed to serialize");
      }
      else
      {
        for (int32_t i = 0; i < deleted_tables.count(); ++i)
        {
          if (OB_SUCCESS != (ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos, deleted_tables.at(i))))
          {
            TBSYS_LOG(WARN, "failed to serialize");
            break;
          }
        }
        if (OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = flush_log(OB_RT_REMOVE_TABLE, log_data, pos)))
          {
            TBSYS_LOG(WARN, "failed to flush log, err=%d", ret);
          }
        }
      }
      if (NULL != log_data)
      {
        ob_free(log_data);
        log_data = NULL;
      }
      return ret;
    }

    int ObRootLogWorker::add_load_table(const ObString& table_name, const uint64_t table_id, const uint64_t old_table_id,
        const ObString& uri, const int64_t start_time, const int64_t tablet_version)
    {
      int ret = OB_SUCCESS;
      char* log_data = NULL;
      log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH, ObModIds::OB_RS_LOG_WORKER));
      int64_t pos = 0;
      if (log_data == NULL)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "allocate memory failed");
      }
      else if (OB_SUCCESS != (ret = table_name.serialize(log_data, OB_MAX_PACKET_LENGTH, pos)))
      {
        TBSYS_LOG(WARN, "failed to serialize table name=%.*s, ret=%d", table_name.length(), table_name.ptr(), ret);
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos, table_id)))
      {
        TBSYS_LOG(WARN, "failed to serialize tableid=%lu, ret=%d", table_id, ret);
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos, old_table_id)))
      {
        TBSYS_LOG(WARN, "failed to serialize old tableid=%lu, ret=%d", old_table_id, ret);
      }
      else if (OB_SUCCESS != (ret = uri.serialize(log_data, OB_MAX_PACKET_LENGTH, pos)))
      {
        TBSYS_LOG(WARN, "failed to serialize uri=%.*s, ret=%d", uri.length(), uri.ptr(), ret);
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos, start_time)))
      {
        TBSYS_LOG(WARN, "failed to serialize start_time=%ld, ret=%d", start_time, ret);
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos, tablet_version)))
      {
        TBSYS_LOG(WARN, "failed to serialize tablet_version=%ld, ret=%d", tablet_version, ret);
      }
      else if (OB_SUCCESS != (ret = flush_log(OB_RT_ADD_LOAD_TABLE, log_data, pos)))
      {
        TBSYS_LOG(WARN, "failed to flush log, err=%d", ret);
      }
      if (NULL != log_data)
      {
        ob_free(log_data);
        log_data = NULL;
      }
      return ret;
    }

    int ObRootLogWorker::set_load_table_status(const uint64_t table_id, const int32_t status, const int64_t end_time)
    {
      int ret = OB_SUCCESS;
      char* log_data = NULL;
      log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH, ObModIds::OB_RS_LOG_WORKER));
      int64_t pos = 0;
      if (log_data == NULL)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "allocate memory failed");
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos, table_id)))
      {
        TBSYS_LOG(WARN, "failed to serialize tableid=%lu, ret=%d", table_id, ret);
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi32(log_data, OB_MAX_PACKET_LENGTH, pos, status)))
      {
        TBSYS_LOG(WARN, "failed to serialize status=%d, ret=%d", status, ret);
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos, end_time)))
      {
        TBSYS_LOG(WARN, "failed to serialize end_time=%ld, ret=%d", end_time, ret);
      }
      else if (OB_SUCCESS != (ret = flush_log(OB_RT_SET_LOAD_TABLE_STATUS, log_data, pos)))
      {
        TBSYS_LOG(WARN, "failed to flush log, err=%d", ret);
      }
      if (NULL != log_data)
      {
        ob_free(log_data);
        log_data = NULL;
      }
      return ret;
    }

    int ObRootLogWorker::add_new_tablet(const ObTabletInfo tablet, const ObArray<int32_t> &chunkservers, const int64_t mem_version)
    {
      int ret = OB_SUCCESS;

      char* log_data = NULL;
      log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH, ObModIds::OB_RS_LOG_WORKER));
      if (log_data == NULL)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "allocate memory failed");
      }

      int64_t pos = 0;
      if (ret == OB_SUCCESS)
      {
        ret = serialization::encode_vi32(log_data, OB_MAX_PACKET_LENGTH, pos,
            static_cast<int32_t>(chunkservers.count()));
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to encode chunkserver count. ret=%d", ret);
        }
      }

      if (ret == OB_SUCCESS)
      {
        ret = tablet.serialize(log_data, OB_MAX_PACKET_LENGTH, pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to serialize tablet, ret=%d", ret);
        }
      }

      if (ret == OB_SUCCESS)
      {
        for (int32_t i=0; i < chunkservers.count(); ++i)
        {
          ret = serialization::encode_vi32(log_data, OB_MAX_PACKET_LENGTH, pos, chunkservers.at(i));
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "serialize failed");
            break;
          }
        }
      }

      if (ret == OB_SUCCESS)
      {
        ret = serialization::encode_vi32(log_data, OB_MAX_PACKET_LENGTH, pos, static_cast<int32_t>(mem_version));
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to encode mem_version. ret=%d", ret);
        }
      }

      if (ret == OB_SUCCESS)
      {
        ret = flush_log(OB_RT_ADD_NEW_TABLET, log_data, pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to flush log to buf. ret=%d", ret);
        }
      }
      if (log_data != NULL)
      {
        ob_free(log_data);
        log_data = NULL;
      }
      return ret;
    }

    int ObRootLogWorker::batch_add_new_tablet(const common::ObTabletInfoList& tablets,
                                              int** server_indexs, int* count, const int64_t mem_version)
    {
      int ret = OB_SUCCESS;

      char* log_data = NULL;
      log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH, ObModIds::OB_RS_LOG_WORKER));
      if (log_data == NULL)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "allocate memory failed");
      }

      int64_t pos = 0;
      int64_t index = tablets.tablet_list.get_array_index();
      ObTabletInfo *p_table_info = NULL;
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos, index);
      }
      if (OB_SUCCESS == ret)
      {
        for (int64_t i = 0; OB_SUCCESS == ret && i < index; i++)
        {
          p_table_info = tablets.tablet_list.at(i);
          if (NULL == p_table_info)
          {
            TBSYS_LOG(WARN, "p_table_info should not be NULL");
            ret = OB_ERROR;
          }
          if (ret == OB_SUCCESS)
          {
            ret = serialization::encode_vi32(log_data, OB_MAX_PACKET_LENGTH, pos, count[i]);
          }

          if (ret == OB_SUCCESS)
          {
            ret = p_table_info->serialize(log_data, OB_MAX_PACKET_LENGTH, pos);
          }

          if (ret == OB_SUCCESS)
          {
            for (int j = 0; j < count[i]; ++j)
            {
              ret = serialization::encode_vi32(log_data, OB_MAX_PACKET_LENGTH, pos, server_indexs[i][j]);
              if (ret != OB_SUCCESS)
              {
                TBSYS_LOG(ERROR, "serialize failed");
                break;
              }
            }
          }
        }
      }

      if (ret == OB_SUCCESS)
      {
        ret = serialization::encode_vi32(log_data, OB_MAX_PACKET_LENGTH, pos, static_cast<int32_t>(mem_version));
      }

      if (ret == OB_SUCCESS)
      {
        ret = flush_log(OB_RT_BATCH_ADD_NEW_TABLET, log_data, pos);
      }

      if (log_data != NULL)
      {
        ob_free(log_data);
        log_data = NULL;
      }

      return ret;
    }

    int ObRootLogWorker::cs_merge_over(const ObServer& server, const int64_t timestamp)
    {
      return log_server_with_ts(OB_RT_CS_MERGE_OVER, server, "hn server_version null", timestamp);
    }

    int ObRootLogWorker::init_first_meta(const ObTabletMetaTableRow &first_meta_row)
    {
      int ret = OB_SUCCESS;

      char* log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH, ObModIds::OB_RS_LOG_WORKER));
      if (log_data == NULL)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "allocate memory failed");
      }

      int64_t pos = 0;
      if (ret == OB_SUCCESS)
      {
        ret = first_meta_row.serialize(log_data, OB_MAX_PACKET_LENGTH, pos);
      }

      if (ret == OB_SUCCESS)
      {
        ret = flush_log(OB_RT_SYNC_FIRST_META_ROW, log_data, pos);
      }

      if (log_data != NULL)
      {
        ob_free(log_data);
        log_data = NULL;
      }
      return ret;
    }

int ObRootLogWorker::clean_root_table()
{
  int ret = OB_SUCCESS;

  int64_t pos = 0;
  int64_t unused_value = 0;
  char* log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH, ObModIds::OB_RS_LOG_WORKER));
  if (log_data == NULL)
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TBSYS_LOG(ERROR, "allocate memory failed");
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos, unused_value)))
  {
    TBSYS_LOG(ERROR, "Seriliaze config version failed! ret: [%d]", ret);
  }

  if (ret == OB_SUCCESS)
  {
    ret = flush_log(OB_RT_CLEAN_ROOT_TABLE, log_data, pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to flush log for cmd clean_root_table, ret=%d", ret);
    }
  }

  if (log_data != NULL)
  {
    ob_free(log_data);
    log_data = NULL;
  }
  return ret;
}


    int ObRootLogWorker::got_config_version(int64_t config_version)
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      char* log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH, ObModIds::OB_RS_LOG_WORKER));

      if (log_data == NULL)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "allocate memory failed");
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos, config_version)))
      {
        TBSYS_LOG(ERROR, "Seriliaze config version failed! ret: [%d]", ret);
      }
      else if (OB_SUCCESS != (ret = flush_log(OB_RT_GOT_CONFIG_VERSION, log_data, pos)))
      {
        TBSYS_LOG(ERROR, "Flush log failed! ret: [%d]", ret);
      }

      if (log_data != NULL)
      {
        ob_free(log_data);
        log_data = NULL;
      }
      return ret;
    }

    int ObRootLogWorker::sync_us_frozen_version(const int64_t frozen_version, const int64_t last_frozen_time)
    {
      int ret = OB_SUCCESS;

      char* log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH, ObModIds::OB_RS_LOG_WORKER));
      if (log_data == NULL)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "allocate memory failed");
      }

      int64_t pos = 0;
      if (ret == OB_SUCCESS)
      {
        ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos,frozen_version);
      }

      if (ret == OB_SUCCESS)
      {
        ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos,last_frozen_time);
      }

      if (ret == OB_SUCCESS)
      {
        ret = flush_log(OB_RT_SYNC_FROZEN_VERSION_AND_TIME, log_data, pos);
      }

      if (log_data != NULL)
      {
        ob_free(log_data);
        log_data = NULL;
      }
      return ret;
    }

    int ObRootLogWorker::log_server(const LogCommand cmd, const ObServer& server)
    {
      int ret = OB_SUCCESS;

      char* log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH, ObModIds::OB_RS_LOG_WORKER));
      if (log_data == NULL)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "allocate memory failed");
      }

      int64_t pos = 0;
      if (ret == OB_SUCCESS)
      {
        ret = server.serialize(log_data, OB_MAX_PACKET_LENGTH, pos);
      }

      if (ret == OB_SUCCESS)
      {
        ret = flush_log(cmd, log_data, pos);
      }

      if (log_data != NULL)
      {
        ob_free(log_data);
        log_data = NULL;
      }

      return ret;
    }

    int ObRootLogWorker::set_ups_list(const common::ObUpsList &ups_list)
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      char* log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH, ObModIds::OB_RS_LOG_WORKER));

      if (NULL == log_data)
      {
        TBSYS_LOG(ERROR, "no memory");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (OB_SUCCESS != (ret = ups_list.serialize(log_data, OB_MAX_PACKET_LENGTH, pos)))
      {
        TBSYS_LOG(ERROR, "serialize error");
      }
      else
      {
        ret = flush_log(OB_RT_SET_UPS_LIST, log_data, pos);
      }
      if (NULL != log_data)
      {
        ob_free(log_data);
        log_data = NULL;
      }
      return ret;
    }

    int ObRootLogWorker::flush_log(const LogCommand cmd, const char* log_data, const int64_t& serialize_size)
    {
      int ret = OB_SUCCESS;
      //add pangtianze [Paxos rs_election] 20150619:b
      if (ret != OB_SUCCESS)
      //add:e
      {
        TBSYS_LOG(DEBUG, "flush update log, cmd type: %d", cmd);

        tbsys::CThreadGuard guard(log_manager_->get_log_sync_mutex());
        ret = log_manager_->write_and_flush_log(cmd, log_data, serialize_size);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "write and flush log failed:cmd[%d], log[%s], size[%ld], ret[%d]",
              cmd, log_data, serialize_size, ret);
          // coredump
          abort();
        }
      }
      return ret;
    }

    //////////////////////////////////////////////////
    ///// slave apply log methods
    //////////////////////////////////////////////////

    int ObRootLogWorker::apply(common::LogCommand cmd, const char* log_data, const int64_t& data_len)
    {
      int ret = OB_SUCCESS;

      TBSYS_LOG(INFO, "start replay log, cmd type: %d", cmd);
      switch (cmd)
      {
        case OB_RT_SCHEMA_SYNC:
          ret = do_schema_sync(log_data, data_len);
          break;
        case OB_RT_CS_REGIST:
          ret = do_cs_regist(log_data, data_len);
          break;
        case OB_RT_LMS_REGIST:
          ret = do_lms_regist(log_data, data_len);
          break;
        case OB_RT_MS_REGIST:
          ret = do_ms_regist(log_data, data_len);
          break;
        case OB_RT_SERVER_DOWN:
          ret = do_server_down(log_data, data_len);
          break;
        case OB_RT_CS_LOAD_REPORT:
          ret = do_cs_load_report(log_data, data_len);
          break;
        case OB_RT_CS_MIGRATE_DONE:
          ret = do_cs_migrate_done(log_data, data_len);
          break;
        case OB_RT_REPORT_TABLETS:
          ret = do_report_tablets(log_data, data_len);
          break;
        case OB_RT_ADD_RANGE_FOR_LOAD_DATA:
          ret = do_add_range_for_load_data(log_data, data_len);
          break;
        case OB_RT_ADD_NEW_TABLET:
          ret = do_add_new_tablet(log_data, data_len);
          break;
        case OB_RT_BATCH_ADD_NEW_TABLET:
          ret = do_batch_add_new_tablet(log_data, data_len);
          break;
        case OB_RT_CREATE_TABLE_DONE:
          ret = do_create_table_done();
          break;
        case OB_RT_BEGIN_BALANCE:
          ret = do_begin_balance();
          break;
        case OB_RT_BALANCE_DONE:
          ret = do_balance_done();
          break;
        case OB_RT_CS_MERGE_OVER:
          ret = do_cs_merge_over(log_data, data_len);
          break;
        case OB_LOG_CHECKPOINT:
          ret = do_check_point(log_data, data_len);
          break;
        case OB_RT_SYNC_FROZEN_VERSION:
          ret = do_sync_frozen_version(log_data,data_len);
          break;
        case OB_RT_SYNC_FROZEN_VERSION_AND_TIME:
          ret = do_sync_frozen_version_and_time(log_data, data_len);
          break;
        case OB_RT_SYNC_FIRST_META_ROW:
          ret = do_init_first_meta_row(log_data, data_len);
          break;
        case OB_RT_SET_UPS_LIST:
          ret = do_set_ups_list(log_data, data_len);
          break;
       case OB_RT_SET_BYPASS_VERSION:
          ret = do_set_bypass_version(log_data, data_len);
          break;
        case OB_RT_REMOVE_REPLICA:
          ret = do_remove_replica(log_data, data_len);
          break;
        case OB_RT_CS_DELETE_REPLICAS:
          ret = do_delete_replicas(log_data, data_len);
          break;
        case OB_RT_REMOVE_TABLE:
          ret = do_remove_table(log_data, data_len);
          break;
        case OB_LOG_SWITCH_LOG:
          TBSYS_LOG(INFO, "apply: switch_log");
          break;
        case OB_RT_CLEAN_ROOT_TABLE:
          ret = do_clean_root_table(log_data, data_len);
          break;
        case OB_RT_GOT_CONFIG_VERSION:
          ret = do_got_config_version(log_data, data_len);
          break;
        case OB_RT_ADD_LOAD_TABLE:
          ret = do_add_load_table(log_data, data_len);
          break;
        case OB_RT_SET_LOAD_TABLE_STATUS:
          ret = do_set_load_table_status(log_data, data_len);
          break;
        default:
          TBSYS_LOG(WARN, "unknow log command [%d]", cmd);
          ret = OB_INVALID_ARGUMENT;
          break;
      }
      return ret;
    }

    int ObRootLogWorker::do_check_point(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;

      int64_t pos = 0;
      int64_t ckpt_id;

      ret = serialization::decode_i64(log_data, log_length, pos, &ckpt_id);

      if (ret == OB_SUCCESS)
      {
        if (root_server_->is_master())
        {
          TBSYS_LOG(WARN, "this is master, may have lost checkpointing, ckpt=%ld", ckpt_id);
        }
        else
        {
          ret = log_manager_->do_check_point(ckpt_id);
        }
      }

      return ret;
    }

    int ObRootLogWorker::do_schema_sync(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;
      UNUSED(log_data);
      UNUSED(log_length);
      return ret;
    }

    int ObRootLogWorker::do_cs_regist(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;

      int64_t pos = 0;
      int64_t csr_ts = 0;
      const char* server_version = NULL;
      int64_t server_version_length = 0;

      ObServer server;
      ret = server.deserialize(log_data, log_length, pos);

      if (ret == OB_SUCCESS)
      {
        ret = serialization::decode_vi64(log_data, log_length, pos, &csr_ts);
      }

      if (ret == OB_SUCCESS)
      {
        server_version = serialization::decode_vstr(log_data, log_length, pos, &server_version_length);
        if (NULL == server_version)
        {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "decode vstr error");
        }
      }

      if (ret == OB_SUCCESS)
      {
        int32_t status = 0; // we don't care this
        ret = root_server_->regist_chunk_server(server, server_version, status, csr_ts);
      }

      return ret;
    }
    int ObRootLogWorker::do_lms_regist(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;

      int64_t pos = 0;
      int32_t sql_port = 0;
      int64_t msr_ts = 0;
      const char* server_version = NULL;
      int64_t server_version_length = 0;

      ObServer server;
      ret = server.deserialize(log_data, log_length, pos);

      if (ret == OB_SUCCESS)
      {
        ret = serialization::decode_vi32(log_data, log_length, pos, &sql_port);
      }

      if (ret == OB_SUCCESS)
      {
        server_version = serialization::decode_vstr(log_data, log_length, pos, &server_version_length);
        if (server_version == NULL)
        {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "decode vstr error");
        }
      }

      if (ret == OB_SUCCESS)
      {
        ret = serialization::decode_vi64(log_data, log_length, pos, &msr_ts);
      }

      if (ret == OB_SUCCESS)
      {
        ret = root_server_->regist_merge_server(server, sql_port, true, server_version, msr_ts);
      }

      return ret;
    }


    int ObRootLogWorker::do_ms_regist(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;

      int64_t pos = 0;
      int32_t sql_port = 0;
      int64_t msr_ts = 0;
      const char* server_version = NULL;
      int64_t server_version_length = 0;

      ObServer server;
      ret = server.deserialize(log_data, log_length, pos);

      if (ret == OB_SUCCESS)
      {
        ret = serialization::decode_vi32(log_data, log_length, pos, &sql_port);
      }

      if (ret == OB_SUCCESS)
      {
        server_version = serialization::decode_vstr(log_data, log_length, pos, &server_version_length);
        if (server_version == NULL)
        {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "decode vstr error");
        }
      }

      if (ret == OB_SUCCESS)
      {
        ret = serialization::decode_vi64(log_data, log_length, pos, &msr_ts);
      }

      if (ret == OB_SUCCESS)
      {
        // all logged server are not listen merge server
        ret = root_server_->regist_merge_server(server, sql_port, false, server_version, msr_ts);
      }

      return ret;
    }

    int ObRootLogWorker::do_server_down(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;

      int64_t pos = 0;
      int64_t sd_ts = 0;

      ObServer server;
      ret = server.deserialize(log_data, log_length, pos);

      if (ret == OB_SUCCESS)
      {
        ret = serialization::decode_vi64(log_data, log_length, pos, &sd_ts);
      }

      if (ret == OB_SUCCESS)
      {
        ObChunkServerManager::iterator it = root_server_->server_manager_.find_by_ip(server);
        if (it != NULL)
        {
          it->status_ = ObServerStatus::STATUS_DEAD;
          tbsys::CRLockGuard guard(root_server_->root_table_rwlock_);
          if (root_server_->root_table_ != NULL)
          {
            root_server_->root_table_->server_off_line(static_cast<int32_t>(it - root_server_->server_manager_.begin()), sd_ts);
            ret = OB_SUCCESS;
          }
        }
      }

      return ret;
    }

    int ObRootLogWorker::do_cs_load_report(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;

      ObServer server;
      int64_t pos = 0;
      int64_t capacity = 0;
      int64_t used = 0;

      ret = server.deserialize(log_data, log_length, pos);

      if (ret == OB_SUCCESS)
      {
        ret = serialization::decode_vi64(log_data, log_length, pos, &capacity);
      }

      if (ret == OB_SUCCESS)
      {
        ret = serialization::decode_vi64(log_data, log_length, pos, &used);
      }

      if (ret == OB_SUCCESS)
      {
        //ignore return value
        root_server_->update_capacity_info(server, capacity, used);
      }

      return ret;
    }

    int ObRootLogWorker::do_cs_migrate_done(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;

      int64_t pos = 0;


      int32_t result = OB_SUCCESS;
      ObDataSourceDesc desc;
      int64_t occupy_size = 0;
      uint64_t crc_sum = 0;
      int64_t row_checksum = 0;
      int64_t row_count = 0;
      int32_t type = 0;

       if(OB_SUCCESS == ret
          && OB_SUCCESS == (ret = desc.range_.deserialize(log_data, log_length, pos))
          && OB_SUCCESS == (ret = desc.src_server_.deserialize(log_data, log_length, pos))
          && OB_SUCCESS == (ret = desc.dst_server_.deserialize(log_data, log_length, pos))
          && OB_SUCCESS == (ret = serialization::decode_bool(log_data, log_length, pos, &desc.keep_source_))
          && OB_SUCCESS == (ret = serialization::decode_i64(log_data, log_length, pos, &desc.tablet_version_)))
       {
         //ok
         desc.type_ = ObDataSourceDesc::OCEANBASE_INTERNAL;//before bypass version of ob, all migrate if in oceanbase
       }
       else
       {
         TBSYS_LOG(WARN, "failed to deserialize part1 of migrate done info, ret=%d", ret);
       }

       if (log_length == pos)
       {
         TBSYS_LOG(INFO, "for compatible old commit log");
       }
       else if(OB_SUCCESS == (ret = serialization::decode_i32(log_data, log_length, pos, &type))
          && OB_SUCCESS == (ret = serialization::decode_i64(log_data, log_length, pos, &desc.sstable_version_))
          && OB_SUCCESS == (ret = desc.uri_.deserialize(log_data, log_length, pos))
          && OB_SUCCESS == (ret = serialization::decode_vi32(log_data, log_length, pos, &result))
          && OB_SUCCESS == (ret = serialization::decode_vi64(log_data, log_length, pos, &occupy_size))
          && OB_SUCCESS == (ret = serialization::decode_vi64(log_data, log_length, pos, reinterpret_cast<int64_t*>(&crc_sum)))
          && OB_SUCCESS == (ret = serialization::decode_vi64(log_data, log_length, pos, reinterpret_cast<int64_t*>(&row_checksum)))
          && OB_SUCCESS == (ret = serialization::decode_vi64(log_data, log_length, pos, &row_count)))
       {
         // ok
         desc.type_ = static_cast<ObDataSourceDesc::ObDataSourceType>(result);
       }
       else
       {
         TBSYS_LOG(WARN, "failed to deserialize part2 of migrate done, ret=%d", ret);
       }

      if (ret == OB_SUCCESS)
      {
        ret = root_server_->migrate_over(result, desc,
            occupy_size, crc_sum, row_checksum, row_count);
        if (OB_ENTRY_NOT_EXIST == ret)
        {
          ret = OB_SUCCESS;
        }
      }

      return ret;
    }
int ObRootLogWorker::do_add_range_for_load_data(const char* log_data, const int64_t& log_length)
{
  int ret = OB_SUCCESS;

  int64_t pos = 0;
  const bool is_replay_log = true;
  ObTabletReportInfoList tablets;
  if (ret == OB_SUCCESS)
  {
    ret = tablets.deserialize(log_data, log_length, pos);
  }
  if (ret == OB_SUCCESS)
  {
    ret = root_server_->add_range_to_root_table(tablets, is_replay_log);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to report tablet for load data. ret=%d", ret);
    }
  }
  return ret;
}

    int ObRootLogWorker::do_report_tablets(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;

      int64_t pos = 0;
      ObServer server;
      ObTabletReportInfoList tablets;
      int64_t timestamp = 0;

      ret = serialization::decode_vi64(log_data, log_length, pos, &timestamp);

      if (ret == OB_SUCCESS)
      {
        ret = server.deserialize(log_data, log_length, pos);
      }
      if (ret == OB_SUCCESS)
      {
        ret = tablets.deserialize(log_data, log_length, pos);
      }

      if (ret == OB_SUCCESS)
      {
        root_server_->report_tablets(server, tablets, timestamp);
      }

      return ret;
    }

    int ObRootLogWorker::do_add_new_tablet(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      int64_t mem_version = 0;

      int count = 0;
      ObTabletInfo tablet;
      ObObj start_key;
      ObObj end_key;
      int64_t obj_count = 1;
      tablet.range_.start_key_.assign(&start_key, obj_count);
      tablet.range_.end_key_.assign(&end_key, obj_count);
      ObArray<int32_t> chunkservers;
      int32_t cs_id = OB_INVALID_INDEX;

      ret = serialization::decode_vi32(log_data, log_length, pos, &count);

      if (ret == OB_SUCCESS)
      {
        if (OB_SUCCESS != (ret = tablet.deserialize(log_data, log_length, pos)))
        {
          TBSYS_LOG(WARN, "fail to deserialize tablet. ret=%d", ret);
        }
      }

      if (ret == OB_SUCCESS && count > 0 )
      {
	    //add zhaoqiong[roottable tablet management]20150302:b
		//for (int i = 0; i < count && i < OB_SAFE_COPY_COUNT; i++)
        for (int i = 0; i < count && i < OB_MAX_COPY_COUNT; i++)
		//add e
        {
          ret = serialization::decode_vi32(log_data, log_length, pos, &cs_id);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "fail to decode cs_id. ret=%d", ret);
            break;
          }
          else if (OB_SUCCESS != (ret = chunkservers.push_back(cs_id)))
          {
            TBSYS_LOG(ERROR, "failed to push back into array, err=%d", ret);
            break;
          }
        }
      }
      if (ret == OB_SUCCESS)
      {
        if (OB_SUCCESS != (ret = serialization::decode_vi64(log_data, log_length, pos, &mem_version)))
        {
          TBSYS_LOG(WARN, "fail to decode mem_version. ret=%d", ret);
        }
      }
      if (ret == OB_SUCCESS)
      {
        if (OB_SUCCESS != (ret = root_server_->create_new_table(true, tablet, chunkservers, mem_version)))
        {
          TBSYS_LOG(WARN, "fail to replay create new table. err = %d", ret);
        }
      }
      else
      {
        TBSYS_LOG(WARN, "fail to serialize data to buff. ret=%d", ret);
      }

      return ret;
    }
    int ObRootLogWorker::do_batch_add_new_tablet(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;

      int64_t pos = 0;
      int64_t mem_version = 0;

      int64_t index = 0;
      ObTabletInfoList tablets;
      int32_t **server_index = NULL;
      int32_t * create_count = NULL;
      ret = serialization::decode_vi64(log_data, log_length, pos, &index);
      if (OB_SUCCESS == ret)
      {
        create_count = new (std::nothrow)int32_t[index];
        server_index = new (std::nothrow)int32_t*[index];
        if (NULL == server_index || NULL == create_count)
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          for (int32_t i = 0; i < index; i++)
          {
		    //add zhaoqiong[roottable tablet management]20150302:b
			//server_index[i] = new(std::nothrow) int32_t[OB_SAFE_COPY_COUNT];
            server_index[i] = new(std::nothrow) int32_t[OB_MAX_COPY_COUNT];
			//add e
            if (NULL == server_index[i])
            {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              break;
            }
          }
        }
      }
      ObTabletInfo *tablet_info = new(std::nothrow) ObTabletInfo[index];
      if (NULL == tablet_info)
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "new tablet_info fail.");
      }
      for (int64_t i = 0; OB_SUCCESS == ret && i < index; i++)
      {
        ret = serialization::decode_vi32(log_data, log_length, pos, &create_count[i]);
        if (ret == OB_SUCCESS)
        {
          ret = tablet_info[i].deserialize(log_data, log_length, pos);
        }

        if (ret == OB_SUCCESS && create_count[i] > 0 )
        {
		  //add zhaoqiong[roottable tablet management]20150302:b
		  //for (int j = 0; j < create_count[i] && j < OB_SAFE_COPY_COUNT; j++)
          for (int j = 0; j < create_count[i] && j < OB_MAX_COPY_COUNT; j++)
		  //add e
          {
            ret = serialization::decode_vi32(log_data, log_length, pos, server_index[i] + j);
            if (ret != OB_SUCCESS)
            {
              break;
            }
          }
        }
        if (OB_SUCCESS == ret)
        {
          tablets.add_tablet(tablet_info[i]);
        }
      }
      if (ret == OB_SUCCESS)
      {
        ret = serialization::decode_vi64(log_data, log_length, pos, &mem_version);
      }
      if (ret == OB_SUCCESS)
      {
        ret = root_server_->slave_batch_create_new_table(tablets, server_index,
            create_count, mem_version);
      }
      for (int64_t i = 0; i < index; i++)
      {
        if (NULL != server_index[i])
        {
          delete [] server_index[i];
        }
      }
      if (NULL != server_index)
      {
        delete [] server_index;
      }
      if (NULL != tablet_info)
      {
        delete [] tablet_info;
      }
      if (NULL != create_count)
      {
        delete [] create_count;
      }
      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(INFO, "do batch add new tablet success.");
      }
      return ret;
    }

    int ObRootLogWorker::do_remove_replica(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;
      ObTabletReportInfo tablet;
      ObObj start_rowkey_obj_array[OB_MAX_ROWKEY_COLUMN_NUMBER];
      ObObj end_rowkey_obj_array[OB_MAX_ROWKEY_COLUMN_NUMBER];
      tablet.tablet_info_.range_.start_key_.assign(start_rowkey_obj_array, OB_MAX_ROWKEY_COLUMN_NUMBER);
      tablet.tablet_info_.range_.end_key_.assign(end_rowkey_obj_array, OB_MAX_ROWKEY_COLUMN_NUMBER);
      int64_t pos = 0;
      if (OB_SUCCESS != (ret = tablet.deserialize(log_data, log_length, pos)))
      {
        TBSYS_LOG(WARN, "deserialize tablet failed:ret[%d]", ret);
      }
      else
      {
        // replay log
        ret = root_server_->remove_replica(true, tablet);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "replay removed replica failed:ret[%d]", ret);
        }
      }
      return ret;
    }

    int ObRootLogWorker::do_delete_replicas(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;
      ObServer server;
      ObTabletReportInfoList replicas;
      int64_t pos = 0;
      ret = server.deserialize(log_data, log_length, pos);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "deserialize server failed:ret[%d]", ret);
      }
      else
      {
        if (OB_SUCCESS != (ret = replicas.deserialize(log_data, log_length, pos)))
        {
          TBSYS_LOG(WARN, "deserialize error");
        }
        else if (OB_SUCCESS != (ret = root_server_->delete_replicas(true, server, replicas)))
        {
          TBSYS_LOG(ERROR, "replay remove replica error, err=%d", ret);
        }
      }
      return ret;
    }

    int ObRootLogWorker::do_remove_table(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;
      int64_t table_count = 0;
      int64_t pos = 0;
      if (OB_SUCCESS != (ret = serialization::decode_vi64(log_data, log_length, pos, &table_count)))
      {
        TBSYS_LOG(WARN, "failed to serialize table count");
      }
      else
      {
        ObArray<uint64_t> list;
        int64_t table_id = 0;
        for (int64_t i = 0; i < table_count; ++i)
        {
          if (OB_SUCCESS != (ret = serialization::decode_vi64(log_data, log_length, pos, &table_id)))
          {
            TBSYS_LOG(WARN, "failed to serialize table id");
            break;
          }
          else
          {
            ret = list.push_back(table_id);
            if (ret != OB_SUCCESS)
            {
              TBSYS_LOG(WARN, "add table id failed:table_id[%lu], ret[%d]", table_id, ret);
              break;
            }
          }
        }
        if (OB_SUCCESS == ret)
        {
          TBSYS_LOG(INFO, "delete table from root table in replay commit log");
          ret = root_server_->delete_tables(true, list);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "drop root table table failed:table_id[%lu], ret[%d]", table_id, ret);
          }
        }
      }
      return ret;
    }

    int ObRootLogWorker::do_add_load_table(const char* log_data, const int64_t& log_length)
    {
      OB_ASSERT(balancer_);
      int ret = OB_SUCCESS;
      ObString table_name;
      uint64_t table_id = 0;
      uint64_t old_table_id = 0;
      ObString uri;
      int64_t start_time = 0;
      int64_t tablet_version = 0;
      int64_t pos = 0;
      if (OB_SUCCESS != (ret = table_name.deserialize(log_data, log_length, pos)))
      {
        TBSYS_LOG(WARN, "failed to deserialize table name, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::decode_vi64(log_data, log_length, pos, reinterpret_cast<int64_t*>(&table_id))))
      {
        TBSYS_LOG(WARN, "failed to deserialize table id, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::decode_vi64(log_data, log_length, pos, reinterpret_cast<int64_t*>(&old_table_id))))
      {
        TBSYS_LOG(WARN, "failed to deserialize old table id, ret=%d", ret);
      }
      else if(OB_SUCCESS != (ret = uri.deserialize(log_data, log_length, pos)))
      {
        TBSYS_LOG(WARN, "failed to deserialize uri, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::decode_vi64(log_data, log_length, pos, &start_time)))
      {
        TBSYS_LOG(WARN, "failed to deserialize start_time, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::decode_vi64(log_data, log_length, pos, &tablet_version)))
      {
        TBSYS_LOG(WARN, "failed to deserialize tablet_version, ret=%d", ret);
      }
      else
      {
        ret = balancer_->add_load_table_from_log(table_name, table_id, old_table_id, uri, start_time, tablet_version);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "add load table failed:table_name=%.*s table_id=%lu uri=%.*s",
              table_name.length(), table_name.ptr(), table_id, uri.length(), uri.ptr());
        }
        else
        {
          TBSYS_LOG(INFO, "add load table: table_name=%.*s table_id=%lu uri=%.*s from replay commit log",
              table_name.length(), table_name.ptr(), table_id, uri.length(), uri.ptr());
        }
      }
      return ret;
    }

    int ObRootLogWorker::do_set_load_table_status(const char* log_data, const int64_t& log_length)
    {
      OB_ASSERT(balancer_);
      int ret = OB_SUCCESS;
      uint64_t table_id = 0;
      int32_t status = 0;
      int64_t end_time = 0;
      int64_t pos = 0;
      if (OB_SUCCESS != (ret = serialization::decode_vi64(log_data, log_length, pos, reinterpret_cast<int64_t*>(&table_id))))
      {
        TBSYS_LOG(WARN, "failed to deserialize table id, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::decode_vi32(log_data, log_length, pos, &status)))
      {
        TBSYS_LOG(WARN, "failed to deserialize status, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::decode_vi64(log_data, log_length, pos, &end_time)))
      {
        TBSYS_LOG(WARN, "failed to deserialize end_time, ret=%d", ret);
      }
      else
      {
        ret = balancer_->set_load_table_status_from_log(table_id, status, end_time);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "failed to set load table status: table_id=%lu status=%d from replay commit log",
              table_id, status);
        }
        else
        {
          TBSYS_LOG(INFO, "set load table task status: table_id=%lu status=%s endtime=%ld from replay commit log",
              table_id, ObLoadDataInfo::trans_status(status), end_time);
        }
      }
      return ret;
    }

    int ObRootLogWorker::do_create_table_done()
    {
      return OB_SUCCESS;
    }

    int ObRootLogWorker::do_begin_balance()
    {
      return OB_SUCCESS;
    }

    int ObRootLogWorker::do_balance_done()
    {
      return OB_SUCCESS;
    }

    int ObRootLogWorker::do_cs_merge_over(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;

      int64_t pos = 0;
      ObServer server;
      int64_t cs_merge_ts = 0;

      ret = server.deserialize(log_data, log_length, pos);

      if (ret == OB_SUCCESS)
      {
        ret = serialization::decode_vi64(log_data, log_length, pos, &cs_merge_ts);
      }

      if (ret == OB_SUCCESS)
      {
        //ignore return value
        root_server_->waiting_job_done(server, cs_merge_ts);
      }

      return ret;
    }

    int ObRootLogWorker::do_sync_frozen_version(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;

      int64_t pos = 0;
      int64_t frozen_version = 0;

      ret = serialization::decode_vi64(log_data, log_length, pos,&frozen_version);

      if (ret == OB_SUCCESS)
      {
        ret = root_server_->report_frozen_memtable(frozen_version, 0, true);
      }
      return ret;
    }

int ObRootLogWorker::do_init_first_meta_row(const char* log_data, const int64_t& log_length)
{
  int ret = OB_SUCCESS;

  int64_t pos = 0;

  ObTabletMetaTableRow row;
  ObObj obj_array[OB_MAX_ROWKEY_COLUMN_NUMBER * 2];
  row.end_key_.assign(obj_array, OB_MAX_ROWKEY_COLUMN_NUMBER);
  row.start_key_.assign(obj_array + OB_MAX_ROWKEY_COLUMN_NUMBER, OB_MAX_ROWKEY_COLUMN_NUMBER);
  char buff[OB_MAX_TABLE_NAME_LENGTH];
  row.table_name_.assign(buff, OB_MAX_TABLE_NAME_LENGTH);
  ret = row.deserialize(log_data, log_length, pos);

  if (ret == OB_SUCCESS)
  {
    if (root_server_->get_boot_state() == ObBootState::OB_BOOT_OK)
    {
      TBSYS_LOG(WARN, "root server is boot ok alreay, not need to replay init log.");
    }
    else if (OB_SUCCESS != (ret = root_server_->get_first_meta()->init(row)))
    {
      TBSYS_LOG(WARN, "fail to replay init first meta file. err=%d", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "root server boot strap success. set boot ok");
      root_server_->get_boot()->set_boot_ok();
    }
  }
  return ret;

}

    int ObRootLogWorker::do_sync_frozen_version_and_time(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;

      int64_t pos = 0;
      int64_t frozen_version = 0;
      int64_t last_frozen_time = 0;

      ret = serialization::decode_vi64(log_data, log_length, pos,&frozen_version);

      if (ret == OB_SUCCESS)
      {
        serialization::decode_vi64(log_data, log_length, pos,&last_frozen_time);
      }

      if (ret == OB_SUCCESS)
      {
        ret = root_server_->report_frozen_memtable(frozen_version, last_frozen_time, true);
      }
      return ret;
    }

    int ObRootLogWorker::do_set_ups_list(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      ObUpsList ups_list;
      if (OB_SUCCESS != (ret = ups_list.deserialize(log_data, log_length, pos)))
      {
        TBSYS_LOG(ERROR, "deserialize error");
      }
      else
      {
        // do nothing
      }
      return ret;
    }
int ObRootLogWorker::do_clean_root_table(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      int64_t unused_value = 0;
      if (ret == OB_SUCCESS)
      {
        ret = serialization::decode_vi64(log_data, log_length, pos, &unused_value);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to decode unused_value. ret=%d", ret);
        }
      }
      ret = root_server_->slave_clean_root_table();
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to clean root table for slave_rootserver, ret=%d", ret);
      }
      return ret;
    }

    int ObRootLogWorker::do_got_config_version(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;
      int64_t config_version = 0;
      int64_t pos = 0;
      serialization::decode_vi64(log_data, log_length, pos, &config_version);
      ObConfigManager *config_mgr = NULL;

      if (NULL == root_server_)
      {
        TBSYS_LOG(ERROR, "root server is NULL!");
      }
      else if (NULL == (config_mgr = root_server_->get_config_mgr()))
      {
        TBSYS_LOG(ERROR, "Config manager is NULL!");
      }
      else if (NULL == root_server_->worker_)
      {
        TBSYS_LOG(ERROR, "root worker is NULL!");
      }
      else
      {
        ObRoleMgr::Role role = root_server_->worker_->get_role_manager()->get_role();
        if (role == ObRoleMgr::SLAVE)
        {
          config_mgr->got_version(config_version);
        }
      }
      return ret;
    }

    void ObRootLogWorker::exit()
    {
      root_server_->worker_->stop();
    }


uint64_t ObRootLogWorker::get_cur_log_file_id()
{
  int err = OB_SUCCESS;
  uint64_t ret = 0;
  ObLogCursor log_cursor;
  if (NULL != log_manager_)
  {
    if (OB_SUCCESS != (err = log_manager_->get_flushed_cursor(log_cursor)))
    {
      TBSYS_LOG(ERROR, "get_replayed_cursor()=>%d", err);
    }
    else
    {
      ret = log_cursor.file_id_;
    }
  }
  return ret;
}

uint64_t ObRootLogWorker::get_cur_log_seq()
{
  int err = OB_SUCCESS;
  uint64_t ret = 0;
  ObLogCursor log_cursor;
  if (NULL != log_manager_)
  {
    if (OB_SUCCESS != (err = log_manager_->get_flushed_cursor(log_cursor)))
    {
      TBSYS_LOG(ERROR, "get_replayed_cursor()=>%d", err);
    }
    else
    {
      ret = log_cursor.log_id_;
    }
  }
  return ret;
}
int ObRootLogWorker::set_frozen_version_for_brodcast(const int64_t bypass_version)
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      char* log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH, ObModIds::OB_RS_LOG_WORKER));

      if (NULL == log_data)
      {
        TBSYS_LOG(ERROR, "no memory");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos, bypass_version)))
      {
        TBSYS_LOG(ERROR, "serialize error");
      }
      else
      {
        ret = flush_log(OB_RT_SET_BYPASS_VERSION, log_data, pos);
      }
      if (NULL != log_data)
      {
        ob_free(log_data);
        log_data = NULL;
      }
      return ret;
    }
   int ObRootLogWorker::do_set_bypass_version(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      int64_t bypass_version = -1;
      if (OB_SUCCESS != (ret = serialization::decode_vi64(log_data, log_length, pos, &bypass_version)))
      {
        TBSYS_LOG(ERROR, "deserialize bypass version error");
      }
      else
      {
        root_server_->set_bypass_version(bypass_version);
      }
      return ret;
    }

  } /* rootserver */
} /* oceanbase */
