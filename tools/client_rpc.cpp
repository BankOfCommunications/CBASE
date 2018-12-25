#include <list>
#include "client_rpc.h"
#include "common/ob_result.h"
#include "common/utility.h"
#include "common/ob_row.h"
#include "sql/ob_sql_result_set.h"
#include "mergeserver/ob_ms_sql_proxy.h"

using namespace oceanbase::common;
using namespace oceanbase::rootserver;
using oceanbase::sql::ObSQLResultSet;

template <typename T>
int get_result(ObDataBuffer & data_buff, ObResultCode& result_code, T &data)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_SUCCESS == ret)
  {
    ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d]", pos, ret);
    }
    else
    {
      ret = result_code.result_code_;
    }
  }

  // step 3. deserialize update server addr
  if (OB_SUCCESS == ret)
  {
    ret = data.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "deserialize server list failed:pos[%ld], ret[%d]", pos, ret);
    }
  }
  return ret;
}

template <typename T>
int get_result(ObDataBuffer & data_buff, T &data)
{
  ObResultCode result_code;
  return get_result(data_buff, result_code, data);
}

ObClientRpcStub::ObClientRpcStub()
  : frame_buffer_(FRAME_BUFFER_SIZE)
{
  init_ = false;
  rpc_frame_ = NULL;
}

ObClientRpcStub::~ObClientRpcStub()
{

}


int ObClientRpcStub::request_report_tablet()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == ret)
  {
    ret = send_0_return_0(remote_server_, DEFAULT_TIMEOUT, OB_RS_REQUEST_REPORT_TABLET, DEFAULT_VERSION);
  }
  return ret;
}

int ObClientRpcStub::initialize(const ObServer & remote_server,
    const ObClientManager * rpc_frame, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = init(&frame_buffer_, rpc_frame)))
  {
    TBSYS_LOG(ERROR, "already inited or check input failed:inited[%s], rpc_frame[%p]",
       (init_? "ture": "false"), rpc_frame);
  }
  else
  {
    remote_server_ = remote_server;
    timeout_ = timeout;
  }
  return ret;
}


int ObClientRpcStub::cs_scan(const ObScanParam& scan_param, ObScanner& scanner)
{
  return send_1_return_1(remote_server_, DEFAULT_TIMEOUT, OB_SCAN_REQUEST, DEFAULT_VERSION, scan_param, scanner);
}


int ObClientRpcStub::cs_get(const ObGetParam& get_param, ObScanner& scanner)
{
  return send_1_return_1(remote_server_, DEFAULT_TIMEOUT, OB_GET_REQUEST, DEFAULT_VERSION, get_param, scanner);
}

int ObClientRpcStub::check_backup_privilege(
    const oceanbase::common::ObString username, const oceanbase::common::ObString passwd, bool & result)
{
  return send_2_return_1(remote_server_, DEFAULT_TIMEOUT, OB_CHECK_BACKUP_PRIVILEGE,
      DEFAULT_VERSION,username,passwd, result);
}

int ObClientRpcStub::cs_dump_tablet_image(
    const int32_t index, const int32_t disk_no,
    oceanbase::common::ObString &image_buf)
{
  return send_2_return_1(remote_server_, DEFAULT_TIMEOUT, OB_CS_DUMP_TABLET_IMAGE,
      DEFAULT_VERSION, index, disk_no, image_buf);
}

int ObClientRpcStub::rs_dump_cs_info(ObChunkServerManager &obcsm)
{
  return send_0_return_1(remote_server_, DEFAULT_TIMEOUT, OB_DUMP_CS_INFO, DEFAULT_VERSION, obcsm);
}

int ObClientRpcStub::cs_install_disk(const ObString &mount_path, int32_t& disk_no)
{
  return send_1_return_1(remote_server_, 20 * DEFAULT_TIMEOUT, OB_CS_INSTALL_DISK, DEFAULT_VERSION, mount_path, disk_no);
}

int ObClientRpcStub::cs_uninstall_disk(const int32_t disk_no)
{
  return send_1_return_0(remote_server_, 20 * DEFAULT_TIMEOUT, OB_CS_UNINSTALL_DISK, DEFAULT_VERSION, disk_no);
}

int ObClientRpcStub::fetch_stats(oceanbase::common::ObStatManager &obsm)
{
  return send_0_return_1(remote_server_, DEFAULT_TIMEOUT, OB_FETCH_STATS, DEFAULT_VERSION, obsm);
}

int ObClientRpcStub::get_update_server(ObServer &update_server)
{
  return send_0_return_1(remote_server_, DEFAULT_TIMEOUT, OB_GET_UPDATE_SERVER_INFO, DEFAULT_VERSION, update_server);
}

int ObClientRpcStub::start_merge(const int64_t frozen_memtable_version, const int32_t init_flag)
{
  return send_2_return_0(remote_server_, DEFAULT_TIMEOUT, OB_START_MERGE, DEFAULT_VERSION,
      frozen_memtable_version, init_flag);
}

int ObClientRpcStub::drop_tablets(const int64_t frozen_memtable_version)
{
  return send_1_return_0(remote_server_, DEFAULT_TIMEOUT, OB_START_MERGE, DEFAULT_VERSION,
      frozen_memtable_version);
}

int ObClientRpcStub::start_gc(const int32_t reserve)
{
  return send_1_return_0(remote_server_, DEFAULT_TIMEOUT, OB_CS_START_GC, DEFAULT_VERSION, reserve);
}

int ObClientRpcStub::rs_scan(const ObServer & server, const int64_t timeout,
    const ObScanParam & param, ObScanner & result)
{
  return send_1_return_1(server, timeout, OB_SCAN_REQUEST, DEFAULT_VERSION, param, result);
}

int ObClientRpcStub::get_tablet_info(const uint64_t table_id, const char* table_name,
    const ObNewRange& range, ObTabletLocation location [],int32_t& size)
{
  int ret = OB_SUCCESS;
  int32_t index = 0;
  const int64_t timeout = timeout_ > 0 ? timeout_ : 10000000;  //10s
  if (OB_INVALID_ID == table_id || size <= 0)
  {
    TBSYS_LOG(ERROR,"invalid table id");
    ret = OB_ERROR;
  }

  ObScanParam param;
  ObScanner scanner;
  ObString table_name_str;

  table_name_str.assign_ptr(const_cast<char*>(table_name), static_cast<int32_t>(strlen(table_name)));


  if (OB_SUCCESS == ret)
  {
    param.set(OB_INVALID_ID,table_name_str,range); //use table name
  }

  if ((OB_SUCCESS == ret) && ((ret = rs_scan(remote_server_,timeout,param,scanner)) != OB_SUCCESS) )
  {
    TBSYS_LOG(ERROR,"get tablet from rootserver failed:[%d]",ret);
  }

  ObServer server;
  char tmp_buf[32];
  ObRowkey start_key;
  ObRowkey end_key;
  ObCellInfo * cell = NULL;
  ObScannerIterator iter;
  bool row_change = false;

  if (OB_SUCCESS == ret)
  {
    int64_t ip = 0;
    int64_t port = 0;
    int64_t version = 0;
    iter = scanner.begin();
    for (; OB_SUCCESS == ret && iter != scanner.end() && index < size; ++iter)
    {
      ret = iter.get_cell(&cell, &row_change);

      fprintf(stderr, "row_change=%d, column_name=%.*s\n", row_change,
          cell->column_name_.length(), cell->column_name_.ptr());
      fprintf(stderr, "%s", to_cstring(cell->row_key_));
      cell->value_.dump();

      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "get cell from scanner iterator failed:ret[%d]", ret);
        break;
      }

      if (row_change)
      {
        if (OB_SUCCESS == ret)
        {
          TBSYS_LOG(DEBUG,"row changed");
          if (0 == port || 0 == ip  || 0 == version)
          {
            TBSYS_LOG(DEBUG, "%s,version:%ld", "check failed",version);
          }
          else
          {
            server.set_ipv4_addr(static_cast<int32_t>(ip), static_cast<int32_t>(port));
            server.to_string(tmp_buf,sizeof(tmp_buf));
            TBSYS_LOG(INFO,"add tablet s:%s,%ld",tmp_buf,version);
            ObTabletLocation addr(version, server);
            location[index++] = addr;
          }
          ip = port = version = 0;
        }
      }

      if (OB_SUCCESS == ret && cell != NULL)
      {
        end_key == cell->row_key_;
        if ((cell->column_name_.compare("1_port") == 0)
            || (cell->column_name_.compare("2_port") == 0)
            || (cell->column_name_.compare("3_port") == 0))
        {
          ret = cell->value_.get_int(port);
          TBSYS_LOG(DEBUG,"port is %ld",port);
        }
        else if ((cell->column_name_.compare("1_ipv4") == 0)
            || (cell->column_name_.compare("2_ipv4") == 0)
            || (cell->column_name_.compare("3_ipv4") == 0))
        {
          ret = cell->value_.get_int(ip);
          TBSYS_LOG(DEBUG,"ip is %ld",ip);
        }
        else if (cell->column_name_.compare("1_tablet_version") == 0 ||
                 cell->column_name_.compare("2_tablet_version") == 0 ||
                 cell->column_name_.compare("3_tablet_version") == 0)
        {
          ret = cell->value_.get_int(version);
          TBSYS_LOG(WARN, "%s", to_cstring(cell->row_key_));
          TBSYS_LOG(DEBUG,"tablet_version is %ld",version);
        }

        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "check get value failed:ret[%d]", ret);
          break;
        }
      }
    }

    if (OB_SUCCESS == ret && ip != 0 && port != 0 && version != 0 && index < size) //last row
    {
      server.set_ipv4_addr(static_cast<int32_t>(ip), static_cast<int32_t>(port));
      ObTabletLocation addr(version, server);
      server.to_string(tmp_buf,sizeof(tmp_buf));
      TBSYS_LOG(DEBUG,"add tablet s:%s,%ld",tmp_buf,version);
      location[index++] = addr;
    }
  }

  if (OB_SUCCESS == ret)
  {
    size = index;
    TBSYS_LOG(DEBUG,"get %d tablets from rootserver",size);
  }
  return ret;
}

int ObClientRpcStub::migrate_tablet(const ObDataSourceDesc &desc)
{
  static const int NEW_VERSION = 3;
  return send_1_return_0(remote_server_, DEFAULT_TIMEOUT, OB_CS_MIGRATE, NEW_VERSION, desc);
}

int ObClientRpcStub::create_tablet(const ObNewRange& range, const int64_t last_frozen_version)
{
  return send_2_return_0(remote_server_, DEFAULT_TIMEOUT, OB_CS_CREATE_TABLE, DEFAULT_VERSION, range, last_frozen_version);
}

int ObClientRpcStub::delete_tablet(const ObTabletReportInfoList& info_list,
  bool is_force/*= false*/)
{
  return send_2_return_0(remote_server_, DEFAULT_TIMEOUT, OB_CS_DELETE_TABLETS, DEFAULT_VERSION, info_list, is_force);
}

int ObClientRpcStub::fetch_update_server_list(ObUpsList & server_list)
{
  return send_0_return_1(remote_server_, DEFAULT_TIMEOUT, OB_GET_UPS, DEFAULT_VERSION, server_list);
}

int ObClientRpcStub::fetch_schema(const int64_t version,
    const bool only_core_tables, oceanbase::common::ObSchemaManagerV2& schema)
{
  return send_2_return_1(remote_server_, DEFAULT_TIMEOUT, OB_FETCH_SCHEMA, DEFAULT_VERSION, version, only_core_tables, schema);
}

int ObClientRpcStub::show_param()
{
  int ret = OB_SUCCESS;
  ret = send_0_return_0(remote_server_, DEFAULT_TIMEOUT, OB_CS_SHOW_PARAM, DEFAULT_VERSION);
  // step 4. deserialize the table schema
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(INFO,"send show param success");
  }

  return ret;
}

int ObClientRpcStub::sync_all_tablet_images()
{
  int ret = OB_SUCCESS;
  ret = send_0_return_0(remote_server_, DEFAULT_TIMEOUT, OB_CS_SYNC_ALL_IMAGES, DEFAULT_VERSION);
  // step 4. deserialize the table schema
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(INFO,"send sync_all_tablet_images success");
  }

  return ret;
}

int ObClientRpcStub::delete_table(const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  ret = send_1_return_0(remote_server_, DEFAULT_TIMEOUT, OB_CS_DELETE_TABLE, DEFAULT_VERSION, static_cast<int64_t>(table_id));
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(INFO,"send delete table success");
  }

  return ret;
}

int ObClientRpcStub::load_sstables(const ObTableImportInfoList& table_list)
{
  int ret = OB_SUCCESS;
  ret = send_1_return_0(remote_server_, DEFAULT_TIMEOUT, OB_CS_LOAD_BYPASS_SSTABLE, DEFAULT_VERSION, table_list);
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(INFO,"send load bypass sstables success");
  }

  return ret;
}

int ObClientRpcStub::cs_show_disk(int32_t* disk_no, bool* avail, int32_t &disk_num)
{
  int ret = OB_SUCCESS;
  const int32_t CS_SHOW_DISK_VERSION = 1;
  ObDataBuffer data_buffer;
  const int64_t timeout = timeout_ > 0 ? timeout_ : 10000000;  //10s

  if (OB_SUCCESS != (ret = get_frame_buffer(data_buffer)))
  {
    TBSYS_LOG(ERROR, "failed to alloc in_buffer, ret=%d", ret);
  }
  else
  {
    common::ObResultCode rc;
    int64_t pos = 0;

    if (OB_SUCCESS != (ret =
          rpc_frame_->send_request(remote_server_, OB_CS_SHOW_DISK,
            CS_SHOW_DISK_VERSION, timeout, data_buffer)))
    {
      TBSYS_LOG(WARN, "failed to send request to server: %s, timeout=%ld ret=%d",
          to_cstring(remote_server_), timeout, ret);
    }
    else if (OB_SUCCESS != (ret = rc.deserialize(data_buffer.get_data(), data_buffer.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "failed to deserialize rc, ret:%d", ret);
    }
    else if (OB_SUCCESS != rc.result_code_)
    {
      ret = rc.result_code_;
      TBSYS_LOG(ERROR, "failed show disk, ret:%d", ret);
    }
    else
    {
      if (OB_SUCCESS != (ret = serialization::decode_i32(data_buffer.get_data(), data_buffer.get_position(), pos, &disk_num)))
      {
        TBSYS_LOG(ERROR, "failed to deserialize disk num, ret:%d", ret);
      }
      else
      {
        for (int32_t i = 0; i < disk_num && i < common::OB_MAX_DISK_NUMBER; i++)
        {
          if (OB_SUCCESS == ret)
          {
            ret = serialization::decode_i32(data_buffer.get_data(), data_buffer.get_position(), pos, &disk_no[i]);
          }

          if (OB_SUCCESS == ret)
          {
            ret = serialization::decode_bool(data_buffer.get_data(), data_buffer.get_position(), pos, &avail[i]);
          }
        }
      }
    }
  }
  return ret;
}

int ObClientRpcStub::stop_server(bool restart)
{
  int ret = OB_SUCCESS;
  const int64_t timeout = timeout_ > 0 ? timeout_ : 10000000;  //10s
  const int buff_size = sizeof(ObPacket) + 32;
  char buff[buff_size];
  ObDataBuffer msgbuf(buff, buff_size);
  int8_t need_restart = restart ? 1 : 0;

  // change_log_level消息的内容为一个int32_t类型log_level
  if (OB_SUCCESS != (ret = serialization::encode_i32(
                       msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), need_restart)))
  {
    printf("failed to serialize, err=%d\n", ret);
  }
  else if (OB_SUCCESS != (ret = rpc_frame_->send_request(remote_server_, OB_STOP_SERVER, DEFAULT_VERSION, timeout, msgbuf)))
  {
    printf("failed to send request, err=%d\n", ret);
  }
  else
  {
    ObResultCode result_code;
    msgbuf.get_position() = 0;
    if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
    {
      printf("failed to deserialize response, err=%d\n", ret);
    }
    else if (OB_SUCCESS != (ret = result_code.result_code_))
    {
      printf("failed to stop server, err=%d\n", result_code.result_code_);
    }
    else
    {
      printf("Okay\n");
    }
  }
  return ret;
}

int ObClientRpcStub::change_log_level(int32_t log_level)
{
  return send_1_return_0(remote_server_, DEFAULT_TIMEOUT, OB_CHANGE_LOG_LEVEL, DEFAULT_VERSION, log_level);
}

int ObClientRpcStub::execute_sql(const ObString &query)
{
  int ret = OB_SUCCESS;
  bool fullfilled = 0;
  ObSQLResultSet rs;
  ret = send_1_return_1(remote_server_, DEFAULT_TIMEOUT, OB_SQL_EXECUTE, DEFAULT_VERSION, query, rs);

  printf("execute_sql, query=[%.*s]...\n", query.length(), query.ptr());

  if (OB_SUCCESS == ret)
  {
    do
    {
      {
        for (int64_t i = 0; i < rs.get_fields().count(); i++)
        {
          printf("| %.*s\t", rs.get_fields().at(i).cname_.length(),
                 rs.get_fields().at(i).cname_.ptr());
        }
        puts("|");
        puts("-----------------------------------");
        ObRow row;
        uint64_t tid = 0;
        uint64_t cid = 0;
        const ObObj *cell = NULL;
        while (OB_SUCCESS == (ret = rs.get_new_scanner().get_next_row(row)))
        {
          for (int64_t cell_idx = 0;
               cell_idx < row.get_column_num();
               cell_idx++)
          {
            if (OB_SUCCESS != row.raw_get_cell(cell_idx, cell, tid, cid))
            {
              printf("fail to get cell");
            }
            else
            {
              printf("| ");
              const_cast<ObObj *>(cell)->print_value(stdout);
              printf("\t");
            }
          }
          puts("|");
        }
        if (OB_ITER_END != ret)
        {
          printf("fail to get_next_row. unpexect ret val. ret = [%d]\n", ret);
        }
        else
        {
          ret = OB_SUCCESS;
        }
        rs.get_fullfilled(fullfilled);
        if (!fullfilled && !rs.get_new_scanner().is_empty())
        {
          /*
          data_buff.get_position() = 0;
          if (OB_SUCCESS != (ret = rpc_frame_->get_next(
                               remote_server_, session_id, timeout,
                               msgbuf, msgbuf)))
          {
            printf("failted to send get_next, ret=[%d]\n", ret);
            break;
          }
          */
        }
        else
        {
          break;
        }
      }
    }
    while (true);
  }

  return ret;
}

int ObClientRpcStub::get_frame_buffer(oceanbase::common::ObDataBuffer & data_buffer) const
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    TBSYS_LOG(ERROR, "check inner stat failed.");
    ret = OB_ERROR;
  }

  ThreadSpecificBuffer::Buffer* rpc_buffer = frame_buffer_.get_buffer();
  if (OB_SUCCESS == ret)
  {
    if (NULL == rpc_buffer)
    {
      TBSYS_LOG(ERROR, "get thread rpc buff failed:buffer[%p].", rpc_buffer);
      ret = OB_ERROR;
    }
    else
    {
      rpc_buffer->reset();
      data_buffer.set_data(rpc_buffer->current(), rpc_buffer->remain());
    }
  }
  return ret;
}

int ObClientRpcStub::fetch_cs_sstable_dist(const ObServer& server, const int64_t table_version,
    std::list<std::pair<ObNewRange, ObString> >& sstable_list, oceanbase::common::ModuleArena& arena)
{
  const int64_t timeout = timeout_ > 0 ? timeout_ : 10000000;  //10s
  int ret = OB_SUCCESS;
  ObDataBuffer in_buffer;
  ObDataBuffer out_buffer;
  int64_t total_sstable_count = 0;
  int64_t session_id = 0;
  bool last_flag = false;
  const int32_t OB_CS_FETCH_SSTABLE_DIST_VERSION = 1;
  oceanbase::common::ObResultCode rc;
  ObNewRange range;
  ObString path;
  ObObj start_rowkey_objs[OB_MAX_ROWKEY_COLUMN_NUMBER];
  ObObj end_rowkey_objs[OB_MAX_ROWKEY_COLUMN_NUMBER];

  sstable_list.clear();

  TBSYS_LOG(INFO, "fetching sstable disst from %s with table_version_=%ld",
      to_cstring(server), table_version);

  if (OB_SUCCESS != (ret = get_frame_buffer(in_buffer)))
  {
    TBSYS_LOG(ERROR, "failed to alloc in_buffer, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = get_frame_buffer(out_buffer)))
  {
    TBSYS_LOG(ERROR, "failed to alloc out_buffer, ret=%d", ret);
  }
  else
  {
    in_buffer.get_position() = 0;
    out_buffer.get_position() = 0;

    if (OB_SUCCESS != (ret = serialization::encode_vi64(in_buffer.get_data(),
            in_buffer.get_capacity(), in_buffer.get_position(), table_version)))
    {
      TBSYS_LOG(WARN, "failed to encode table_version_, cap=%ld pos=%ld ret=%d",
          in_buffer.get_capacity(), in_buffer.get_position(), ret);
    }
    else if (OB_SUCCESS != (ret =
          rpc_frame_->send_request(server, OB_CS_FETCH_SSTABLE_DIST,
            OB_CS_FETCH_SSTABLE_DIST_VERSION, timeout, in_buffer, out_buffer, session_id)))
    {
      TBSYS_LOG(WARN, "failed to send request to server: %s, timeout=%ld session_id=%ld ret=%d",
          to_cstring(server), timeout, session_id, ret);
    }

    while (OB_SUCCESS == ret && !last_flag)
    {
      int64_t sstable_count = 0;

      int64_t pos = 0;
      ret = rc.deserialize(out_buffer.get_data(), out_buffer.get_position(), pos);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to deserialize rc, cap=%ld pos=%ld ret=%d",
            out_buffer.get_position(), pos, ret);
      }
      else if (OB_ITER_END == rc.result_code_)
      {
        last_flag = true;
        ret = OB_SUCCESS;
      }
      else if (OB_SUCCESS != rc.result_code_)
      {
        ret = rc.result_code_;
        TBSYS_LOG(WARN, "error happend in cs server[%s], ret=%d", to_cstring(server), ret);
        break;
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(out_buffer.get_data(), out_buffer.get_position(),
            pos, &sstable_count);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to decode sstable_count, cap=%ld pos=%ld ret=%d",
              out_buffer.get_position(), pos, ret);
        }
      }
      for(int64_t i = 0; OB_SUCCESS == ret && i < sstable_count; ++i)
      {
        range.start_key_.assign(start_rowkey_objs, OB_MAX_ROWKEY_COLUMN_NUMBER);
        range.end_key_.assign(end_rowkey_objs, OB_MAX_ROWKEY_COLUMN_NUMBER);
        if (OB_SUCCESS !=
            (ret = range.deserialize(out_buffer.get_data(), out_buffer.get_position(), pos)))
        {
          TBSYS_LOG(WARN, "failed to decode range, cap=%ld pos=%ld ret=%d",
              out_buffer.get_position(), pos, ret);
        }
        else if (OB_SUCCESS !=
            (ret = path.deserialize(out_buffer.get_data(), out_buffer.get_position(), pos)))
        {
          TBSYS_LOG(WARN, "failed to decode path, cap=%ld pos=%ld ret=%d",
              out_buffer.get_position(), pos, ret);
        }
        else
        {
          ObString path_copy;
          ObNewRange range_copy;
          const int64_t path_len = path.length();
          char* path_buf = arena.alloc(path_len);
          if (NULL == path_buf)
          {
            TBSYS_LOG(WARN, "failed to alloc path buf, len %ld", path_len);
            ret = OB_ALLOCATE_MEMORY_FAILED;
          }
          else
          {
            ::memcpy(path_buf, path.ptr(), path_len);
            path_copy.assign_ptr(path_buf, static_cast<int32_t>(path_len));
            if (OB_SUCCESS != (ret = deep_copy_range(arena, range, range_copy)))
            {
              TBSYS_LOG(WARN, "failed to copy range: %s, ret=%d", to_cstring(range), ret);
            }
            sstable_list.push_back(std::pair<ObNewRange, ObString>(range_copy, path_copy));
          }
        }
      }

      total_sstable_count += sstable_count;
      TBSYS_LOG(INFO, "fetch sstable_count %ld, total_sstable_count from this cs: %ld",
         sstable_count, total_sstable_count);
      if (last_flag)
      {
        break;
      }
      int rpc_ret = OB_SUCCESS;
      if (OB_SUCCESS == ret)
      {
        in_buffer.get_position() = 0;
        out_buffer.get_position() = 0;
        if (OB_SUCCESS != (ret =
              rpc_frame_->get_next(server, session_id, timeout, in_buffer, out_buffer)))
        {
          TBSYS_LOG(WARN, "failed to get next from server: %s, ret=%d", to_cstring(server), ret);
        }
      }
      else if (OB_SUCCESS != (rpc_ret =
            rpc_frame_->post_end_next(server, session_id, timeout, in_buffer, NULL, NULL)))
      {
        TBSYS_LOG(WARN, "failed to post end to server %s, ret=%d", to_cstring(server), rpc_ret);
      }
    }
  }

  return ret;
}

int ObClientRpcStub::get_bloom_filter(const oceanbase::common::ObNewRange& range, 
    const int64_t tablet_version, const int64_t bf_version, ObString& bf_buffer)
{
  return ObGeneralRpcStub::get_bloom_filter(DEFAULT_TIMEOUT, remote_server_, 
      range, tablet_version, bf_version, bf_buffer);
}

int ObClientRpcStub::fetch_bypass_table_id(ObBypassTaskInfo& info)
{
  int ret = OB_SUCCESS;
  const int64_t timeout = timeout_ > 0 ? timeout_ : 10000000;  //10s
  ObDataBuffer data_buff;
  ret = get_frame_buffer(data_buff);

  ret = info.serialize(data_buff.get_data(),data_buff.get_capacity(),
    data_buff.get_position());
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "serialize import table task info into buffer failed,"
        "cap=%ld pos=%ld ret=%d", data_buff.get_capacity(), data_buff.get_position(), ret);
  }

  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(remote_server_,
        OB_RS_PREPARE_BYPASS_PROCESS, DEFAULT_VERSION, timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "post request to root server for load_bypass_sstables failed"
          " ret[%d].",  ret);
    }
  }

  int64_t pos = 0;
  if (OB_SUCCESS == ret)
  {
    ObResultCode result_code;
    if (OB_SUCCESS != (ret =
          result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "deserialize result_code failed:cap[%ld] pos[%ld], ret[%d].",
          data_buff.get_position(), pos, ret);
    }
    else if (OB_SUCCESS != result_code.result_code_)
    {
      ret = result_code.result_code_;
      TBSYS_LOG(ERROR, "root server return error code: %d", ret);
    }
    else if (OB_SUCCESS != (ret =
          info.deserialize(data_buff.get_data(), data_buff.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "deserialize bybpass table info failed: cap[%ld] pos[%ld] ret[%d]",
          data_buff.get_position(), pos, ret);
    }
  }

  return ret;
}

int ObClientRpcStub::backup_database(const int32_t backup_mode_id)
{
  int ret = OB_SUCCESS;
  const int64_t timeout = timeout_ > 0 ? timeout_ : 10000000;  //10s
  ObDataBuffer data_buff;
  ret = get_frame_buffer(data_buff);
  ret = serialization::encode_vi32(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position(), backup_mode_id);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "serialize backup mode info into buffer failed,"
        "cap=%ld pos=%ld ret=%d", data_buff.get_capacity(), data_buff.get_position(), ret);
  }

  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(remote_server_,
        OB_BACKUP_DATABASE,DEFAULT_VERSION , timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "post request to server for backup database failed"
          " ret[%d].",  ret);
    }
  }

  int64_t pos = 0;
  if (OB_SUCCESS == ret)
  {
    ObResultCode result_code;
    if (OB_SUCCESS != (ret =
          result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "deserialize result_code failed:cap[%ld] pos[%ld], ret[%d].",
          data_buff.get_position(), pos, ret);
    }
    else if (OB_SUCCESS != result_code.result_code_)
    {
      ret = result_code.result_code_;
      TBSYS_LOG(ERROR, "backup server return error code: %d", ret);
    }
  }
  return ret;
}

int ObClientRpcStub::backup_table(const int32_t backup_mode_id, const oceanbase::common::ObString db_name,
                                  const oceanbase::common::ObString table_name)
{
  int ret = OB_SUCCESS;
  const int64_t timeout = timeout_ > 0 ? timeout_ : 10000000;  //10s
  ObDataBuffer data_buff;
  ret = get_frame_buffer(data_buff);
  if (OB_SUCCESS != (ret = serialization::encode_vi32
                     (data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position(), backup_mode_id)))
  {
    TBSYS_LOG(ERROR, "serialize backup_mod_id into buffer failed,"
        "cap=%ld pos=%ld ret=%d", data_buff.get_capacity(), data_buff.get_position(), ret);
  }
  if (OB_SUCCESS != (ret = serialization::encode_vstr
                     (data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position(), db_name.ptr())))
  {
    TBSYS_LOG(ERROR, "serialize dbname info into buffer failed,"
        "cap=%ld pos=%ld ret=%d", data_buff.get_capacity(), data_buff.get_position(), ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vstr
                          (data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position(), table_name.ptr())))
  {
    TBSYS_LOG(ERROR, "serialize tablename info into buffer failed,"
        "cap=%ld pos=%ld ret=%d", data_buff.get_capacity(), data_buff.get_position(), ret);
  }

  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(remote_server_,
        OB_BACKUP_TABLE, DEFAULT_VERSION, timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "post request to backup server for backup table failed"
          " ret[%d].",  ret);
    }
  }
  int64_t pos = 0;
  if (OB_SUCCESS == ret)
  {
    ObResultCode result_code;
    if (OB_SUCCESS != (ret =
          result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "deserialize result_code failed:cap[%ld] pos[%ld], ret[%d].",
          data_buff.get_position(), pos, ret);
    }
    else if (OB_SUCCESS != result_code.result_code_)
    {
      ret = result_code.result_code_;
      TBSYS_LOG(ERROR, "backup server return error code: %d", ret);
    }
  }
  return ret;
}

int ObClientRpcStub::backup_abort()
{
  int ret = OB_SUCCESS;
  const int64_t timeout = timeout_ > 0 ? timeout_ : 10000000;  //10s
  ObDataBuffer data_buff;
  ret = get_frame_buffer(data_buff);

  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(remote_server_,
        OB_BACKUP_ABORT, DEFAULT_VERSION, timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "post request to root server for backup abort failed"
          " ret[%d].",  ret);
    }
  }
  int64_t pos = 0;
  if (OB_SUCCESS == ret)
  {
    ObResultCode result_code;
    if (OB_SUCCESS != (ret =
          result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "deserialize result_code failed:cap[%ld] pos[%ld], ret[%d].",
          data_buff.get_position(), pos, ret);
    }
    else if (OB_SUCCESS != result_code.result_code_)
    {
      ret = result_code.result_code_;
      TBSYS_LOG(ERROR, "backup server return error code: %d", ret);
    }
  }
  return ret;
}

int ObClientRpcStub::check_backup_process()
{
  int ret = OB_SUCCESS;
  const int64_t timeout = timeout_ > 0 ? timeout_ : 10000000;  //10s
  ObDataBuffer data_buff;
  ret = get_frame_buffer(data_buff);

  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(remote_server_,
        OB_CHECK_BACKUP_PROCESS, DEFAULT_VERSION, timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "post request to root server for backup abort failed"
          " ret[%d].",  ret);
    }
  }
  int64_t pos = 0;
  if (OB_SUCCESS == ret)
  {
    ObResultCode result_code;
    if (OB_SUCCESS != (ret =
          result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "deserialize result_code failed:cap[%ld] pos[%ld], ret[%d].",
          data_buff.get_position(), pos, ret);
    }
    else if (OB_SUCCESS != result_code.result_code_)
    {
      ret = result_code.result_code_;
      TBSYS_LOG(ERROR, "backup server return error code: %d", ret);
    }
  }
  return ret;
}
