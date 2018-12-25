/*
 *  (C) 2007-2010 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         ????.cpp is for what ...
 *
 *  Version: $Id: ipvsadm.c,v 1.27 2005/12/10 16:00:07 wensong Exp $
 *
 *  Authors:
 *     Author Name <email address>
 *        - some work details if you want
 */


#include "tbnet.h"
#include <pthread.h>
#include "common/ob_define.h"
#include "common/ob_read_common_data.h"
#include "common/ob_scanner.h"
#include "common/ob_packet_factory.h"
#include "common/ob_client_manager.h"
#include "common/ob_packet.h"
#include "common/ob_server.h"
#include "common/serialization.h"
#include "common/data_buffer.h"
#include "common/ob_result.h"
#include "common/ob_malloc.h"
#include "common/utility.h"
#include "common/ob_action_flag.h"
#include "test_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::common::serialization;
class BaseClient
{
public:
  int initialize();
  int destory();
  int wait();
public:
  tbnet::DefaultPacketStreamer streamer_;
  tbnet::Transport transport_;
  ObPacketFactory factory_;
  ObClientManager client_;
};


struct ParamSet
{
  const char * server_ip;
  const char * dest_ip;//for migrate tablet
  int32_t port;
  int32_t dest_port;
  uint64_t table_id;
  uint64_t user_id;
  int8_t item_type;
  uint64_t item_id;
  int64_t column_count;
  int64_t memtable_frozen_version;
  int32_t init_flag;
  int thread_num;
  const char* start_key;
  const char* end_key;
  int key_size;
  int is_hex_key;

  const char* table_name;
  
  ParamSet() : server_ip(),dest_ip(),port(),dest_port(),table_id(1001),user_id(0),item_type(0),item_id(0),column_count(1),
               memtable_frozen_version(1),init_flag(1),thread_num(1)
    {
      // server_ip = "10.232.36.29";
      // dest_ip = "10.232.36.30";
    }
};

int BaseClient::initialize()
{
  streamer_.setPacketFactory(&factory_);
  int ret = client_.initialize(&transport_, &streamer_);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "initialize client manager failed.");
  }
  else
  {
    transport_.start();
  }
  return ret;
}

int BaseClient::destory()
{
  transport_.stop();
  transport_.wait();
  return 0;
}

int BaseClient::wait()
{
  transport_.wait();
  return 0;
}

int rpc_cs_drop(ObClientManager& cp,
                const ObServer& cs,
                int64_t memtable_frozen_version)
{
  int ret = OB_SUCCESS;
  int64_t start = 0, end = 0;
  const int32_t BUFFER_SIZE = 2*1024*1024;
  char* param_buffer = new char[BUFFER_SIZE];
  ObDataBuffer ob_inout_buffer;
  ObResultCode rc;
  int64_t return_start_pos = 0;

  if (NULL == param_buffer)
  {
    goto exit;
  }

  ob_inout_buffer.set_data(param_buffer, BUFFER_SIZE);
  ret = encode_i64(ob_inout_buffer.get_data(),
                   ob_inout_buffer.get_capacity(), ob_inout_buffer.get_position(),memtable_frozen_version);
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr,"serialize memtable_frozen_version into buffer failed\n");
    goto exit;
  }

  // send request;
  start = tbsys::CTimeUtil::getTime();
  ret = cp.send_request(cs, OB_DROP_OLD_TABLETS, 1, 2000*2000, ob_inout_buffer);
  end = tbsys::CTimeUtil::getTime();
  fprintf(stderr,"time consume:%ld\n", end - start);
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr,"rpc failed\n");
    goto exit;
  }
  ret = rc.deserialize(ob_inout_buffer.get_data(),
                       ob_inout_buffer.get_position(), return_start_pos);

  if (OB_SUCCESS != ret)
  {
    fprintf(stderr,"deserialize failed\n");
    goto exit;
  }

  fprintf(stderr,"return rc code:%d, msg:%s\n", rc.result_code_, rc.message_.ptr());

  if (OB_SUCCESS != rc.result_code_)
  {
    goto exit;
  }

  fprintf(stderr,"return_start_pos:%ld, %ld\n", return_start_pos, ob_inout_buffer.get_position());


exit:
  if (param_buffer) delete []param_buffer;
  return ret;
}

int rpc_cs_merge(ObClientManager& cp,
                 const ObServer& cs,
                 int64_t memtable_frozen_version,
                 int32_t init_flag)
{
  int ret = OB_SUCCESS;
  int64_t start = 0, end = 0;
  const int32_t BUFFER_SIZE = 2*1024*1024;
  char* param_buffer = new char[BUFFER_SIZE];
  ObDataBuffer ob_inout_buffer;
  ObResultCode rc;
  int64_t return_start_pos = 0;

  if (NULL == param_buffer)
  {
    goto exit;
  }

  ob_inout_buffer.set_data(param_buffer, BUFFER_SIZE);
  ret = encode_i64(ob_inout_buffer.get_data(),
                   ob_inout_buffer.get_capacity(), ob_inout_buffer.get_position(),memtable_frozen_version);
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr,"serialize memtable_frozen_version into buffer failed\n");
    goto exit;
  }

  ret = encode_i32(ob_inout_buffer.get_data(),
                   ob_inout_buffer.get_capacity(), ob_inout_buffer.get_position(),init_flag);
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr,"serialize init flag into buffer failed\n");
    goto exit;
  }


  // send request;
  start = tbsys::CTimeUtil::getTime();
  ret = cp.send_request(cs, OB_START_MERGE, 1, 2000*2000, ob_inout_buffer);
  end = tbsys::CTimeUtil::getTime();
  fprintf(stderr,"time consume:%ld\n", end - start);
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr,"rpc failed\n");
    goto exit;
  }
  ret = rc.deserialize(ob_inout_buffer.get_data(),
                       ob_inout_buffer.get_position(), return_start_pos);

  if (OB_SUCCESS != ret)
  {
    fprintf(stderr,"deserialize failed\n");
    goto exit;
  }

  fprintf(stderr,"return rc code:%d, msg:%s\n", rc.result_code_, rc.message_.ptr());

  if (OB_SUCCESS != rc.result_code_)
  {
    goto exit;
  }

  fprintf(stderr,"return_start_pos:%ld, %ld\n", return_start_pos, ob_inout_buffer.get_position());


exit:
  if (param_buffer) delete []param_buffer;
  return ret;
}

int rpc_cs_migrate(ObClientManager& cp,
                   const ObServer& src,
                   const ObServer& dest,
                   const ObRange& range,
                   bool keep_src)
{
  int ret = OB_SUCCESS;
  int64_t start = 0, end = 0;
  const int32_t BUFFER_SIZE = 2*1024*1024;
  char* param_buffer = new char[BUFFER_SIZE];
  ObDataBuffer ob_inout_buffer;
  ObResultCode rc;
  int64_t return_start_pos = 0;

  if (NULL == param_buffer)
  {
    goto exit;
  }

  ob_inout_buffer.set_data(param_buffer, BUFFER_SIZE);


  ret = range.serialize(ob_inout_buffer.get_data(),
                        ob_inout_buffer.get_capacity(), ob_inout_buffer.get_position());
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr,"serialize migrate range into buffer failed\n");
    goto exit;
  }

  ret = dest.serialize(ob_inout_buffer.get_data(),
                       ob_inout_buffer.get_capacity(), ob_inout_buffer.get_position());
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr,"serialize dest_server into buffer failed\n");
    goto exit;
  }
  ret = encode_bool(ob_inout_buffer.get_data(),ob_inout_buffer.get_capacity(),ob_inout_buffer.get_position(),keep_src);
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr,"serialize keep_src  into buffer failed\n");
    goto exit;
  }

  // send request;
  start = tbsys::CTimeUtil::getTime();
  ret = cp.send_request(src, OB_CS_MIGRATE, 1, 2000*2000, ob_inout_buffer);
  end = tbsys::CTimeUtil::getTime();
  fprintf(stderr,"time consume:%ld\n", end - start);
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr,"rpc failed\n");
    goto exit;
  }
  ret = rc.deserialize(ob_inout_buffer.get_data(),
                       ob_inout_buffer.get_position(), return_start_pos);

  if (OB_SUCCESS != ret)
  {
    fprintf(stderr,"deserialize failed\n");
    goto exit;
  }

  fprintf(stderr,"return rc code:%d, msg:%s\n", rc.result_code_, rc.message_.ptr());

  if (OB_SUCCESS != rc.result_code_)
  {
    goto exit;
  }

  fprintf(stderr,"return_start_pos:%ld, %ld\n", return_start_pos, ob_inout_buffer.get_position());


exit:
  if (param_buffer) delete []param_buffer;
  return ret;
}

/*int rpc_cs_create(ObClientManager& cp,
  const ObServer& cs,
  const ObRange& range)
  {
  int ret = OB_SUCCESS;
  int64_t start = 0, end = 0;
  const int32_t BUFFER_SIZE = 2*1024*1024;
  char* param_buffer = new char[BUFFER_SIZE];
  ObDataBuffer ob_inout_buffer;
  ObResultCode rc;
  int64_t return_start_pos = 0;

  if (NULL == param_buffer)
  {
  goto exit;
  }

  ob_inout_buffer.set_data(param_buffer, BUFFER_SIZE);
  ret = range.serialize(ob_inout_buffer.get_data(),
  ob_inout_buffer.get_capacity(), ob_inout_buffer.get_position());
  if (OB_SUCCESS != ret)
  {
  fprintf(stderr,"serialize scan_param into buffer failed\n");
  goto exit;
  }

  // send request;
  start = tbsys::CTimeUtil::getTime();
  ret = cp.send_request(cs, OB_CREATE_TABLET_REQUEST, 1, 2000*2000, ob_inout_buffer);
  end = tbsys::CTimeUtil::getTime();
  fprintf(stderr,"time consume:%ld\n", end - start);
  if (OB_SUCCESS != ret)
  {
  fprintf(stderr,"rpc failed\n");
  goto exit;
  }
  ret = rc.deserialize(ob_inout_buffer.get_data(),
  ob_inout_buffer.get_position(), return_start_pos);

  if (OB_SUCCESS != ret)
  {
  fprintf(stderr,"deserialize  failed\n");
  goto exit;
  }

  fprintf(stderr,"return rc code:%d, msg:%s\n", rc.result_code_, rc.message_.ptr());

  if (OB_SUCCESS != rc.result_code_)
  {
  goto exit;
  }

  fprintf(stderr,"return_start_pos:%ld, %ld\n", return_start_pos, ob_inout_buffer.get_position());


  exit:
  if (param_buffer) delete []param_buffer;
  return ret;
  }*/


int rpc_cs_scan(ObClientManager& cp,
                const ObServer& cs,
                const ObScanParam& scan_param,
                ObScanner& scanner)
{
  int ret = OB_SUCCESS;
  int64_t start = 0, end = 0;
  const int32_t BUFFER_SIZE = 2*1024*1024;
  char* param_buffer = new char[BUFFER_SIZE];
  ObDataBuffer ob_inout_buffer;
  ObResultCode rc;
  int64_t return_start_pos = 0;

  if (NULL == param_buffer)
  {
    goto exit;
  }

  ob_inout_buffer.set_data(param_buffer, BUFFER_SIZE);
  ret = scan_param.serialize(ob_inout_buffer.get_data(),
                             ob_inout_buffer.get_capacity(), ob_inout_buffer.get_position());
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr,"serialize scan_param into buffer failed\n");
    goto exit;
  }

  // send request;
  start = tbsys::CTimeUtil::getTime();
  ret = cp.send_request(cs, OB_SCAN_REQUEST, 1, 2000*2000, ob_inout_buffer);
  end = tbsys::CTimeUtil::getTime();
  fprintf(stderr,"time consume:%ld\n", end - start);
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr,"rpc failed\n");
    goto exit;
  }


  ret = rc.deserialize(ob_inout_buffer.get_data(),
                       ob_inout_buffer.get_position(), return_start_pos);

  if (OB_SUCCESS != ret)
  {
    fprintf(stderr,"deserialize obscanner failed\n");
    goto exit;
  }

  fprintf(stderr,"return rc code:%d, msg:%s\n", rc.result_code_, rc.message_.ptr());

  if (OB_SUCCESS != rc.result_code_)
  {
    goto exit;
  }

  fprintf(stderr,"return_start_pos:%ld, %ld\n", return_start_pos, ob_inout_buffer.get_position());

  // deserialize output
  ret = scanner.deserialize(ob_inout_buffer.get_data(),
                            ob_inout_buffer.get_position(), return_start_pos);
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr,"deserialize obscanner failed\n");
    goto exit;
  }
  fprintf(stderr,"return_start_pos:%ld, %ld\n", return_start_pos, ob_inout_buffer.get_position());

exit:
  if (param_buffer) delete []param_buffer;
  return ret;
}

int rpc_cs_get(ObClientManager& cp,
               const ObServer& cs,
               const ObGetParam& get_param,
               ObScanner& scanner)
{
  int ret = OB_SUCCESS;
  int64_t start = 0, end = 0;
  const int32_t BUFFER_SIZE = 2*1024*1024;
  char* param_buffer = new char[BUFFER_SIZE];
  ObDataBuffer ob_inout_buffer;
  ObResultCode rc;
  int64_t return_start_pos = 0;

  if (NULL == param_buffer)
  {
    goto exit;
  }

  ob_inout_buffer.set_data(param_buffer, BUFFER_SIZE);
  ret = get_param.serialize(ob_inout_buffer.get_data(),
                            ob_inout_buffer.get_capacity(), ob_inout_buffer.get_position());
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr,"serialize get_param into buffer failed\n");
    goto exit;
  }

  // send request;
  start = tbsys::CTimeUtil::getTime();
  ret = cp.send_request(cs, OB_GET_REQUEST, 1, 2000*2000, ob_inout_buffer);
  end = tbsys::CTimeUtil::getTime();
  fprintf(stderr,"time consume:%ld\n", end - start);
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr,"rpc failed\n");
    goto exit;
  }

  ret = rc.deserialize(ob_inout_buffer.get_data(),
                       ob_inout_buffer.get_position(), return_start_pos);

  if (OB_SUCCESS != ret)
  {
    fprintf(stderr,"deserialize obscanner failed\n");
    goto exit;
  }

  fprintf(stderr,"return rc code:%d, msg:%s\n", rc.result_code_, rc.message_.ptr());

  if (OB_SUCCESS != rc.result_code_)
  {
    goto exit;
  }

  fprintf(stderr,"return_start_pos:%ld, %ld\n", return_start_pos, ob_inout_buffer.get_position());

  // deserialize output
  ret = scanner.deserialize(ob_inout_buffer.get_data(),
                            ob_inout_buffer.get_position(), return_start_pos);
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr,"deserialize obscanner failed\n");
    goto exit;
  }
  fprintf(stderr,"return_start_pos:%ld, %ld\n", return_start_pos, ob_inout_buffer.get_position());

exit:
  if (param_buffer) delete []param_buffer;
  return ret;
}

int drop_test_case(BaseClient& client, const ObServer&server,
                   const int64_t memtable_frozen_version)
{
  int64_t start = tbsys::CTimeUtil::getTime();
  int ret = rpc_cs_drop(client.client_, server,memtable_frozen_version);
  int64_t end = tbsys::CTimeUtil::getTime();
  fprintf(stderr,"rpc_cs_merge time consume:%ld\n", end - start);
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr,"ret:%d\n", ret);
  }
  return ret;
}

int merge_test_case(BaseClient& client,const ObServer&server,
                    const int64_t memtable_frozen_version, const int32_t init_flag)
{
  int64_t start = tbsys::CTimeUtil::getTime();
  int ret = rpc_cs_merge(client.client_, server,memtable_frozen_version,init_flag);
  int64_t end = tbsys::CTimeUtil::getTime();
  fprintf(stderr,"rpc_cs_merge time consume:%ld\n", end - start);
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr,"ret:%d\n", ret);
  }
  return ret;
}

int migrate_test_case(
  BaseClient& client,
  const ObServer& src,
  const ObServer& dest,
  const uint64_t table_id,
  const int64_t user_id,
  const int8_t item_type)
{
  const int32_t key_size = 17;
  char start_key[key_size];
  char end_key[key_size];

  int64_t pos = 0;
  encode_i64(start_key,key_size,pos,user_id);
  if ( item_type <= 1)
    encode_i8(start_key,key_size,pos,item_type);
  else
    encode_i8(start_key,key_size,pos,0x0);
  memset(start_key+pos,0x0,key_size-pos);

  pos = 0;
  encode_i64(end_key,key_size,pos,user_id);
  if( item_type <= 1)
    encode_i8(end_key,key_size,pos,item_type);
  else
    encode_i8(end_key,key_size,pos,static_cast<int8_t>(0xFF));
  memset(end_key+pos,0xFF,key_size-pos);

  hex_dump(start_key,key_size);
  hex_dump(end_key,key_size);

  ObRange range;
  range.table_id_= table_id;
  range.start_key_.assign_ptr(start_key, key_size);
  range.end_key_.assign_ptr(end_key, key_size);

  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();

  bool keep_src = true;

  int64_t start = tbsys::CTimeUtil::getTime();
  int ret = rpc_cs_migrate(client.client_, src, dest,range,keep_src);
  int64_t end = tbsys::CTimeUtil::getTime();
  fprintf(stderr,"rpc_cs_migrate time consume:%ld\n", end - start);
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr,"ret:%d\n", ret);
  }
  return ret;
}


/*int create_test_case(
  BaseClient& client,
  const ObServer& server,
  const uint64_t table_id,
  const int64_t user_id,
  const int8_t item_type)
  {
  const int32_t key_size = 17;
  char start_key[key_size] ;
  char end_key[key_size] ;

  int64_t pos = 0;
  encode_i64(start_key, key_size, pos, user_id);
  if (item_type <= 1)
  encode_i8(start_key, key_size, pos, item_type);
  else
  encode_i8(start_key, key_size, pos, 0x0);
  memset(start_key + pos, 0x0, key_size - pos);

  pos = 0;
  encode_i64(end_key, key_size, pos, user_id);
  if ( item_type <= 1)
  encode_i8(end_key, key_size, pos, item_type);
  else
  encode_i8(end_key, key_size, pos, 0xFF);

  memset(end_key + pos, 0xFF, key_size - pos);

  hex_dump(start_key, key_size);
  hex_dump(end_key, key_size);

  ObRange range;
  range.table_id_= table_id;
  range.start_key_.assign_ptr(start_key, key_size);
  range.end_key_.assign_ptr(end_key, key_size);

  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();


  int64_t start = tbsys::CTimeUtil::getTime();
  int ret = rpc_cs_create(client.client_, server, range);
  int64_t end = tbsys::CTimeUtil::getTime();
  fprintf(stderr,"rpc_cs_create time consume:%ld\n", end - start);
  if (OB_SUCCESS != ret)
  {
  fprintf(stderr,"ret:%d\n", ret);
  }
  return ret;
  }
*/

void dump_scanner(ObScanner &scanner)
{
  ObScannerIterator iter;
  int total_count = 0;
  int row_count = 0;
  bool is_row_changed = false;
  for (iter = scanner.begin(); iter != scanner.end(); iter++)
  {
    ObCellInfo *cell_info;
    iter.get_cell(&cell_info, &is_row_changed);
    if (is_row_changed) 
    {
      ++row_count;
      fprintf(stderr,"table_id:%lu, rowkey:\n", cell_info->table_id_);
      hex_dump(cell_info->row_key_.ptr(), cell_info->row_key_.length());
    }
    fprintf(stderr, "%s\n", common::print_cellinfo(cell_info, "CLI_GET"));
    ++total_count;
  }
  fprintf(stderr, "row_count=%d,total_count=%d\n", row_count, total_count);
}

int scan_test_case(
    BaseClient& client,
    const ObServer& server,
    const uint64_t table_id,
    const int64_t user_id,
  const int8_t item_type,
  const int32_t column_size
  )
{
  ObScanParam input;


  const int32_t key_size = 17;
  char start_key[key_size] ;
  char end_key[key_size] ;

  int64_t pos = 0;
  
  encode_i64(start_key, key_size, pos, user_id);
  if (item_type <= 1)
    encode_i8(start_key, key_size, pos, item_type);
  else
    encode_i8(start_key, key_size, pos, 0x0);
  memset(start_key + pos, 0x0, key_size - pos);

  pos = 0;
  encode_i64(end_key, key_size, pos, user_id);
  if ( item_type <= 1)
    encode_i8(end_key, key_size, pos, item_type);
  else
    encode_i8(end_key, key_size, pos, static_cast<int8_t>(0xFF));

  memset(end_key + pos, 0xFF, key_size - pos);

  hex_dump(start_key, key_size);
  hex_dump(end_key, key_size);


  ObString ob_table_name;
  ObRange range;
  range.table_id_= table_id;
  range.start_key_.assign_ptr(start_key, key_size);
  range.end_key_.assign_ptr(end_key, key_size);
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();

  range.hex_dump();

  input.set(table_id, ob_table_name, range);
  input.set_scan_size(100000);

  for (int i = 0; i < column_size; ++i)
  {
    input.add_column(i + 2);
  }

  ObScanner scanner;
  int64_t start = tbsys::CTimeUtil::getTime();
  int ret = rpc_cs_scan(client.client_, server, input, scanner);
  int64_t end = tbsys::CTimeUtil::getTime();
  fprintf(stderr,"rpc_cs_scan time consume:%ld\n", end - start);
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr,"ret:%d\n", ret);
  }
  else
  {
    dump_scanner(scanner);
  }
  
  return ret;
}

int merge_scan_case(
  BaseClient& client,
  const ObServer& server,
  const char* table_name_ptr,
  const char* start_key_ptr,
  const char* end_key_ptr,
  const int64_t input_key_size,
  const bool is_hex
  )
{
  ObScanParam input;


  int64_t key_size = input_key_size;
  if (is_hex)
  {
    key_size = input_key_size / 2;
  }

  char start_key[input_key_size];
  char end_key[input_key_size];
  char range_str[OB_RANGE_STR_BUFSIZ];


  memset(start_key, 0, input_key_size);
  memset(end_key, 0, input_key_size);
  if (is_hex)
  {
    str_to_hex(start_key_ptr, static_cast<int32_t>(input_key_size), start_key, static_cast<int32_t>(key_size));
    str_to_hex(end_key_ptr, static_cast<int32_t>(input_key_size), end_key, static_cast<int32_t>(key_size));
  }
  else
  {
    memcpy(start_key, start_key_ptr, key_size);
    memcpy(end_key, end_key_ptr, key_size);
  }


  ObString ob_table_name(0, static_cast<int32_t>(strlen(table_name_ptr)), (char*)table_name_ptr);

  ObRange range;
  range.table_id_= 0;
  range.start_key_.assign_ptr(start_key, static_cast<int32_t>(key_size));
  range.end_key_.assign_ptr(end_key, static_cast<int32_t>(key_size));
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();

  range.to_string(range_str, OB_RANGE_STR_BUFSIZ);
  range.hex_dump();
  fprintf(stderr, "input scan range = %s\n", range_str);

  input.set(OB_INVALID_ID, ob_table_name, range);
  input.set_scan_size(100);
  //input.add_column(0); // whole row?

  ObScanner scanner;
  int64_t start = tbsys::CTimeUtil::getTime();
  int ret = rpc_cs_scan(client.client_, server, input, scanner);
  int64_t end = tbsys::CTimeUtil::getTime();
  fprintf(stderr,"rpc_cs_scan time consume:%ld\n", end - start);
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr,"ret:%d\n", ret);
  }
  else
  {
    dump_scanner(scanner);
  }
  return ret;
}

int scan_test_case_common(
  BaseClient& client,
  const ObServer& server,
  const int64_t  table_id,
  const char* start_key_ptr,
  const char* end_key_ptr,
  const int64_t input_key_size,
  const bool is_hex
  )
{
  ObScanParam input;


  int64_t key_size = input_key_size;
  if (is_hex)
  {
    key_size = input_key_size / 2;
  }

  char start_key[input_key_size];
  char end_key[input_key_size];
  char range_str[OB_RANGE_STR_BUFSIZ];


  memset(start_key, 0, input_key_size);
  memset(end_key, 0, input_key_size);
  if (is_hex)
  {
    str_to_hex(start_key_ptr, static_cast<int32_t>(input_key_size), start_key, static_cast<int32_t>(key_size));
    str_to_hex(end_key_ptr, static_cast<int32_t>(input_key_size), end_key, static_cast<int32_t>(key_size));
  }
  else
  {
    memcpy(start_key, start_key_ptr, key_size);
    memcpy(end_key, end_key_ptr, key_size);
  }


  ObString ob_table_name(0, 0, NULL);

  ObRange range;
  range.table_id_= 0;
  range.start_key_.assign_ptr(start_key, static_cast<int32_t>(key_size));
  range.end_key_.assign_ptr(end_key, static_cast<int32_t>(key_size));
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();

  range.to_string(range_str, OB_RANGE_STR_BUFSIZ);
  range.hex_dump();
  fprintf(stderr, "input scan range = %s\n", range_str);


  input.set(table_id, ob_table_name, range);
  input.set_scan_size(100000);
  input.add_column((uint64_t)0ull);


  ObScanner scanner;
  int64_t start = tbsys::CTimeUtil::getTime();
  int ret = rpc_cs_scan(client.client_, server, input, scanner);
  int64_t end = tbsys::CTimeUtil::getTime();
  fprintf(stderr,"rpc_cs_scan time consume:%ld\n", end - start);
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr,"ret:%d\n", ret);
  }
  else
  {
    dump_scanner(scanner);
  }
  
  return ret;
}

int get_test_case(
  BaseClient& client,
  const ObServer& server,
  const uint64_t table_id,
  const int64_t user_id,
  const int8_t item_type,
  const uint64_t item_id
  )
{
  ObGetParam input;
  const int32_t key_size = 17;
  char start_key[key_size] ;
  int64_t pos = 0;

  encode_i64(start_key, key_size, pos, user_id);
  if (item_type <= 1)
    encode_i8(start_key, key_size, pos, item_type);
  else
    encode_i8(start_key, key_size, pos, 0x0);
  encode_i64(start_key, key_size, pos, item_id);
  hex_dump(start_key, key_size);

  ObCellInfo cell;
  cell.table_id_ = table_id;
  cell.row_key_.assign(start_key, key_size);
  cell.column_id_ = 0;  //get full row
//  cell.value_.set_extend_field(ObActionFlag::OP_READ);
  input.add_cell(cell);

  ObScanner scanner;
  int64_t start = tbsys::CTimeUtil::getTime();
  int ret = rpc_cs_get(client.client_, server, input, scanner);
  int64_t end = tbsys::CTimeUtil::getTime();
  fprintf(stderr,"rpc_cs_get time consume:%ld\n", end - start);
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr,"ret:%d\n", ret);
  }
  else
  {
    dump_scanner(scanner);
  }
  return ret;
}

int get_test_case_common(
  BaseClient& client,
  const ObServer& server,
  const uint64_t table_id,
  const char* start_key,
  const int64_t column_count
  )
{
  ObGetParam input;
  int32_t key_size = static_cast<int32_t>(strlen(start_key));
  UNUSED(column_count);


  ObCellInfo cell;
  cell.table_id_ = table_id;
  cell.row_key_.assign((char*)start_key, key_size);
  cell.column_id_ = 0;  //get full row
  input.add_cell(cell);

  ObScanner scanner;
  int64_t start = tbsys::CTimeUtil::getTime();
  int ret = rpc_cs_get(client.client_, server, input, scanner);
  int64_t end = tbsys::CTimeUtil::getTime();
  fprintf(stderr,"rpc_cs_get time consume:%ld\n", end - start);
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr,"ret:%d\n", ret);
  }
  else
  {
    dump_scanner(scanner);
  }
  return ret;
}

int get_test_case_common_hex(
  BaseClient& client,
  const ObServer& server,
  const uint64_t table_id,
  const int64_t key_value,
  const int64_t key_size 
  )
{
  ObGetParam input;

  char start_key[key_size] ;
  memset(start_key, 0, key_size);
  int64_t pos = 0;

  encode_i64(start_key, key_size, pos, key_value);
  hex_dump(start_key, static_cast<int32_t>(key_size));


  ObCellInfo cell;
  cell.table_id_ = table_id;
  cell.row_key_.assign((char*)start_key, static_cast<int32_t>(key_size));
  cell.column_id_ = 0;  //get full row
  input.add_cell(cell);

  ObScanner scanner;
  int64_t start = tbsys::CTimeUtil::getTime();
  int ret = rpc_cs_get(client.client_, server, input, scanner);
  int64_t end = tbsys::CTimeUtil::getTime();
  fprintf(stderr,"rpc_cs_get time consume:%ld\n", end - start);
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr,"ret:%d\n", ret);
  }
  else
  {
    dump_scanner(scanner);
  }
  return ret;
}


void* thread_run(void* arg)
{
  ParamSet *param = reinterpret_cast<ParamSet*>(arg);
  ObServer cs;
  cs.set_ipv4_addr(param->server_ip, param->port);
  
  BaseClient client;
  if (OB_SUCCESS != client.initialize())
  {
    TBSYS_LOG(ERROR, "initialize client failed.");
    //return OB_ERROR;
  }
  // get_test_case(client, cs, param->table_id, param->user_id, param->item_type, param->item_id);
  //scan_test_case(client, cs, param->table_id, param->user_id, param->item_type, param->column_count);
  // create_test_case(client, cs, param->table_id,param->user_id,param->item_type);
  // migrate_test_case();
  // merge_test_case();
  return (void *)0;
}
void printParam(ParamSet& param){
  printf("param is\n");
  printf("\tserver_ip \t\t%s\n",param.server_ip);
   printf("\tdest_ip \t\t%s\n",param.dest_ip);
  printf("\tport \t\t\t%d\n",param.port);
  printf("\tdest_port \t\t%d\n",param.dest_port);
  printf("\tteble_id \t\t%ld\n",param.table_id);
  printf("\tuser_id \t\t%ld\n",param.user_id);
  printf("\titem_type \t\t%d\n",param.item_type);
  printf("\titem_id \t\t%ld\n",param.item_id);
  printf("\tcolumn_count \t\t%ld\n",param.column_count);
  printf("\tmtable_frozen_V \t%ld\n",param.memtable_frozen_version);
  printf("\tinit_flag \t\t%d\n",param.init_flag);
  printf("\tstart_key \t\t%s\n",param.start_key);
  printf("\tend_key \t\t%s\n",param.end_key);

  //int64_t memtable_frozen_version;
  //int32_t init_flag;
  //int thread_num;
 
}

void printhelp()
{
  printf( "Usage:\n "
            "-s server_ip -p port -T table_id [-u user_id -t item_type] [-S start_key -E end_key]\n "
            "[-c column_count]  [-i item_id] [-d dest_ip] [-P dest_port]\n" 
            " [-m memtable_frozen_version]  [-f init_flag]\n");
        
}

void printusage()
{
  printf("====================TEST MANUAL====================\n");
  printf("command: \tget\t\trun get_test\n"
         " \t\tscan\t\trun scan_test\n"
         " \t\tmigrate\t\trun migrate_test\n"
         " \t\tmerge\t\trun merge_test\n"
         " \t\tdrop\t\trun drop_test\n"
         " \t\thelp\t\tprint this help\n"
         " \t\tquit\t\tquit\n");
}

bool parse_param(ParamSet& param, char *cmd)
{
  bool print_help = false;  
  const char * delim = " ";
  char *p;
  printf("%s ",strtok(cmd,delim));
  while((p=strtok(NULL,delim)))
  {
    if (strstr(p,"h"))
    {
      print_help = true;
      break;;
    }
    else  if (strstr(p,"S"))
    {
      param.start_key = strtok(NULL,delim);
      continue;
    }
    else  if (strstr(p,"E"))
    {
      param.end_key = strtok(NULL,delim);
      continue;
    }
    else  if (strstr(p,"s"))
    {
      param.server_ip = strtok(NULL,delim);
      continue;
    }
    else if (strstr(p,"p"))
    {
      param.port = atoi(strtok(NULL,delim));
      continue;
    }
    else if (strstr(p,"P"))
    {
      param.dest_port = atoi(strtok(NULL,delim));
      continue;
    }
    else if (strstr(p,"T"))
    {
      param.table_id = strtoll(strtok(NULL,delim),NULL,10);
      continue;
    }
    else if (strstr(p,"u"))
    {
      param.user_id = strtoll(strtok(NULL,delim),NULL,10);
      continue;
    }
    else if (strstr(p,"t"))
    {
      param.item_type = static_cast<int8_t>(strtoll(strtok(NULL,delim),NULL,10));
      continue;
    }
    else if (strstr(p,"i"))
    {
      param.item_id = strtoll(strtok(NULL,delim),NULL,10);
      continue;
    }
    else if (strstr(p,"c"))
    {
      param.column_count = atoi(strtok(NULL,delim));
      continue;
    }
    else if (strstr(p,"m"))
    {
      param.memtable_frozen_version = atoi(strtok(NULL,delim));
      continue;
    }
    else if (strstr(p,"f"))
    {
      param.init_flag = atoi(strtok(NULL,delim));
      continue;
    }
    else if (strstr(p,"d"))
    {
      param.dest_ip = strtok(NULL,delim);
      continue;
    }
    else if (strstr(p,"X"))
    {
      param.is_hex_key = atoi(strtok(NULL, delim));
      continue;
    }
    else if (strstr(p,"N"))
    {
      param.table_name = strtok(NULL,delim) ;
      continue;
    }
    else if (strstr(p,"K"))
    {
      param.key_size = atoi(strtok(NULL,delim));
      continue;
    }
  }
  // printParam(param);
  return print_help;
}

void run_merge_scan_case(ParamSet& param, char * cmd)
{
  bool print_help = parse_param(param,cmd);
  if ( true == print_help )
    printhelp();
  else
  {
    printParam(param);
    ObServer cs;
    cs.set_ipv4_addr(param.server_ip, param.port);
    BaseClient client;
    if (OB_SUCCESS != client.initialize())
    {
      TBSYS_LOG(ERROR, "initialize client failed.");
    }
    merge_scan_case(client, cs, param.table_name, param.start_key, param.end_key, param.key_size, param.is_hex_key);
    client.destory();
    printf("===============GET TEST DONE==============\n");
  }
}

void run_get_test_case(ParamSet& param, char * cmd)
{
  bool print_help = parse_param(param,cmd);
  if ( true == print_help )
    printhelp();
  else
  {
    printParam(param);
    ObServer cs;
    cs.set_ipv4_addr(param.server_ip, param.port);
    BaseClient client;
    if (OB_SUCCESS != client.initialize())
    {
      TBSYS_LOG(ERROR, "initialize client failed.");
    }
    get_test_case(client, cs, param.table_id, param.user_id, param.item_type, param.item_id);
    client.destory();
    printf("===============GET TEST DONE==============\n");
  }
}

void run_get_test_case_common(ParamSet& param, char * cmd)
{
  bool print_help = parse_param(param,cmd);
  if ( true == print_help )
    printhelp();
  else
  {
    printParam(param);
    ObServer cs;
    cs.set_ipv4_addr(param.server_ip, param.port);
    BaseClient client;
    if (OB_SUCCESS != client.initialize())
    {
      TBSYS_LOG(ERROR, "initialize client failed.");
    }
    get_test_case_common(client, cs, param.table_id, param.start_key, param.column_count);
    client.destory();
    printf("===============GET TEST DONE==============\n");
  }
}

void run_get_test_case_common_hex(ParamSet& param, char * cmd)
{
  bool print_help = parse_param(param,cmd);
  if ( true == print_help )
    printhelp();
  else
  {
    printParam(param);
    ObServer cs;
    cs.set_ipv4_addr(param.server_ip, param.port);
    BaseClient client;
    if (OB_SUCCESS != client.initialize())
    {
      TBSYS_LOG(ERROR, "initialize client failed.");
    }
    get_test_case_common_hex(client, cs, param.table_id, param.user_id, param.item_id);
    client.destory();
    printf("===============GET TEST DONE==============\n");
  }
}

void run_scan_test_case_common(ParamSet& param, char * cmd)
{
  bool print_help = parse_param(param,cmd);
  if ( true == print_help )
    printhelp();
  else
  {
    printParam(param);
    ObServer cs;
    cs.set_ipv4_addr(param.server_ip, param.port);
    BaseClient client;
    if (OB_SUCCESS != client.initialize())
    {
      TBSYS_LOG(ERROR, "initialize client failed.");
    }
    //scan_test_case_common(client, cs, param.table_id, param.start_key, param.end_key, param.column_count);
    scan_test_case_common(client, cs, param.table_id, param.start_key, param.end_key, param.key_size, param.is_hex_key);
    client.destory();
    printf("================SCAN TEST DONE===============\n");
  }
}

void run_scan_test_case(ParamSet& param, char * cmd)
{
  bool print_help = parse_param(param,cmd);
  if ( true == print_help )
    printhelp();
  else
  {
    printParam(param);
    ObServer cs;
    cs.set_ipv4_addr(param.server_ip, param.port);
    BaseClient client;
    if (OB_SUCCESS != client.initialize())
    {
      TBSYS_LOG(ERROR, "initialize client failed.");
    }
    scan_test_case(client, cs, param.table_id, param.user_id, static_cast<int8_t>(param.item_type), static_cast<int32_t>(param.column_count));
    client.destory();
    printf("================SCAN TEST DONE===============\n");
  }
}

void run_migrate_test_case(ParamSet& param, char * cmd)
{
  bool print_help = parse_param(param,cmd);
  if ( true == print_help )
    printhelp();
  else
  {
    printParam(param);
    ObServer cs;
    cs.set_ipv4_addr(param.server_ip, param.port);
    ObServer ds;
    ds.set_ipv4_addr(param.dest_ip, param.dest_port);
    BaseClient client;
    if (OB_SUCCESS != client.initialize())
    {
      TBSYS_LOG(ERROR, "initialize client failed.");
    }
    migrate_test_case(client, cs, ds, param.table_id, param.user_id, param.item_type);
    client.destory();
    printf("================MIGRATE TEST DONE================\n");
  }
}
void run_merge_test_case(ParamSet& param, char * cmd)
{
  bool print_help = parse_param(param,cmd);
  if ( true == print_help )
    printhelp();
  else
  {
    printParam(param);
    ObServer cs;
    cs.set_ipv4_addr(param.server_ip, param.port);
    BaseClient client;
    if (OB_SUCCESS != client.initialize())
    {
      TBSYS_LOG(ERROR, "initialize client failed.");
    }
    merge_test_case(client, cs, param.memtable_frozen_version, param.init_flag);
    client.destory();
    printf("==============MERGE TEST DONE================\n");
  }
}  

void run_drop_test_case(ParamSet& param, char * cmd)
{
  bool print_help = parse_param(param,cmd);
  if ( true == print_help )
    printhelp();
  else
  {
    printParam(param);
    ObServer cs;
    cs.set_ipv4_addr(param.server_ip, param.port);
    BaseClient client;
    if (OB_SUCCESS != client.initialize())
    {
      TBSYS_LOG(ERROR, "initialize client failed.");
    }
    drop_test_case(client, cs, param.memtable_frozen_version);
    client.destory();
    printf("==============DROP TEST DONE==================\n");
  }
}

char* parse_cmd(const char* cmdlist, char* cmd)
{
  const char *p = cmdlist;
  while (p && *p++ != ' ');
  int len = static_cast<int32_t>(p - cmdlist - 1);
  strncpy(cmd, cmdlist, len);
  cmd[len] = 0;
  return cmd;
}

int main(int argc, char* argv[])
{
  UNUSED(argc);
  UNUSED(argv);
//  const static int MAX_THREAD_NUM = 10000;
  // pthread_t pid[MAX_THREAD_NUM];
//  int i = 0;
  ParamSet param;
  printusage();
  ob_init_memory_pool();
  char cmdlist[1024];
  char cmd[128];
  while(printf("client_test>")&&fgets(cmdlist,1024,stdin))
  {

    parse_cmd(cmdlist, cmd);
    printf("cmdlist=(%s), cmd=(%s)\n", cmdlist, cmd);
    
    if (strcmp(cmd,"get") == 0)
      run_get_test_case(param,cmdlist);
    else if (strcmp(cmd,"get_common") == 0)
      run_get_test_case_common(param,cmdlist);
    else if (strcmp(cmd,"get_common_hex") == 0)
      run_get_test_case_common_hex(param,cmdlist);
    else if (strcmp(cmd,"scan") == 0)
      run_scan_test_case(param,cmdlist);
    else if (strcmp(cmd,"scan_common") == 0)
      run_scan_test_case_common(param,cmdlist);
    else if (strcmp(cmd,"merge_scan") == 0)
      run_merge_scan_case(param,cmdlist);
    else if (strcmp(cmd,"migrate") == 0)
      run_migrate_test_case(param,cmdlist);
    else if (strcmp(cmd,"merge") == 0)
      run_merge_test_case(param,cmdlist);
    else if(strcmp(cmd,"drop") == 0)
      run_drop_test_case(param,cmdlist);
    else if(strcmp(cmd,"help") == 0)
      printusage();
    else if(strcmp(cmd,"quit") == 0)
      break;
  }

  /* ObServer cs;
     cs.set_ipv4_addr(param.server_ip, param.port);
     ObServer ds;
     ds.set_ipv4_addr(param.dest_ip, param.port);
     BaseClient client;
     if (OB_SUCCESS != client.initialize())
     {
     TBSYS_LOG(ERROR, "initialize client failed.");
     return OB_ERROR;
     }*/

/*  for ( int idx =0 ;idx<param.thread_num;idx++)
    {
    pthread_create(&pid[idx], NULL,thread_run,(void*)&param);
    }
  
    for ( int idx =0 ;idx<param.thread_num;idx++)
    {
    pthread_join(pid[idx], NULL);
    }*/
  // get_test_case(client, cs, param.table_id, param.user_id, param.item_type, param.item_id);
  // scan_test_case(client, cs, param.table_id, param.user_id, param.item_type, param.column_count);
  // create_test_case(client, cs, param.table_id,param.user_id,param.item_type);
  // migrate_test_case(client, cs, cs, param.table_id, param.user_id, param.item_type);
  // merge_test_case(client, cs, param.memtable_frozen_version, param.init_flag);
  // drop_test_case(client, cs, param.memtable_frozen_version);
//  client.destory();
}
