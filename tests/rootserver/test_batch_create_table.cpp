/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 * First Create_time: 2011-8-5
 *
 * Authors:
 *   rongxuan <rongxuan.lc@taobao.com>
 *     - some work details if you want
 */

#include "common/ob_tablet_info.h"
#include "common/serialization.h"
#include <gtest/gtest.h>
#include "../common/test_rowkey_helper.h"
using namespace oceanbase::common;
static CharArena allocator_;
void build_range(ObNewRange & r, int64_t tid, int8_t flag, const char* sk, const char* ek)
{
  ObString start_key(static_cast<int32_t>(strlen(sk)), static_cast<int32_t>(strlen(sk)), (char*)sk);
  ObString end_key(static_cast<int32_t>(strlen(ek)), static_cast<int32_t>(strlen(ek)), (char*)ek);
  r.table_id_ = tid;
  r.border_flag_.set_data(flag);
  r.start_key_ = make_rowkey(sk, &allocator_);
  r.end_key_ = make_rowkey(ek, &allocator_);
}

TEST(TestLog, log)
{
  ObTabletInfoList tablets;
  int ret = OB_SUCCESS;
  int64_t table_id = 1001;
  ObNewRange range1;
  range1.start_key_.set_min_row();
  range1.border_flag_.set_inclusive_end();
  build_range(range1, table_id, range1.border_flag_.get_data(), "0000", "2222");
  ObTabletInfo *tablet1 = new ObTabletInfo (range1, 0, 0, 0);

  ObNewRange range2;
  range2.border_flag_.unset_inclusive_start();
  range2.border_flag_.set_inclusive_end();
  build_range(range2, table_id, range2.border_flag_.get_data(), "2222", "5555");
  ObTabletInfo *tablet2 = new ObTabletInfo (range2, 0, 0, 0);

  ObNewRange range3;
  range3.border_flag_.unset_inclusive_start();
  range3.border_flag_.set_inclusive_end();
  build_range(range3, table_id, range3.border_flag_.get_data(), "5555", "8888");
  ObTabletInfo *tablet3 = new ObTabletInfo (range3, 0, 0, 0);

  ObNewRange range4;
  range4.border_flag_.unset_inclusive_start();
  range4.end_key_.set_max_row();
  build_range(range4, table_id, range4.border_flag_.get_data(), "8888", "0000");
  ObTabletInfo *tablet4 = new ObTabletInfo (range4, 0, 0, 0);

  tablets.add_tablet(*tablet1);
  tablets.add_tablet(*tablet2);
  tablets.add_tablet(*tablet3);
  tablets.add_tablet(*tablet4);

  int32_t server_index[4][3];
  int32_t count[4];
  int64_t version = 4;
  for (int32_t i = 0; i < 4; i ++)
  {
    for (int32_t j = 0 ; j < 3; j++)
    {
      server_index[i][j] = j;
    }
    count[i] = 2 ;
  }

  char* log_data = NULL;
  log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH, ObModIds::TEST));
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
          ret = serialization::encode_vi32(log_data, OB_MAX_PACKET_LENGTH, pos, server_index[i][j]);
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
    ret = serialization::encode_vi32(log_data, OB_MAX_PACKET_LENGTH, pos, static_cast<int32_t>(version));
  }
  /////反序列化
  int64_t pos2 = 0;
  int64_t mem_version = 0;

  int64_t index2 = 0;
  ObTabletInfoList tablets2;
  int32_t **server_index2 = NULL;
  int64_t log_length = OB_MAX_PACKET_LENGTH;
  int32_t * create_count = NULL;
  ret = serialization::decode_vi64(log_data, log_length, pos2, &index2);
  EXPECT_EQ(index2, index);
  if (OB_SUCCESS == ret)
  {
    create_count = new (std::nothrow)int32_t[index2];
    server_index2 = new (std::nothrow)int32_t*[index2];
    if (NULL == server_index2 || NULL == create_count)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else
    {
      for (int32_t i = 0; i < index2; i++)
      {
        server_index2[i] = new(std::nothrow) int32_t[OB_SAFE_COPY_COUNT];
        if (NULL == server_index2[i])
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          break;
        }
      }
    }
  }
  ObTabletInfo *p_table_info2 = new ObTabletInfo[index2];
  for (int64_t i = 0; OB_SUCCESS == ret && i < index2; i++)
  {
    ret = serialization::decode_vi32(log_data, log_length, pos2, &create_count[i]);
    if (ret == OB_SUCCESS)
    {
      ret = p_table_info2[i].deserialize(log_data, log_length, pos2);
    }
    static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
    p_table_info2[i].range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
    TBSYS_LOG(INFO, "tablet_index=%ld, range=%s", i, row_key_dump_buff);

    if (ret == OB_SUCCESS && create_count[i] > 0 )
    {
      for (int j = 0; j < create_count[i] && j < OB_SAFE_COPY_COUNT; j++)
      {
        ret = serialization::decode_vi32(log_data, log_length, pos2, server_index2[i] + j);
        if (ret != OB_SUCCESS)
        {
          break;
        }
        EXPECT_EQ(server_index2[i][j] , j);
      }
    }
    if (ret == OB_SUCCESS)
    {
      tablets.add_tablet(p_table_info2[i]);
    }
  }
  if (ret == OB_SUCCESS)
  {
    ret = serialization::decode_vi64(log_data, log_length, pos2, &mem_version);
    EXPECT_EQ(mem_version, version);
  }
}
int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
