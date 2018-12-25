/*
* (C) 2007-2011 TaoBao Inc.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
* test_compactsstable_mem.cpp is for what ...
*
* Version: $id$
*
* Authors:
*   MaoQi maoqi@taobao.com
*
*/
#include <stdlib.h>
#include <gtest/gtest.h>
#include "common/ob_define.h"
#include "common/ob_schema.h"
#include "common/ob_malloc.h"
#include "compactsstable/ob_compactsstable_mem.h"
#include "sstable/ob_sstable_schema.h"
#include "common/file_directory_utils.h"
#include "common/ob_object.h"
#include "sstable/ob_disk_path.h"

using namespace oceanbase::common;
using namespace oceanbase::sstable;
using namespace oceanbase::compactsstable;
namespace oceanbase
{
  namespace compactsstable
  {
    class TestCompactSSTableMem: public ::testing::Test
    {
    public:
      virtual void SetUp()
        {
          int ret = OB_SUCCESS;

          ObFrozenVersionRange version_range;
          version_range.major_version_ = 10;
          version_range.minor_version_start_ = 1;
          version_range.minor_version_end_ = 10;
          version_range.is_final_minor_version_ = 1;
          writer = new ObCompactSSTableMem();
          if ((ret = writer->init(version_range,0)) != OB_SUCCESS)
          {
            fprintf(stderr,"create sstable failed");
          }
        }

      int32_t get_int_size(const int64_t v)
      {
        /*
         * from yubai.lk 
         * 注意 为了减少计算代价 与常规的int类型表示范围不同
         * int8_t 表示范围为 [-127, 127] 而非 [-128, 127]
         * int16_t 表示范围为 [-32767, 32767] 而非 [-32768, 127]
         * int32_t 表示范围为 [-2147483647, 2147483647] 而非 [-2147483648, 2147483647]
         */

        static const int64_t INT8_MASK  = 0xffffffffffffff80;
        static const int64_t INT16_MASK = 0xffffffffffff8000;
        static const int64_t INT32_MASK = 0xffffffff80000000;

        int64_t abs_v = v;
        
        if (0 > abs_v)
        {
          abs_v = llabs(v);
        }
        int32_t ret = 8;
        if (!(INT8_MASK & abs_v))
        {
          ret = 1;
        }
        else if (!(INT16_MASK & abs_v))
        {
          ret = 2;
        }
        else if (!(INT32_MASK & abs_v))
        {
          ret = 4;
        }
        else
        {
          //int64
        }
        return ret;
      }      

      int32_t generate_obj(ObObj& obj,bool delete_row = false)
      {
        int32_t size = 0;
        if (delete_row)
        {
          obj.set_ext(ObActionFlag::OP_DEL_ROW);
          size = 1;
        }
        else
        {
          ObObjType type_array[] =
            {
              ObIntType,
              ObPreciseDateTimeType,
              ObVarcharType,
              ObCreateTimeType,
              ObModifyTimeType,
            };
          int32_t array_size = static_cast<int32_t>(sizeof(type_array) / sizeof(type_array[0]));
          ObObjType type =  type_array[abs((int32_t)random() % array_size)];

          int64_t int_value = 0;
          static char varchar_value[1024];
          ObString string_value;

          switch(type)
          {
          case ObIntType:
            int_value = random();
            size = get_int_size(int_value);
            obj.set_int(int_value);
            break;
          case ObVarcharType:
            int_value = labs(random() % 1024L);
            if (0 == int_value)
            {
              int_value = 1;
            }
            string_value.assign_ptr(varchar_value,static_cast<int32_t>(int_value));
            size =  2 + static_cast<int32_t>(int_value);
            obj.set_varchar(string_value);
            break;
          case ObPreciseDateTimeType:
            obj.set_precise_datetime(random());
            size = 8;
            break;
          case ObCreateTimeType:
            obj.set_createtime(random());
            size = 8;
            break;
          case ObModifyTimeType:
            obj.set_createtime(random());
            size = 8;
            break;
          default:
            TBSYS_LOG(WARN,"should not be here");
            break;
          };
        }
        return size;
      }

      int generate_row(ObCompactRow& row,ObString& rowkey,int64_t& row_size)
      {
        int ret = 0;
        int64_t size = 0;
        ObObj obj;
        uint64_t col_id = 1;
        int32_t col_num = abs(static_cast<int32_t>(random()) % 12);
        obj.set_varchar(rowkey);

        if ((ret = row.add_col(col_id++,obj)) != 0)
        {
          fprintf(stderr,"add col failed,ret=%d",ret);
        }
        else
        {
          size += (rowkey.length() + 1 + 2 + 2 );//1 meta, 2 varchar len, 2 column id

          if (0 == col_num)
          {
            //test for delete row
            generate_obj(obj,true);
            if ((ret = row.add_col(col_id++,obj)) != 0)
            {
              fprintf(stderr,"add col failed,ret=%d",ret);
            }
            else
            {
              size += 1; //delete row have no col_id
            }
          }
          else
          {
            for(int32_t i=0; 0 == ret && i<col_num; ++i)
            {
              size += generate_obj(obj);
              size += 3; //col_id  + meta

              if ((ret = row.add_col(col_id++,obj)) != 0)
              {
                fprintf(stderr,"add col failed,ret=%d",ret);
              }
              else if (row.get_row_size() != size)
              {
                TBSYS_LOG(WARN,"row size not match,row.get_row_size() is %ld,size is %ld",row.get_row_size(),size);
                ret = OB_ERROR;
              }
            }
          }
        }

        if ((0 == ret) && (0 == (ret = row.set_end()) ))
        {
          row_size = size + 1;
        }
        return ret;
      }

      virtual void TearDown()
        {
          if (writer != NULL)
          {
            delete writer;
            writer = NULL;
          }
        }
    protected:
      uint64_t table_id;
      ObCompactSSTableMem* writer;
    };

    TEST_F(TestCompactSSTableMem,write)
    {
      int ret = OB_SUCCESS;

      if (OB_SUCCESS == ret)
      {
        ObCompactRow row;
        char rowkey_buf[16];
        ObString rowkey;

        ObObj obj;

        int64_t approx;
        for(int64_t i=0;i<16384 && OB_SUCCESS == ret;++i)
        {
          row.reset();
          snprintf(rowkey_buf,sizeof(rowkey_buf),"%08ld",i);
          rowkey.assign_ptr(rowkey_buf,static_cast<int32_t>(strlen(rowkey_buf)));
          obj.set_varchar(rowkey);
          row.add_col(1,obj);
          obj.set_int(97);
          row.add_col(16,obj);
          row.set_end();
          if ((ret = writer->append_row(1001L,row,approx)) != OB_SUCCESS)
          {
            fprintf(stderr,"append row failed\n");
          }
        }
        EXPECT_EQ(0,ret);
      }
    }

    TEST_F(TestCompactSSTableMem,write_mix)
    {
      int ret = OB_SUCCESS;

      ObCompactRow row;
      char rowkey_buf[16];
      ObString rowkey;
      ObObj obj;
      int64_t approx = 0;
      int64_t total_size = 0;
      int64_t row_size = 0;
      int64_t row_count = 500000;
      for(int64_t i=0;i < row_count && OB_SUCCESS == ret;++i)
      {
        row.reset();
        snprintf(rowkey_buf,sizeof(rowkey_buf),"%08ld",i);
        rowkey.assign_ptr(rowkey_buf,static_cast<int32_t>(strlen(rowkey_buf)));

        ASSERT_EQ(0,generate_row(row,rowkey,row_size));
        ASSERT_EQ(row_size,row.get_row_size());
        ASSERT_EQ(0,writer->append_row(1001L,row,approx));
        total_size += row_size;
      }

      ASSERT_EQ(ObVersion::get_version(10,10,true),writer->get_data_version());
      ASSERT_EQ(0,writer->get_frozen_time());
      ASSERT_EQ(row_count,writer->get_row_count());
      ASSERT_EQ(row_count,writer->row_index_.offset_);
      ASSERT_EQ(((row_count / 8192) + 1) * 8192,writer->row_index_.max_index_entry_num_);
    }
    

    TEST_F(TestCompactSSTableMem,write_order)
    {
      int ret = OB_SUCCESS;

      if (OB_SUCCESS == ret)
      {
        ObCompactRow row;
        char rowkey_buf[16];
        ObString rowkey;

        ObObj obj;

        int64_t approx;
        for(int64_t i=10;i>0 && OB_SUCCESS == ret;--i)
        {
          row.reset();
          snprintf(rowkey_buf,sizeof(rowkey_buf),"%08ld",i);

          rowkey.assign_ptr(rowkey_buf,static_cast<int32_t>(strlen(rowkey_buf)));
          obj.set_varchar(rowkey);
          row.add_col(1,obj);
          obj.set_int(97);
          row.add_col(16,obj);
          row.set_end();
          if (10 == i)
          {
            ASSERT_EQ(0,writer->append_row(1001L,row,approx));
          }
          else
          {
            ASSERT_NE(0,writer->append_row(1001L,row,approx));
          }
        }
      }
    }

  }
}
int main(int argc, char** argv)
{
  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
