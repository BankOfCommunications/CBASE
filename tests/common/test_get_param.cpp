/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * test_get_param.cpp for test get param 
 *
 * Authors: 
 *   huating <huating.zmq@taobao.com>
 *
 */

#include <tblog.h>
#include <gtest/gtest.h>
#include "ob_action_flag.h"
#include "ob_malloc.h"
#include "ob_get_param.h"
#include "test_rowkey_helper.h"

using namespace std;
using namespace oceanbase::common;

namespace oceanbase
{
  namespace tests
  {
    namespace common 
    {
      static const int64_t table_id = 100;
      static const int64_t DISK_NUM = 6;
      static const int64_t SSTABLE_NUM = DISK_NUM * 2;
      static const int64_t SSTABLE_ROW_NUM = 100;
      static const int64_t ROW_NUM = SSTABLE_NUM * SSTABLE_ROW_NUM;
      static const int64_t COL_NUM = 5;
      static const int64_t OB_MAX_GET_COLUMN_NUMBER = 128;
      static const ObString table_name(strlen("sstable") + 1, strlen("sstable") + 1, (char*)"sstable");
      static ObCellInfo** cell_infos;
      static char* row_key_strs[ROW_NUM][COL_NUM];
      static const int64_t SERIALIZE_BUF_SIZE = 2048 * 1024;
      static char* serialize_buf = NULL;
      static CharArena  allocator_;

      /// bool operator==(const ObCellInfo &a, const ObCellInfo &b)
      /// {
      ///   return ((a.column_id_ == b.column_id_)
      ///     && (a.column_name_ == b.column_name_)
      ///     && (a.row_key_ == b.row_key_)
      ///     && (a.table_id_ == b.table_id_)
      ///     && (a.table_name_ == b.table_name_)
      ///     && (a.value_ == b.value_));
      /// }

      class TestObGetParam : public ::testing::Test
      {
      public:
        static void SetUpTestCase()
        {
          int err = OB_SUCCESS;

          TBSYS_LOGGER.setLogLevel("ERROR");
          err = ob_init_memory_pool();
          ASSERT_EQ(OB_SUCCESS, err);

          serialize_buf = new char[SERIALIZE_BUF_SIZE];
          //malloc
          cell_infos = new ObCellInfo*[ROW_NUM];
          for (int64_t i = 0; i < ROW_NUM; ++i)
          {
            cell_infos[i] = new ObCellInfo[COL_NUM];
          }

          for (int64_t i = 0; i < ROW_NUM; ++i)
          {
            for (int64_t j = 0; j < COL_NUM; ++j)
            {
              row_key_strs[i][j] = new char[50];
            }
          }

          // init cell infos
          for (int64_t i = 0; i < ROW_NUM; ++i)
          {
            for (int64_t j = 0; j < COL_NUM; ++j)
            {
              cell_infos[i][j].table_id_ = table_id;
              sprintf(row_key_strs[i][j], "row_key_%08ld", i);
              cell_infos[i][j].row_key_ = make_rowkey(row_key_strs[i][j], &allocator_);
              cell_infos[i][j].column_id_ = j + 2;
              cell_infos[i][j].value_.set_int(1000 + i * COL_NUM + j);
            }
          }
        }

        static void TearDownTestCase()
        {
          if (NULL != serialize_buf)
          {
            delete [] serialize_buf;
          }

          for (int64_t i = 0; i < ROW_NUM; ++i)
          {
            for (int64_t j = 0; j < COL_NUM; ++j)
            {
              if (NULL != row_key_strs[i][j])
              {
                delete[] row_key_strs[i][j];
                row_key_strs[i][j] = NULL;
              }
            }
          }

          for (int64_t i = 0; i < ROW_NUM; ++i)
          {
            if (NULL != cell_infos[i])
            {
              delete[] cell_infos[i];
              cell_infos[i] = NULL;
            }
          }
          if (NULL != cell_infos)
          {
            delete[] cell_infos;
          }
        }

        virtual void SetUp()
        {
  
        }
      
        virtual void TearDown()
        {
      
        }
      };

      TEST_F(TestObGetParam, test_get_param_one_cell)
      {
        ObCellInfo cell_info;

        cell_info = cell_infos[0][0];
        cell_info.column_id_ = 0;

        ObGetParam get_param(cell_info);
        const ObGetParam::ObRowIndex* row_index;

        EXPECT_EQ(1, get_param.get_cell_size());
        EXPECT_EQ(1, get_param.get_row_size());

        row_index = get_param.get_row_index();
        EXPECT_TRUE(cell_infos[0][0].row_key_ == get_param[row_index[0].offset_]->row_key_);
      }

      TEST_F(TestObGetParam, test_get_param_one_row)
      {
        int ret = OB_SUCCESS;
        ObGetParam get_param;
        const ObGetParam::ObRowIndex* row_index;

        for (int64_t i = 0; i < COL_NUM; ++i)
        {
          ret = get_param.add_cell(cell_infos[0][i]);
          EXPECT_EQ(OB_SUCCESS, ret);
        }

        EXPECT_EQ(COL_NUM, get_param.get_cell_size());
        EXPECT_EQ(1, get_param.get_row_size());

        row_index = get_param.get_row_index();
        EXPECT_TRUE(cell_infos[0][0].row_key_ == get_param[row_index[0].offset_]->row_key_);
      }

      TEST_F(TestObGetParam, test_get_param_five_rows)
      {
        int ret = OB_SUCCESS;
        ObGetParam get_param;
        const ObGetParam::ObRowIndex* row_index;

        for (int64_t i = 0; i < 5; ++i)
        {
          ret = get_param.add_cell(cell_infos[i][0]);
          EXPECT_EQ(OB_SUCCESS, ret);
        }

        EXPECT_EQ(COL_NUM, get_param.get_cell_size());
        EXPECT_EQ(5, get_param.get_row_size());

        row_index = get_param.get_row_index();
        for (int64_t i = 0; i < 5; ++i)
        {
          EXPECT_TRUE(cell_infos[i][0].row_key_ == get_param[row_index[i].offset_]->row_key_);
        }
      }

      TEST_F(TestObGetParam, test_get_param_max_rows)
      {
        int ret = OB_SUCCESS;
        ObGetParam get_param;
        const ObGetParam::ObRowIndex* row_index;

        for (int64_t i = 0; i < OB_MAX_GET_COLUMN_NUMBER; ++i)
        {
          ret = get_param.add_cell(cell_infos[i][0]);
          ASSERT_EQ(OB_SUCCESS, ret);
        }

        EXPECT_EQ(OB_MAX_GET_COLUMN_NUMBER, get_param.get_cell_size());
        EXPECT_EQ(OB_MAX_GET_COLUMN_NUMBER, get_param.get_row_size());

        row_index = get_param.get_row_index();
        for (int64_t i = 0; i < OB_MAX_GET_COLUMN_NUMBER; ++i)
        {
          EXPECT_TRUE(cell_infos[i][0].row_key_ == get_param[row_index[i].offset_]->row_key_);
        }
      }

      TEST_F(TestObGetParam, test_get_param_three_rows)
      {
        int ret = OB_SUCCESS;
        ObGetParam get_param;
        const ObGetParam::ObRowIndex* row_index;

        for (int j = 0; j < 3; ++j)
        {
          for (int64_t i = 0; i < COL_NUM; ++i)
          {
            ret = get_param.add_cell(cell_infos[j][i]);
            EXPECT_EQ(OB_SUCCESS, ret);
          }
        }

        EXPECT_EQ(COL_NUM * 3, get_param.get_cell_size());
        EXPECT_EQ(3, get_param.get_row_size());

        row_index = get_param.get_row_index();
        for (int j = 0; j < 3; ++j)
        {
          EXPECT_TRUE(cell_infos[j][0].row_key_ == get_param[row_index[j].offset_]->row_key_);
        }
      }

      TEST_F(TestObGetParam, test_get_param_ten_rows)
      {
        int ret = OB_SUCCESS;
        ObGetParam get_param;
        const ObGetParam::ObRowIndex* row_index;

        for (int j = 0; j < 10; ++j)
        {
          for (int64_t i = j % COL_NUM; i < COL_NUM; ++i)
          {
            ret = get_param.add_cell(cell_infos[j][i]);
            EXPECT_EQ(OB_SUCCESS, ret);
          }
        }

        EXPECT_EQ(30, get_param.get_cell_size());
        EXPECT_EQ(10, get_param.get_row_size());

        row_index = get_param.get_row_index();
        for (int j = 0; j < 10; ++j)
        {
          EXPECT_TRUE(cell_infos[j][0].row_key_ == get_param[row_index[j].offset_]->row_key_);
        }
      }

      TEST_F(TestObGetParam, test_get_param_max_cells)
      {
        int ret = OB_SUCCESS;
        ObGetParam get_param;
        const ObGetParam::ObRowIndex* row_index;

        for (int j = 0; j < ROW_NUM; ++j)
        {
          for (int64_t i = 0; i < COL_NUM; ++i)
          {
            ret = get_param.add_cell(cell_infos[j][i]);
            EXPECT_EQ(OB_SUCCESS, ret);
          }
        }

        EXPECT_EQ(ROW_NUM * COL_NUM, get_param.get_cell_size());
        EXPECT_EQ(ROW_NUM, get_param.get_row_size());

        row_index = get_param.get_row_index();
        for (int j = 0; j < ROW_NUM; ++j)
        {
          EXPECT_TRUE(cell_infos[j][0].row_key_ == get_param[row_index[j].offset_]->row_key_);
        }
      }

      TEST_F(TestObGetParam, test_get_param_max_cells2)
      {
        int ret = OB_SUCCESS;
        ObGetParam get_param;
        ObCellInfo param_cell;
        const ObGetParam::ObRowIndex* row_index;

        for (int j = 0; j < ROW_NUM; ++j)
        {
          for (int64_t i = 0; i < COL_NUM; ++i)
          {
            param_cell.reset();
            param_cell = cell_infos[j][i];
            param_cell.table_id_ = table_id + i;
            ret = get_param.add_cell(param_cell);
            EXPECT_EQ(OB_SUCCESS, ret);
          }
        }

        EXPECT_EQ(ROW_NUM * COL_NUM, get_param.get_cell_size());
        EXPECT_EQ(ROW_NUM * 5, get_param.get_row_size());

        row_index = get_param.get_row_index();
        for (int j = 0; j < ROW_NUM * 5; ++j)
        {
          EXPECT_TRUE(cell_infos[j/5][j%5].row_key_ == get_param[row_index[j].offset_]->row_key_);
          EXPECT_EQ(get_param[row_index[j].offset_]->table_id_, (uint64_t)(table_id + j%5));
        }
      }

      TEST_F(TestObGetParam, test_add_cells_wrong)
      {
        int ret = OB_SUCCESS;
        ObGetParam get_param;
        ObCellInfo param_cell;

        ret = get_param.add_cell(cell_infos[0][0]);
        EXPECT_EQ(OB_SUCCESS, ret);
        param_cell = cell_infos[0][2];
        param_cell.table_name_ = table_name;
        ret = get_param.add_cell(param_cell);
        EXPECT_EQ(OB_ERROR, ret);
      }

      TEST_F(TestObGetParam, test_rollback)
      {
        int ret = OB_SUCCESS;
        ObGetParam get_param;
        const ObGetParam::ObRowIndex* row_index;

        for (int j = 0; j < 10; ++j)
        {
          for (int64_t i = 0; i < COL_NUM; ++i)
          {
            ret = get_param.add_cell(cell_infos[j][i]);
            EXPECT_EQ(OB_SUCCESS, ret);
          }
        }

        EXPECT_EQ(50, get_param.get_cell_size());
        EXPECT_EQ(10, get_param.get_row_size());

        ret = get_param.rollback();
        EXPECT_EQ(OB_SUCCESS, ret);
        EXPECT_EQ(45, get_param.get_cell_size());
        EXPECT_EQ(9, get_param.get_row_size());

        row_index = get_param.get_row_index();
        for (int j = 0; j < 9; ++j)
        {
          EXPECT_TRUE(cell_infos[j][0].row_key_ == get_param[row_index[j].offset_]->row_key_);
        }

        ret = get_param.rollback(COL_NUM);
        EXPECT_EQ(OB_SUCCESS, ret);
        EXPECT_EQ(40, get_param.get_cell_size());
        EXPECT_EQ(8, get_param.get_row_size());

        row_index = get_param.get_row_index();
        for (int j = 0; j < 8; ++j)
        {
          EXPECT_TRUE(cell_infos[j][0].row_key_ == get_param[row_index[j].offset_]->row_key_);
        }
      }

      TEST_F(TestObGetParam, test_reset)
      {
        int ret = OB_SUCCESS;
        ObGetParam get_param;

        EXPECT_EQ(0, get_param.get_cell_size());
        EXPECT_EQ(0, get_param.get_row_size());

        get_param.reset();
        EXPECT_EQ(OB_SUCCESS, ret);
        EXPECT_EQ(0, get_param.get_cell_size());
        EXPECT_EQ(0, get_param.get_row_size());

        for (int j = 0; j < 10; ++j)
        {
          for (int64_t i = 0; i < COL_NUM; ++i)
          {
            ret = get_param.add_cell(cell_infos[j][i]);
            EXPECT_EQ(OB_SUCCESS, ret);
          }
        }

        EXPECT_EQ(50, get_param.get_cell_size());
        EXPECT_EQ(10, get_param.get_row_size());

        get_param.reset();
        EXPECT_EQ(OB_SUCCESS, ret);
        EXPECT_EQ(0, get_param.get_cell_size());
        EXPECT_EQ(0, get_param.get_row_size());
      }

      TEST_F(TestObGetParam, test_simple_serialize)
      {
        int ret = OB_SUCCESS;
        ObGetParam get_param;
        ObGetParam des_get_param;
        ObCellInfo* cell = NULL;
        int64_t pos = 0;
        get_param.set_is_result_cached(false);
        get_param.set_is_read_consistency(false);

        for (int j = 0; j < 10; ++j)
        {
          for (int64_t i = 0; i < COL_NUM; ++i)
          {
            ret = get_param.add_cell(cell_infos[j][i]);
            EXPECT_EQ(OB_SUCCESS, ret);
          }
        }
        EXPECT_EQ(50, get_param.get_cell_size());
        EXPECT_EQ(10, get_param.get_row_size());

        ret = get_param.serialize(serialize_buf, SERIALIZE_BUF_SIZE, pos);
        EXPECT_EQ(OB_SUCCESS, ret);
        EXPECT_EQ(pos, get_param.get_serialize_size());

        pos = 0;
        ret = des_get_param.deserialize(serialize_buf, SERIALIZE_BUF_SIZE, pos);
        EXPECT_EQ(OB_SUCCESS, ret);
        EXPECT_EQ(pos, des_get_param.get_serialize_size());
        EXPECT_FALSE(des_get_param.get_is_result_cached());
        EXPECT_FALSE(des_get_param.get_is_read_consistency());

        EXPECT_EQ(50, des_get_param.get_cell_size());
        EXPECT_EQ(10, des_get_param.get_row_size());
        for (int j = 0; j < 10; ++j)
        {
          for (int64_t i = 0; i < COL_NUM; ++i)
          {
            cell = des_get_param[j * COL_NUM + i];
            EXPECT_TRUE(cell->table_id_ == cell_infos[j][i].table_id_);
            EXPECT_TRUE(cell->column_id_ == cell_infos[j][i].column_id_);
            EXPECT_TRUE(cell->row_key_ == cell_infos[j][i].row_key_);
          }
        }
      }

      TEST_F(TestObGetParam, test_serialize_deserialize_maxcol)
      {
        int ret = OB_SUCCESS;
        ObGetParam get_param;
        ObGetParam des_get_param;
        ObVersionRange ver_range;
        ObCellInfo* cell = NULL;
        int64_t pos = 0;
        //
        get_param.set_is_result_cached(false);

        for (int j = 0; j < ROW_NUM; ++j)
        {
          for (int64_t i = 0; i < COL_NUM; ++i)
          {
            ret = get_param.add_cell(cell_infos[j][i]);
            EXPECT_EQ(OB_SUCCESS, ret);
          }
        }
        EXPECT_EQ(ROW_NUM * COL_NUM, get_param.get_cell_size());
        EXPECT_EQ(ROW_NUM, get_param.get_row_size());

        ret = get_param.serialize(serialize_buf, SERIALIZE_BUF_SIZE, pos);
        EXPECT_EQ(OB_SUCCESS, ret);
        EXPECT_EQ(pos, get_param.get_serialize_size());

        pos = 0;
        ret = des_get_param.deserialize(serialize_buf, SERIALIZE_BUF_SIZE, pos);
        EXPECT_EQ(OB_SUCCESS, ret);
        EXPECT_EQ(pos, des_get_param.get_serialize_size());

        EXPECT_EQ(ROW_NUM * COL_NUM, des_get_param.get_cell_size());
        EXPECT_EQ(ROW_NUM, des_get_param.get_row_size());
        for (int j = 0; j < ROW_NUM; ++j)
        {
          for (int64_t i = 0; i < COL_NUM; ++i)
          {
            cell = des_get_param[j * COL_NUM + i];
            EXPECT_TRUE(cell->table_id_ == cell_infos[j][i].table_id_);
            EXPECT_TRUE(cell->column_id_ == cell_infos[j][i].column_id_);
            EXPECT_TRUE(cell->row_key_ == cell_infos[j][i].row_key_);
            EXPECT_FALSE(des_get_param.get_is_result_cached());
            ver_range = des_get_param.get_version_range();
            EXPECT_EQ(0, ver_range.start_version_);
            EXPECT_EQ(0, ver_range.end_version_);
          }
        }

        //reuse get_param
        des_get_param.reset();
        pos = 0;
        ret = des_get_param.deserialize(serialize_buf, SERIALIZE_BUF_SIZE, pos);
        EXPECT_EQ(OB_SUCCESS, ret);
        EXPECT_EQ(pos, des_get_param.get_serialize_size());
        EXPECT_FALSE(des_get_param.get_is_result_cached());
        EXPECT_TRUE(des_get_param.get_is_read_consistency());

        EXPECT_EQ(ROW_NUM * COL_NUM, des_get_param.get_cell_size());
        EXPECT_EQ(ROW_NUM, des_get_param.get_row_size());
        for (int j = 0; j < ROW_NUM; ++j)
        {
          for (int64_t i = 0; i < COL_NUM; ++i)
          {
            cell = des_get_param[j * COL_NUM + i];
            EXPECT_TRUE(cell->table_id_ == cell_infos[j][i].table_id_);
            EXPECT_TRUE(cell->column_id_ == cell_infos[j][i].column_id_);
            EXPECT_TRUE(cell->row_key_ == cell_infos[j][i].row_key_);
            EXPECT_FALSE(des_get_param.get_is_result_cached());
            ver_range = des_get_param.get_version_range();
            EXPECT_EQ(0, ver_range.start_version_);
            EXPECT_EQ(0, ver_range.end_version_);
          }
        }
      }

      TEST_F(TestObGetParam, test_serialize_deserialize_two_table)
      {
        int ret = OB_SUCCESS;
        ObGetParam get_param;
        ObGetParam des_get_param;
        ObVersionRange ver_range;
        ObCellInfo* cell = NULL;
        ObCellInfo cell_param;
        int64_t pos = 0;

        for (int j = 0; j < ROW_NUM; ++j)
        {
          for (int64_t i = 0; i < COL_NUM; ++i)
          {
            ret = get_param.add_cell(cell_infos[j][i]);
            EXPECT_EQ(OB_SUCCESS, ret);
          }
          for (int64_t i = 0; i < COL_NUM; ++i)
          {
            cell_param = cell_infos[j][i];
            cell_param.table_id_ += 1;
            ret = get_param.add_cell(cell_param);
            EXPECT_EQ(OB_SUCCESS, ret);
          }
        }
        EXPECT_EQ(ROW_NUM * COL_NUM * 2, get_param.get_cell_size());
        EXPECT_EQ(ROW_NUM * 2, get_param.get_row_size());

        ret = get_param.serialize(serialize_buf, SERIALIZE_BUF_SIZE, pos);
        EXPECT_EQ(OB_SUCCESS, ret);
        EXPECT_EQ(pos, get_param.get_serialize_size());

        pos = 0;
        ret = des_get_param.deserialize(serialize_buf, SERIALIZE_BUF_SIZE, pos);
        EXPECT_EQ(OB_SUCCESS, ret);
        EXPECT_EQ(pos, des_get_param.get_serialize_size());

        EXPECT_EQ(ROW_NUM * COL_NUM * 2, des_get_param.get_cell_size());
        EXPECT_EQ(ROW_NUM * 2, des_get_param.get_row_size());
        for (int j = 0; j < ROW_NUM; ++j)
        {
          for (int64_t i = 0; i < COL_NUM; ++i)
          {
            cell = des_get_param[j * COL_NUM * 2 + i];
            EXPECT_TRUE(cell->table_id_ == cell_infos[j][i].table_id_);
            EXPECT_TRUE(cell->column_id_ == cell_infos[j][i].column_id_);
            EXPECT_TRUE(cell->row_key_ == cell_infos[j][i].row_key_);
            EXPECT_FALSE(des_get_param.get_is_result_cached());
            ver_range = des_get_param.get_version_range();
            EXPECT_EQ(0, ver_range.start_version_);
            EXPECT_EQ(0, ver_range.end_version_);
          }
          for (int64_t i = 0; i < COL_NUM; ++i)
          {
            cell = des_get_param[j * COL_NUM * 2 + COL_NUM + i];
            EXPECT_EQ(cell->table_id_, cell_infos[j][i].table_id_ + 1);
            EXPECT_TRUE(cell->column_id_ == cell_infos[j][i].column_id_);
            EXPECT_TRUE(cell->row_key_ == cell_infos[j][i].row_key_);
            EXPECT_FALSE(des_get_param.get_is_result_cached());
            ver_range = des_get_param.get_version_range();
            EXPECT_EQ(0, ver_range.start_version_);
            EXPECT_EQ(0, ver_range.end_version_);
          }
        }
      }

      TEST_F(TestObGetParam, deep_copy_args)
      {
        const char *c_str = NULL;
        ObString val;
        ObString str;
        ObStringBuf buffer;
        ObCellInfo cell;
        ObGetParam get_param(true);
        c_str = "table_name";
        str.assign((char*)c_str, static_cast<int32_t>(strlen(c_str)));
        EXPECT_EQ(OB_SUCCESS, buffer.write_string(str, &(cell.table_name_)));

        c_str = "column_name";
        str.assign((char*)c_str, static_cast<int32_t>(strlen(c_str)));
        EXPECT_EQ(OB_SUCCESS, buffer.write_string(str, &(cell.column_name_)));

        c_str = "rowkey";
        str.assign((char*)c_str, static_cast<int32_t>(strlen(c_str)));
        EXPECT_EQ(OB_SUCCESS, buffer.write_string(TestRowkeyHelper(str, &allocator_), &(cell.row_key_)));

        c_str = "varchar";
        str.assign((char*)c_str, static_cast<int32_t>(strlen(c_str)));
        EXPECT_EQ(OB_SUCCESS, buffer.write_string(str, &(val)));
        cell.value_.set_varchar(val);

        EXPECT_EQ(get_param.add_cell(cell), OB_SUCCESS);

        EXPECT_TRUE((*get_param[0]) == cell);
        EXPECT_NE(get_param[0]->table_name_.ptr() , cell.table_name_.ptr());
        EXPECT_NE(get_param[0]->column_name_.ptr() , cell.column_name_.ptr());
        EXPECT_NE(get_param[0]->row_key_.ptr() , cell.row_key_.ptr());

        get_param.reset(true);
        EXPECT_EQ(get_param.add_only_one_cell(cell), OB_SUCCESS);
        EXPECT_TRUE((*get_param[0]) == cell);
        EXPECT_NE(get_param[0]->table_name_.ptr() , cell.table_name_.ptr());
        EXPECT_NE(get_param[0]->column_name_.ptr() , cell.column_name_.ptr());
        EXPECT_NE(get_param[0]->row_key_.ptr() , cell.row_key_.ptr());

        get_param.reset(false);
        EXPECT_EQ(get_param.add_cell(cell), OB_SUCCESS);
        EXPECT_TRUE((*get_param[0]) == cell);
        EXPECT_EQ(get_param[0]->table_name_.ptr() , cell.table_name_.ptr());
        EXPECT_EQ(get_param[0]->column_name_.ptr() , cell.column_name_.ptr());
        EXPECT_EQ(get_param[0]->row_key_.ptr() , cell.row_key_.ptr());
      }
    }//end namespace common
  }//end namespace tests
}//end namespace oceanbase

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
