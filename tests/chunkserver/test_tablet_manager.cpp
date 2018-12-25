/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *  test_tablet_manager.cpp for test tablet manager
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#include <tblog.h>
#include <gtest/gtest.h>
#include "ob_chunk_server_main.h"
#include "mock_root_server.h"
#include "test_helper.h"
#include "ob_scanner.h"
#include "common/ob_base_client.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::chunkserver;

namespace oceanbase
{
  namespace tests
  {
    namespace chunkserver
    {
      static ObTabletManager tablet_manager;
      static TabletManagerIniter manager_initer(tablet_manager);

      class TestTabletManager : public ::testing::Test
      {
      public:
        static void SetUpTestCase()
        {
          int err = OB_SUCCESS;
          err = manager_initer.init(true);
          ASSERT_EQ(OB_SUCCESS, err);
        }

        static void TearDownTestCase()
        {
        }

      public:
        virtual void SetUp()
        {

        }

        virtual void TearDown()
        {

        }

        void tablet_scan_and_check(ObTabletManager& tablet_mgr, int64_t index)
        {
          int err = OB_SUCCESS;
          ObScanParam scan_param;
          ObNewRange scan_range;
          ObCellInfo** cell_infos;
          ObCellInfo* ci;
          ObCellInfo expected;
          ObString table_name(strlen("sstable") + 1, strlen("sstable") + 1, (char*)"sstable");

          cell_infos = manager_initer.get_mult_sstable_builder().get_cell_infos(index);
          scan_range.table_id_ = SSTableBuilder::table_id;
          scan_range.start_key_ = cell_infos[0][0].row_key_;
          scan_range.end_key_ =
            cell_infos[SSTableBuilder::ROW_NUM - 1][SSTableBuilder::COL_NUM - 1].row_key_;
          scan_range.border_flag_.set_inclusive_start();
          scan_range.border_flag_.set_inclusive_end();

          scan_param.set(SSTableBuilder::table_id, table_name, scan_range);
          for (int64_t j = 0; j < SSTableBuilder::COL_NUM; ++j)
          {
            scan_param.add_column(cell_infos[0][j].column_id_);
          }
          reset_query_thread_local_buffer();

          ObScanner scanner;
          err = tablet_mgr.scan(scan_param, scanner);
          EXPECT_EQ(OB_SUCCESS, err);

          // check result
          int64_t count = 0;
          sstable::ObSSTableScanner *seq_scanner = GET_TSI_MULT(sstable::ObSSTableScanner, TSI_CS_SSTABLE_SCANNER_1);
          if (NULL == seq_scanner)
          {
            TBSYS_LOG(ERROR, "failed to get thread local sequence scaner, seq_scanner=%p",
                seq_scanner);
            err= OB_ALLOCATE_MEMORY_FAILED;
          }
          else
          {
            while (OB_SUCCESS == err && OB_SUCCESS == (err = seq_scanner->next_cell()))
            {
              err = seq_scanner->get_cell(&ci);
              EXPECT_EQ(OB_SUCCESS, err);
              expected = cell_infos[count / SSTableBuilder::COL_NUM][count % SSTableBuilder::COL_NUM];
              expected.table_name_ = table_name;
              check_cell(expected, *ci);
              ++count;
            }
          }
          EXPECT_EQ(SSTableBuilder::ROW_NUM * SSTableBuilder::COL_NUM, count);
          seq_scanner->cleanup();
          tablet_mgr.end_scan();
        }

        void tablet_get_and_check(ObTabletManager& tablet_mgr, int64_t index)
        {
          int ret = OB_SUCCESS;
          ObGetParam get_param;
          ObCellInfo param_cell;
          ObScanner scanner;
          ObCellInfo** cell_infos;
          ObCellInfo ci;
          ObCellInfo expected;
          ObString table_name(strlen("sstable") + 1, strlen("sstable") + 1, (char*)"sstable");;

          cell_infos = manager_initer.get_mult_sstable_builder().get_cell_infos(index);
          for (int i = 0; i < SSTableBuilder::ROW_NUM; i++)
          {
            param_cell = cell_infos[i][0];
            param_cell.column_id_ = 0;
            ret = get_param.add_cell(param_cell);
            EXPECT_EQ(OB_SUCCESS, ret);
          }

          ret = tablet_mgr.get(get_param, scanner);
          ASSERT_EQ(OB_SUCCESS, ret);

          // check result
          int64_t count = 0;
          int64_t col_num = SSTableBuilder::COL_NUM + 1;
          ObScannerIterator iter;
          for (iter = scanner.begin(); iter != scanner.end(); iter++)
          {
            EXPECT_EQ(OB_SUCCESS, iter.get_cell(ci));
            if (count % col_num == 0)
            {
              // rowkey column
              EXPECT_EQ((uint64_t)1, ci.column_id_);
            }
            else
            {
              expected = cell_infos[count / col_num][count % col_num - 1];
              expected.table_name_ = table_name;
              check_cell(expected, ci);
            }
            ++count;
          }
          EXPECT_EQ(SSTableBuilder::ROW_NUM * col_num, count);
          tablet_mgr.end_get();
        }

        void tablet_get_with_mult_tablets(ObTabletManager& tablet_mgr,
                                          int64_t row_index = 0,
                                          int64_t row_count_each = 1)
        {
          int ret = OB_SUCCESS;
          ObGetParam get_param;
          ObCellInfo param_cell;
          ObScanner scanner;
          ObCellInfo** cell_infos;
          ObCellInfo ci;
          ObCellInfo expected;
          ObString table_name(strlen("sstable") + 1, strlen("sstable") + 1, (char*)"sstable");;

          for (int j = 0; j < MultSSTableBuilder::SSTABLE_NUM; j++)
          {
            cell_infos = manager_initer.get_mult_sstable_builder().get_cell_infos(j);
            for (int64_t i = row_index; i < row_index + row_count_each; i++)
            {
              param_cell = cell_infos[i][0];
              param_cell.column_id_ = 0;
              ret = get_param.add_cell(param_cell);
              EXPECT_EQ(OB_SUCCESS, ret);
            }
          }

          ret = tablet_mgr.get(get_param, scanner);
          ASSERT_EQ(OB_SUCCESS, ret);

          // check result
          int64_t count = 0;
          int64_t sstable_index = 0;
          int64_t index_i = 0;
          int64_t col_num = SSTableBuilder::COL_NUM + 1;
          int64_t sstable_columns = row_count_each * col_num;
          ObScannerIterator iter;
          for (iter = scanner.begin(); iter != scanner.end(); iter++)
          {
            sstable_index = count / sstable_columns;
            cell_infos = manager_initer.get_mult_sstable_builder()
                         .get_cell_infos(sstable_index);
            index_i = row_index + (count - sstable_index * sstable_columns) / col_num;
            EXPECT_EQ(OB_SUCCESS, iter.get_cell(ci));
            if (count % col_num == 0)
            {
              // rowkey column
              EXPECT_EQ((uint64_t)1, ci.column_id_);
            }
            else
            {
              expected = cell_infos[index_i][count % col_num - 1];
              expected.table_name_ = table_name;
              check_cell(expected, ci);
            }
            ++count;
          }
          EXPECT_EQ(row_count_each * col_num
                    * MultSSTableBuilder::SSTABLE_NUM, count);
          tablet_mgr.end_get();
        }

        void get_tablet_info_list(ObTabletManager& tablet_mgr,
          ObTabletReportInfoList& tablet_list,
          const int64_t tablet_count)
        {
          int err = OB_SUCCESS;
          int64_t fill_count = 0;
          ObTabletReportInfo tablet_info;
          ObTablet* tablet = NULL;
          ObMultiVersionTabletImage& image = tablet_mgr.get_serving_tablet_image();

          tablet_list.reset();
          err = image.begin_scan_tablets();
          ASSERT_EQ(OB_SUCCESS, err);

          while (OB_SUCCESS == err && fill_count < tablet_count)
          {
            err = image.get_next_tablet(tablet);
            ASSERT_EQ(OB_SUCCESS, err);
            ASSERT_TRUE(NULL != tablet);

            tablet_info.tablet_info_.range_ = tablet->get_range();
            tablet_info.tablet_location_.tablet_version_ = tablet->get_data_version();
            tablet_info.tablet_location_.tablet_seq_ = tablet->get_sequence_num();

            err = tablet_list.add_tablet(tablet_info);
            ASSERT_EQ(OB_SUCCESS, err);
            err = image.release_tablet(tablet);
            ASSERT_EQ(OB_SUCCESS, err);
            fill_count++;
          }
          err = image.end_scan_tablets();
          ASSERT_EQ(OB_SUCCESS, err);
        }
      };

      TEST_F(TestTabletManager, test_load_tablets)
      {
        int err = OB_SUCCESS;
        ObTabletManager tablet_mgr;
        TabletManagerIniter initer(tablet_mgr);
        int32_t disk_no_array[MultSSTableBuilder::DISK_NUM];

        err = initer.init();
        ASSERT_EQ(OB_SUCCESS, err);

        for (int i = 0; i < MultSSTableBuilder::DISK_NUM; i++)
        {
          disk_no_array[i] = i + 1;
        }
        err = tablet_mgr.load_tablets(disk_no_array, MultSSTableBuilder::DISK_NUM);
        ASSERT_EQ(OB_SUCCESS, err);

        for (int i = 0; i < MultSSTableBuilder::SSTABLE_NUM; i++)
        {
          tablet_scan_and_check(tablet_manager, i);
          tablet_scan_and_check(tablet_mgr, i);
          tablet_get_and_check(tablet_manager, i);
          tablet_get_and_check(tablet_mgr, i);
        }
      }

      TEST_F(TestTabletManager, test_get_with_mult_tablets)
      {
        int err = OB_SUCCESS;
        ObTabletManager tablet_mgr;
        TabletManagerIniter initer(tablet_mgr);
        int32_t disk_no_array[MultSSTableBuilder::DISK_NUM];

        err = initer.init();
        ASSERT_EQ(OB_SUCCESS, err);

        for (int i = 0; i < MultSSTableBuilder::DISK_NUM; i++)
        {
          disk_no_array[i] = i + 1;
        }
        err = tablet_mgr.load_tablets(disk_no_array, MultSSTableBuilder::DISK_NUM);
        ASSERT_EQ(OB_SUCCESS, err);

        tablet_get_with_mult_tablets(tablet_manager, 0, 1);
        tablet_get_with_mult_tablets(tablet_manager, SSTableBuilder::ROW_NUM / 2, 1);
        tablet_get_with_mult_tablets(tablet_manager, SSTableBuilder::ROW_NUM - 1, 1);

        tablet_get_with_mult_tablets(tablet_mgr, 0, 1);
        tablet_get_with_mult_tablets(tablet_mgr, SSTableBuilder::ROW_NUM / 2, 1);
        tablet_get_with_mult_tablets(tablet_mgr, SSTableBuilder::ROW_NUM - 1, 1);
      }

      TEST_F(TestTabletManager, test_report_tablets)
      {
        int err = OB_SUCCESS;
        ObTabletManager tablet_mgr;
        const char* dst_addr = "localhost";
        TabletManagerIniter initer(tablet_mgr);
        int32_t disk_no_array[MultSSTableBuilder::DISK_NUM];

        err = initer.init();
        ASSERT_EQ(OB_SUCCESS, err);
        for (int i = 0; i < MultSSTableBuilder::DISK_NUM; i++)
        {
          disk_no_array[i] = i + 1;
        }
        err = tablet_mgr.load_tablets(disk_no_array, MultSSTableBuilder::DISK_NUM);
        ASSERT_EQ(OB_SUCCESS, err);

        ObChunkServerMain* cm = ObChunkServerMain::get_instance();
        ObChunkServer& cs = cm->get_chunk_server();
        cs.initialize();
        cs.get_config().root_server_ip.set_value(dst_addr);
        cs.get_config().root_server_port = MOCK_SERVER_LISTEN_PORT;

        /* init global root server */
        ObServer root_server = cs.get_root_server();

        // init global client manager
        ObBaseClient client_manager ;
        EXPECT_EQ(OB_SUCCESS, client_manager.initialize(root_server));

        cs.get_rpc_stub().init(cs.get_thread_specific_rpc_buffer(),
                               &client_manager.get_client_mgr());

        tbsys::CThread test_root_server_thread;
        MockRootServerRunner test_root_server;
        test_root_server_thread.start(&test_root_server, NULL);

        sleep(1);

        err = tablet_mgr.report_tablets();
        EXPECT_EQ(0, err);

        client_manager.destroy();
      }

      TEST_F(TestTabletManager, test_merge_tablets)
      {
        int err = OB_SUCCESS;
        ObTabletManager tablet_mgr;
        TabletManagerIniter initer(tablet_mgr);
        int32_t disk_no_array[MultSSTableBuilder::DISK_NUM];
        ObTabletReportInfoList tablet_list;
        int64_t merge_tablet_count_once = 6;

        err = initer.init();
        ASSERT_EQ(OB_SUCCESS, err);

        for (int i = 0; i < MultSSTableBuilder::DISK_NUM; i++)
        {
          disk_no_array[i] = i + 1;
        }
        err = tablet_mgr.load_tablets(disk_no_array, MultSSTableBuilder::DISK_NUM);
        ASSERT_EQ(OB_SUCCESS, err);

        for (int64_t j = 0; j < MultSSTableBuilder::SSTABLE_NUM / merge_tablet_count_once; ++j)
        {
          get_tablet_info_list(tablet_mgr, tablet_list, merge_tablet_count_once);
          err = tablet_mgr.merge_multi_tablets(tablet_list);
          ASSERT_EQ(OB_SUCCESS, err);
          ASSERT_EQ(j + 1, tablet_list.tablets_[0].tablet_location_.tablet_seq_);

          for (int i = 0; i < MultSSTableBuilder::SSTABLE_NUM; i++)
          {
            tablet_scan_and_check(tablet_mgr, i);
            tablet_get_and_check(tablet_mgr, i);
          }
          tablet_get_with_mult_tablets(tablet_mgr, 0, 1);
          tablet_get_with_mult_tablets(tablet_mgr, SSTableBuilder::ROW_NUM / 2, 1);
          tablet_get_with_mult_tablets(tablet_mgr, SSTableBuilder::ROW_NUM - 1, 1);
        }
      }

      TEST_F(TestTabletManager, test_merge_empty_tablets)
      {
        int err = OB_SUCCESS;
        ObTabletManager tablet_mgr;
        TabletManagerIniter initer(tablet_mgr);
        ObTabletReportInfoList tablet_list;
        int64_t merge_tablet_count_once = 6;

        err = initer.init(true, true);
        ASSERT_EQ(OB_SUCCESS, err);

        for (int64_t j = 0; j < MultSSTableBuilder::SSTABLE_NUM / merge_tablet_count_once; ++j)
        {
          get_tablet_info_list(tablet_mgr, tablet_list, merge_tablet_count_once);
          err = tablet_mgr.merge_multi_tablets(tablet_list);
          ASSERT_EQ(OB_SUCCESS, err);
          ASSERT_EQ(j + 1, tablet_list.tablets_[0].tablet_location_.tablet_seq_);
        }
      }

    } // end namespace chunkserver
  } // end namespace tests
} // end namespace oceanbase

class FooEnvironment : public testing::Environment
{
  public:
    virtual void SetUp()
    {
      TBSYS_LOGGER.setLogLevel("ERROR");
      ob_init_memory_pool();
      prepare_sstable_directroy(10);
    }
    virtual void TearDown()
    {
    }
};

int main(int argc, char** argv)
{
  testing::AddGlobalTestEnvironment(new FooEnvironment);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
