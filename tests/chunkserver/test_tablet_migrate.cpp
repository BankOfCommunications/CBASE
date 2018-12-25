#include <tblog.h>
#include <gtest/gtest.h>
#include "common/ob_server.h"
#include "common/ob_result.h"
#include "ob_action_flag.h"
#include "thread_buffer.h"
#include "ob_scanner.h"
#include "ob_sstable_reader.h"
#include "ob_sstable_writer.h"
#include "ob_sstable_getter.h"
#include "ob_tablet_manager.h"
#include "ob_chunk_server.h"
using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::chunkserver;

namespace oceanbase
{
  namespace tests
  {
    namespace chunkserver
    {
      static const int64_t table_id = 100;
      static const int64_t sstable_file_id = 1001;
      static const int64_t sstable_file_offset = 0;
      static const int64_t DISK_NUM = 1;
      static const int64_t SSTABLE_NUM = DISK_NUM * 2;
      static const int64_t SSTABLE_ROW_NUM = 100;
      static const int64_t ROW_NUM = SSTABLE_NUM * SSTABLE_ROW_NUM;
      static const int64_t COL_NUM = 5;
      static const int64_t NON_EXISTENT_ROW_NUM = 100;
      static const ObString table_name(strlen("sstable") + 1, strlen("sstable") + 1, "sstable");
      static const int MOCK_SERVER_LISTEN_PORT = 33248;
      int64_t num_file = 0 ;
      char path[ObTablet::MAX_SSTABLE_PER_TABLET][OB_MAX_FILE_NAME_LENGTH];

      static char sstable_file_path[OB_MAX_FILE_NAME_LENGTH];
      static ObCellInfo** cell_infos;
      static char* row_key_strs[ROW_NUM + NON_EXISTENT_ROW_NUM][COL_NUM];
      static ObTabletManager tablet_mgr;
      static ObChunkServer chunk_server;
      // static ObChunkService  chunk_service;
      ObRange ranges[SSTABLE_NUM];
      ObSSTableId sst_ids[SSTABLE_NUM];

      class TestObTabletMigrate : public ::testing::Test
      {

      public:

        static  int cs_migrate(const common::ObRange& range,
                               const common::ObServer& src_server, const common::ObServer& dest_server, bool keep_src)
          {
            static const int MY_VERSION = 1;
            int ret = OB_SUCCESS;
            ThreadSpecificBuffer::Buffer* my_buffer = chunk_server.get_rpc_buffer();
            if (my_buffer != NULL)
            {
              my_buffer->reset();
              ObDataBuffer thread_buff(my_buffer->current(), my_buffer->remain());
              ret = range.serialize(thread_buff.get_data(),
                                    thread_buff.get_capacity(), thread_buff.get_position());

              if (OB_SUCCESS == ret)
              {
                ret = dest_server.serialize(thread_buff.get_data(),
                                            thread_buff.get_capacity(), thread_buff.get_position());
              }
              if (OB_SUCCESS == ret)
              {
                ret = common::serialization::encode_bool(thread_buff.get_data(),
                                                         thread_buff.get_capacity(), thread_buff.get_position(), keep_src);
              }
              if (OB_SUCCESS == ret)
              {
                ret = chunk_server.get_client_manager().send_request(src_server, OB_CS_MIGRATE, MY_VERSION, 20000, thread_buff);
              }
              ObDataBuffer out_buffer(thread_buff.get_data(), thread_buff.get_position());
              if (ret == OB_SUCCESS)
              {
                common::ObResultCode result_msg;
                ret = result_msg.deserialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
                if (ret == OB_SUCCESS)
                {
                  ret = result_msg.result_code_;
                  if (ret != OB_SUCCESS)
                  {
                    TBSYS_LOG(INFO, "rpc return error code is %d msg is %s", result_msg.result_code_, result_msg.message_.ptr());
                  }
                }
              }
            }
            else
            {
              TBSYS_LOG(ERROR, "can not get thread buffer");
              ret = OB_ERROR;
            }
            return ret;
          }


        static void test_migrate_tablet()
          {
            int ret = OB_SUCCESS;
            bool keep_src = false;
            const char * ip_addr = "localhost";
            ObServer dest_server;
            dest_server.set_ipv4_addr(ip_addr,MOCK_SERVER_LISTEN_PORT);
            //ret = cs_migrate(ranges[0], dest_server,dest_server, keep_src);
            ret = chunk_server.get_tablet_manager().migrate_tablet(ranges[0], dest_server, path, num_file, keep_src);
            //ret = tablet_mgr.migrate_tablet(ranges[0], dest_server, path, num_file, keep_src);
            ASSERT_EQ(OB_SUCCESS, ret);
          }

        static void test_load_tablet()
          {
            int ret = OB_SUCCESS;
            ret = chunk_server.get_tablet_manager().dest_load_tablet(ranges[0], path, num_file);
            // ret = tablet_mgr.dest_load_tablet(ranges[0], path, num_file);
            ASSERT_EQ(OB_SUCCESS, ret);
          }

        static void init_cell_infos()
          {
            //malloc
            cell_infos = new ObCellInfo*[ROW_NUM + NON_EXISTENT_ROW_NUM];
            for (int64_t i = 0; i < ROW_NUM + NON_EXISTENT_ROW_NUM; ++i)
            {
              cell_infos[i] = new ObCellInfo[COL_NUM];
            }

            for (int64_t i = 0; i < ROW_NUM + NON_EXISTENT_ROW_NUM; ++i)
            {
              for (int64_t j = 0; j < COL_NUM; ++j)
              {
                row_key_strs[i][j] = new char[50];
              }
            }

            // init cell infos
            for (int64_t i = 0; i < ROW_NUM + NON_EXISTENT_ROW_NUM; ++i)
            {
              for (int64_t j = 0; j < COL_NUM; ++j)
              {
                cell_infos[i][j].table_id_ = table_id;
                sprintf(row_key_strs[i][j], "row_key_%08ld", i);
                cell_infos[i][j].row_key_.assign(row_key_strs[i][j], strlen(row_key_strs[i][j]));
                //cell_infos[i][j].op_info_.set_update();
                cell_infos[i][j].column_id_ = j + 2;
                cell_infos[i][j].value_.set_int(1000 + i * COL_NUM + j);
              }
            }
          }

        static void destroy_cell_infos()
          {
            for (int64_t i = 0; i < ROW_NUM + NON_EXISTENT_ROW_NUM; ++i)
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

            for (int64_t i = 0; i < ROW_NUM + NON_EXISTENT_ROW_NUM; ++i)
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

        static int init_sstable(const ObCellInfo** cell_infos, const int64_t row_num,
                                const int64_t col_num, const int64_t sst_id = 0L)
          {
            int err = OB_SUCCESS;

            ObSSTableSchema sstable_schema;
            ObSSTableSchemaColumnDef column_def;

            EXPECT_TRUE(NULL != cell_infos);
            EXPECT_TRUE(row_num > 0);
            EXPECT_TRUE(col_num > 0);

            for (int64_t i = 0; i < col_num; ++i)
            {
              column_def.column_name_id_ = cell_infos[0][i].column_id_;
              column_def.column_value_type_ = cell_infos[0][i].value_.get_type();
              sstable_schema.add_column_def(column_def);
            }

            uint64_t table_id = cell_infos[0][0].table_id_;
            ObString path;
            int64_t sstable_file_id = 0;
            ObString compress_name;
            char* path_str = sstable_file_path;
            int64_t path_len = OB_MAX_FILE_NAME_LENGTH;

            if (0 == sst_id)
            {
              sstable_file_id = 100;
            }
            else
            {
              sstable_file_id = sst_id;
            }

            ObSSTableId sstable_id(sst_id);
            get_sstable_path(sstable_id, path_str, path_len);
            char cmd[256];
            sprintf(cmd, "mkdir -p %s", path_str);
            system(cmd);
            path.assign((char*)path_str, strlen(path_str));
            compress_name.assign("lzo_1.0", strlen("lzo_1.0"));
            remove(path.ptr());

            ObSSTableWriter writer;
            err = writer.create_sstable(sstable_schema, path, compress_name, table_id);
            EXPECT_EQ(OB_SUCCESS, err);

            for (int64_t i = 0; i < row_num; ++i)
            {
              ObSSTableRow row;
              err = row.set_row_key(cell_infos[i][0].row_key_);
              EXPECT_EQ(OB_SUCCESS, err);
              for (int64_t j = 0; j < col_num; ++j)
              {
                err = row.add_obj(cell_infos[i][j].value_);
                EXPECT_EQ(OB_SUCCESS, err);
              }

              int64_t space_usage = 0;
              err = writer.append_row(row, space_usage);
              EXPECT_EQ(OB_SUCCESS, err);
            }

            int64_t offset = 0;
            err = writer.close_sstable(offset);
            EXPECT_EQ(OB_SUCCESS, err);

            return err;
          }

        static int create_tablet(const ObRange& range, const ObSSTableId& sst_id)
          {
            int err = OB_SUCCESS;

            if (range.empty())
            {
              TBSYS_LOG(ERROR, "create_tablet error, input range is empty.");
              err = OB_INVALID_ARGUMENT;
            }

            if (OB_SUCCESS == err)
            {
              ObTablet* tablet = NULL;
              // find tablet if exist?
              //ObTabletImage &image = tablet_mgr.get_serving_tablet_image();
              ObTabletImage &image = chunk_server.get_tablet_manager().get_serving_tablet_image();
              err = image.acquire_tablet(range, tablet);
              if (OB_SUCCESS == err)
              {
                TBSYS_LOG(ERROR, "tablet already exists! dump input and exist:");
                range.hex_dump(TBSYS_LOG_LEVEL_ERROR);
                tablet->get_range().hex_dump(TBSYS_LOG_LEVEL_ERROR);
                err = OB_ERROR;
                tablet = NULL;
                image.release_tablet(tablet);
              }
              else
              {
                err = image.alloc_tablet_object(range, tablet);
                if (OB_SUCCESS == err)
                {
                  err = tablet->add_sstable_by_id(sst_id);
                  EXPECT_EQ(OB_SUCCESS, err);
                }
                if (OB_SUCCESS == err)
                {
                  err = image.add_tablet(tablet, true);
                  EXPECT_EQ(OB_SUCCESS, err);
                }
              }
            }

            return err;
          }

        static int create_and_load_tablets()
          {
            int ret = OB_SUCCESS;

            for (int64_t i = 0; i < SSTABLE_NUM; ++i)
            {
              sst_ids[i].sstable_file_id_ = i % DISK_NUM + 256 * (i / DISK_NUM + 1);
              sst_ids[i].sstable_file_offset_ = 0;

              ret = init_sstable((const ObCellInfo**)(&cell_infos[i * SSTABLE_ROW_NUM]),
                                 SSTABLE_ROW_NUM, COL_NUM, sst_ids[i].sstable_file_id_);
              EXPECT_EQ(OB_SUCCESS, ret);
            }

            // init range
            for (int64_t i = 0; i < SSTABLE_NUM; ++i)
            {
              ranges[i].table_id_ = table_id;
              ranges[i].end_key_ = cell_infos[(i + 1) * SSTABLE_ROW_NUM - 1][0].row_key_;
              ranges[i].border_flag_.set_inclusive_end();
              ranges[i].border_flag_.unset_inclusive_start();

              if (0 == i)
              {
                ranges[i].start_key_.set_min_row();
              }
              else
              {
                ranges[i].start_key_ = cell_infos[i * SSTABLE_ROW_NUM - 1][0].row_key_;
              }

              ret = create_tablet(ranges[i], sst_ids[i]);
              EXPECT_EQ(OB_SUCCESS, ret);
              if (OB_SUCCESS != ret)
              {
                ret = OB_ERROR;
                break;
              }
            }

            return ret;
          }

        static int init_mgr()
          {
            int err = OB_SUCCESS;


            chunk_server.initialize();

            ObBlockCacheConf conf;
            conf.dicache_max_num = 1000;
            conf.dicache_min_timeo = 1000 * 1000;
            conf.mbcache_max_num = 1024;
            conf.mbcache_min_timeo = 1000 * 1000;
            conf.memalign_size = 512;
            conf.memblock_size = 1024 * 1024L;
            conf.ficache_max_num = 1024;
            conf.ficache_min_timeo = 100 * 1000 * 1000;

            ObBlockIndexCacheConf bic_conf;
            bic_conf.cache_mem_size = 128 * 1024 * 1024;
            bic_conf.max_no_active_usec = -1;
            bic_conf.hash_slot_num = 1024;

            err = chunk_server.get_tablet_manager().init(conf, bic_conf);
            EXPECT_EQ(OB_SUCCESS, err);

            chunk_server.start_service();
            err = create_and_load_tablets();
            EXPECT_EQ(OB_SUCCESS, err);

            return err;
          }

      public:
        static void SetUpTestCase()
          {
            int err = OB_SUCCESS;

            TBSYS_LOGGER.setLogLevel("ERROR");
            err = ob_init_memory_pool();
            ASSERT_EQ(OB_SUCCESS, err);

            init_cell_infos();

            err = init_mgr();
            ASSERT_EQ(OB_SUCCESS, err);
          }

        static void TearDownTestCase()
          {
            //destroy_cell_infos();
          }

        virtual void SetUp()
          {

          }

        virtual void TearDown()
          {

          }
      };

      TEST_F(TestObTabletMigrate, test_migrate_tablet)
      {
        test_migrate_tablet();
      }

      TEST_F(TestObTabletMigrate, test_load_tablet)
      {
        test_load_tablet();
      }

    }//end namespace common
  }//end namespace tests
}//end namespace oceanbase

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
