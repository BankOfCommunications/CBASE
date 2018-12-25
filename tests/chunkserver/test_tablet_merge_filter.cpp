/**
 * (C) 2010-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * test_tablet_manager_get.cpp for test get interface of tablet
 * manager, it also test the switch cache feature.
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */

#include <tblog.h>
#include <gtest/gtest.h>
#include "common/ob_schema.h"
#include "ob_tablet_merge_filter.h"
#include "chunkserver/ob_tablet_image.h"

using namespace oceanbase::common;
using namespace oceanbase::chunkserver;
using namespace oceanbase::sstable;

namespace oceanbase
{
  namespace tests
  {
    namespace chunkserver
    {
      static ObTabletImage g_image;

      const char* SCHEMA_WITHOUT_EXPIRE = "./schema/schema_without_expire.ini";
      const char* SCHEMA_WITH_INVALID_EXPIRE = "./schema/schema_with_invalid_expire.ini";
      const char* SCHEMA_WITH_VALID_EXPIRE = "./schema/schema_with_valid_expire.ini";
      const char* SCHEMA_WITH_EXPIRE = "./schema/schema_with_expire.ini";

      static const int64_t LAST_DO_EXPIRE_VERSION = 10;
      static const int64_t FROZEN_VERSION = 11;
      static const int64_t COLUMN_COUNT = 8;

      class TestTabletMergerFilter : public ::testing::Test
      {
      public:
        static void SetUpTestCase()
        {
        }

        static void TearDownTestCase()
        {
        }

        virtual void SetUp()
        {
        }

        virtual void TearDown()
        {
        }

        void load_schema(ObSchemaManagerV2& schema, const char* schema_file)
        {
          tbsys::CConfig c1;
          ASSERT_EQ(true, schema.parse_from_file(schema_file,c1));
        }

        void init_tablet(ObTablet& tablet)
        {
          ObNewRange range;
          range.table_id_ = 1001;
          tablet.set_range(range);
          tablet.set_last_do_expire_version(LAST_DO_EXPIRE_VERSION);
        }

        void init_sstable_row(ObSSTableRow& row)
        {
          int ret = OB_SUCCESS;
          ObObj obj;
          ObString str;

          row.clear();
          str.assign_ptr((char*)"info_user_nick", (int32_t)strlen("info_user_nick"));
          obj.set_varchar(str);
          ret = row.add_obj(obj);
          ASSERT_TRUE(OB_SUCCESS == ret);

          obj.set_int(1);
          ret = row.add_obj(obj);
          ASSERT_TRUE(OB_SUCCESS == ret);

          obj.set_datetime(tbsys::CTimeUtil::getTime());
          ret = row.add_obj(obj);
          ASSERT_TRUE(OB_SUCCESS == ret);

          obj.set_int(1);
          ret = row.add_obj(obj);
          ASSERT_TRUE(OB_SUCCESS == ret);

          obj.set_createtime(tbsys::CTimeUtil::getTime());
          ret = row.add_obj(obj);
          ASSERT_TRUE(OB_SUCCESS == ret);

          obj.set_modifytime(tbsys::CTimeUtil::getTime());
          ret = row.add_obj(obj);
          ASSERT_TRUE(OB_SUCCESS == ret);

          str.assign_ptr((char*)"info_tag", (int32_t)strlen("info_tag"));
          obj.set_varchar(str);
          ret = row.add_obj(obj);
          ASSERT_TRUE(OB_SUCCESS == ret);

          obj.set_int(1);
          ret = row.add_obj(obj);
          ASSERT_TRUE(OB_SUCCESS == ret);

          ASSERT_EQ(COLUMN_COUNT, row.get_obj_count());
        }

        void add_extra_basic_obj_result(ObSSTableRow& row)
        {
          ObObj obj;

          obj.set_int(1);
          row.add_obj(obj);
        }

        void add_composite_result(ObSSTableRow& row, bool result)
        {
          ObObj obj;

          obj.set_bool(result);
          row.add_obj(obj);
        }

        void init_scan_param(ObScanParam& scan_parma)
        {
          int ret = OB_SUCCESS;

          scan_parma.reset();
          for (int64_t i = 2; i < 9; i++)
          {
            ret = scan_parma.add_column(i);
            ASSERT_TRUE(OB_SUCCESS == ret);
          }
          ret = scan_parma.add_column(27);
          ASSERT_TRUE(OB_SUCCESS == ret);
        }
      };

      TEST_F(TestTabletMergerFilter, test_init_fail)
      {
        int ret = OB_SUCCESS;
        ObTablet tablet(&g_image);
        ObSchemaManagerV2 schema;
        ObTabletMergerFilter merge_filter;

        ret = merge_filter.init(schema, 1, NULL, 0, 0);
        ASSERT_TRUE(OB_SUCCESS != ret);
        ASSERT_FALSE(merge_filter.need_filter());
        ret = merge_filter.init(schema, 1, NULL, 1, 0);
        ASSERT_TRUE(OB_SUCCESS != ret);
        ASSERT_FALSE(merge_filter.need_filter());
        ret = merge_filter.init(schema, 1, NULL, 1, 0);
        ASSERT_TRUE(OB_SUCCESS != ret);
        ASSERT_FALSE(merge_filter.need_filter());
        ret = merge_filter.init(schema, 1, NULL, -1, 0);
        ASSERT_TRUE(OB_SUCCESS != ret);
        ASSERT_FALSE(merge_filter.need_filter());

        ret = merge_filter.init(schema, 1, &tablet, 0, 0);
        ASSERT_TRUE(OB_SUCCESS != ret);
        ASSERT_FALSE(merge_filter.need_filter());
        ret = merge_filter.init(schema, 1, &tablet, 1, 0);
        ASSERT_TRUE(OB_SUCCESS != ret);
        ASSERT_FALSE(merge_filter.need_filter());
        ret = merge_filter.init(schema, 1, &tablet, 1, 0);
        ASSERT_TRUE(OB_SUCCESS != ret);
        ASSERT_FALSE(merge_filter.need_filter());
        ret = merge_filter.init(schema, 1, &tablet, -1, 0);
        ASSERT_TRUE(OB_SUCCESS != ret);
        ASSERT_FALSE(merge_filter.need_filter());

        ret = merge_filter.init(schema, 1, NULL, 0, 0);
        ASSERT_TRUE(OB_SUCCESS != ret);
        ASSERT_FALSE(merge_filter.need_filter());
        ret = merge_filter.init(schema, 1, NULL, 1, -1);
        ASSERT_TRUE(OB_SUCCESS != ret);
        ASSERT_FALSE(merge_filter.need_filter());
        ret = merge_filter.init(schema, 1, NULL, 1, 10);
        ASSERT_TRUE(OB_SUCCESS != ret);
        ASSERT_FALSE(merge_filter.need_filter());
        ret = merge_filter.init(schema, 1, NULL, -1, 100);
        ASSERT_TRUE(OB_SUCCESS != ret);
        ASSERT_FALSE(merge_filter.need_filter());

        ret = merge_filter.init(schema, 1, &tablet, 0, 0);
        ASSERT_TRUE(OB_SUCCESS != ret);
        ASSERT_FALSE(merge_filter.need_filter());
        ret = merge_filter.init(schema, 1, &tablet, 1, -1);
        ASSERT_TRUE(OB_SUCCESS != ret);
        ASSERT_FALSE(merge_filter.need_filter());
        ret = merge_filter.init(schema, 1, &tablet, 1, 10);
        ASSERT_TRUE(OB_SUCCESS != ret);
        ASSERT_FALSE(merge_filter.need_filter());
        ret = merge_filter.init(schema, 1, &tablet, -1, 100);
        ASSERT_TRUE(OB_SUCCESS != ret);
        ASSERT_FALSE(merge_filter.need_filter());
      }

      TEST_F(TestTabletMergerFilter, test_without_expire)
      {
        int ret = OB_SUCCESS;
        ObTablet tablet(&g_image);
        ObSchemaManagerV2 schema;
        ObTabletMergerFilter merge_filter;

        init_tablet(tablet);
        load_schema(schema, SCHEMA_WITHOUT_EXPIRE);
        ret = merge_filter.init(schema, 1, &tablet, FROZEN_VERSION,
          tbsys::CTimeUtil::getTime());
        ASSERT_TRUE(OB_SUCCESS == ret);
        ASSERT_FALSE(merge_filter.need_filter());

        merge_filter.reset();
        tablet.set_last_do_expire_version(0);
        ret = merge_filter.init(schema, 1, &tablet, FROZEN_VERSION,
          tbsys::CTimeUtil::getTime());
        ASSERT_TRUE(OB_SUCCESS == ret);
        ASSERT_FALSE(merge_filter.need_filter());
      }

      TEST_F(TestTabletMergerFilter, test_with_invalid_expire)
      {
        int ret = OB_SUCCESS;
        ObTablet tablet(&g_image);
        ObSchemaManagerV2 schema;
        ObTabletMergerFilter merge_filter;
        ObScanParam scan_param;
        ObSSTableRow row;
        bool is_expired = false;

        init_tablet(tablet);
        tbsys::CConfig c1;
        ASSERT_FALSE(schema.parse_from_file(SCHEMA_WITH_INVALID_EXPIRE,c1));
        return;

        load_schema(schema, SCHEMA_WITH_INVALID_EXPIRE);
        ret = merge_filter.init(schema, 1, &tablet, FROZEN_VERSION,
          tbsys::CTimeUtil::getTime());
        ASSERT_TRUE(OB_SUCCESS == ret);
        ASSERT_TRUE(merge_filter.need_filter());
        ret = merge_filter.adjust_scan_param(0, 0, scan_param);
        ASSERT_TRUE(OB_SUCCESS != ret);
        ASSERT_EQ(0, scan_param.get_column_id_size());
        ASSERT_EQ(0, scan_param.get_composite_columns_size());
        for (int64_t i = 0; i < 10; ++i)
        {
          ret = merge_filter.check_and_trim_row(0, i, row, is_expired);
          ASSERT_TRUE(OB_SUCCESS != ret);
          ASSERT_FALSE(is_expired);
        }
        init_scan_param(scan_param);
        ASSERT_EQ(COLUMN_COUNT, scan_param.get_column_id_size());
        ret = merge_filter.adjust_scan_param(0, 0, scan_param);
        ASSERT_TRUE(OB_SUCCESS != ret);
        ASSERT_EQ(COLUMN_COUNT, scan_param.get_column_id_size());
        ASSERT_EQ(0, scan_param.get_composite_columns_size());
        init_sstable_row(row);
        for (int64_t i = 0; i < 10; ++i)
        {
          ret = merge_filter.check_and_trim_row(0, i, row, is_expired);
          ASSERT_TRUE(OB_SUCCESS == ret);
          ASSERT_FALSE(is_expired);
        }
        ASSERT_EQ(COLUMN_COUNT, row.get_obj_count());

        merge_filter.reset();
        tablet.set_last_do_expire_version(0);
        ret = merge_filter.init(schema, 1, &tablet, FROZEN_VERSION,
          tbsys::CTimeUtil::getTime() + 10 * 1000 * 1000);
        ASSERT_TRUE(OB_SUCCESS == ret);
        ASSERT_TRUE(merge_filter.need_filter());
        ret = merge_filter.adjust_scan_param(0, 0, scan_param);
        ASSERT_TRUE(OB_SUCCESS != ret);
        ASSERT_EQ(COLUMN_COUNT, scan_param.get_column_id_size());
        ASSERT_EQ(0, scan_param.get_composite_columns_size());
        row.clear();
        scan_param.reset();
        for (int64_t i = 0; i < 10; ++i)
        {
          ret = merge_filter.check_and_trim_row(0, i, row, is_expired);
          ASSERT_TRUE(OB_SUCCESS != ret);
          ASSERT_FALSE(is_expired);
        }
        init_scan_param(scan_param);
        ASSERT_EQ(COLUMN_COUNT, scan_param.get_column_id_size());
        ret = merge_filter.adjust_scan_param(0, 0, scan_param);
        ASSERT_TRUE(OB_SUCCESS != ret);
        ASSERT_EQ(COLUMN_COUNT, scan_param.get_column_id_size());
        ASSERT_EQ(0, scan_param.get_composite_columns_size());
        init_sstable_row(row);
        for (int64_t i = 0; i < 10; ++i)
        {
          ret = merge_filter.check_and_trim_row(0, i, row, is_expired);
          ASSERT_TRUE(OB_SUCCESS == ret);
          ASSERT_FALSE(is_expired);
        }
        ASSERT_EQ(COLUMN_COUNT, row.get_obj_count());
      }

      TEST_F(TestTabletMergerFilter, test_with_valid_expire)
      {
        int ret = OB_SUCCESS;
        ObTablet tablet(&g_image);
        ObSchemaManagerV2 schema;
        ObTabletMergerFilter merge_filter;
        ObScanParam scan_param;
        ObSSTableRow row;
        bool is_expired = false;
        bool comp_result = false;

        init_tablet(tablet);
        load_schema(schema, SCHEMA_WITH_VALID_EXPIRE);
        ret = merge_filter.init(schema, 1, &tablet, FROZEN_VERSION,
          tbsys::CTimeUtil::getTime());
        ASSERT_TRUE(OB_SUCCESS == ret);
        ASSERT_FALSE(merge_filter.need_filter());
        init_scan_param(scan_param);
        ASSERT_EQ(COLUMN_COUNT, scan_param.get_column_id_size());
        ret = merge_filter.adjust_scan_param(0, 0, scan_param);
        ASSERT_TRUE(OB_SUCCESS == ret);
        ASSERT_EQ(COLUMN_COUNT, scan_param.get_column_id_size());
        ASSERT_EQ(0, scan_param.get_composite_columns_size());
        init_sstable_row(row);
        for (int64_t i = 0; i < 10; ++i)
        {
          ret = merge_filter.check_and_trim_row(0, i, row, is_expired);
          ASSERT_TRUE(OB_SUCCESS == ret);
          ASSERT_FALSE(is_expired);
        }
        ASSERT_EQ(COLUMN_COUNT, row.get_obj_count());

        merge_filter.reset();
        tablet.set_last_do_expire_version(LAST_DO_EXPIRE_VERSION - 1);
        ret = merge_filter.init(schema, 1, &tablet, FROZEN_VERSION,
          tbsys::CTimeUtil::getTime() + 10 * 1000 * 1000);
        ASSERT_TRUE(OB_SUCCESS == ret);
        ASSERT_TRUE(merge_filter.need_filter());
        init_scan_param(scan_param);
        ASSERT_EQ(COLUMN_COUNT, scan_param.get_column_id_size());
        ret = merge_filter.adjust_scan_param(0, 0, scan_param);
        ASSERT_TRUE(OB_SUCCESS == ret);
        ASSERT_EQ(COLUMN_COUNT, scan_param.get_column_id_size());
        ASSERT_EQ(1, scan_param.get_composite_columns_size());
        init_sstable_row(row);
        add_composite_result(row, comp_result);
        for (int64_t i = 0; i < 10; ++i)
        {
          ret = merge_filter.check_and_trim_row(0, i, row, is_expired);
          ASSERT_TRUE(OB_SUCCESS == ret);
          if (comp_result)
          {
            ASSERT_TRUE(is_expired);
          }
          else
          {
            ASSERT_FALSE(is_expired);
          }
          ASSERT_EQ(COLUMN_COUNT, row.get_obj_count());
          comp_result = !comp_result;
          add_composite_result(row, comp_result);
          ASSERT_EQ(COLUMN_COUNT + 1, row.get_obj_count());
        }
      }

      TEST_F(TestTabletMergerFilter, test_mult_group_with_valid_expire)
      {
        int ret = OB_SUCCESS;
        ObTablet tablet(&g_image);
        ObSchemaManagerV2 schema;
        ObTabletMergerFilter merge_filter;
        ObScanParam scan_param;
        ObSSTableRow row;
        bool is_expired = false;
        bool comp_result = false;

        init_tablet(tablet);
        load_schema(schema, SCHEMA_WITH_EXPIRE);
        ret = merge_filter.init(schema, 4, &tablet, FROZEN_VERSION,
          tbsys::CTimeUtil::getTime());
        ASSERT_TRUE(OB_SUCCESS == ret);
        ASSERT_FALSE(merge_filter.need_filter());
        init_scan_param(scan_param);
        ASSERT_EQ(COLUMN_COUNT, scan_param.get_column_id_size());
        ret = merge_filter.adjust_scan_param(0, 0, scan_param);
        ASSERT_TRUE(OB_SUCCESS == ret);
        ASSERT_EQ(COLUMN_COUNT, scan_param.get_column_id_size());
        ASSERT_EQ(0, scan_param.get_composite_columns_size());
        init_sstable_row(row);
        for (int64_t j = 0; j < 4; ++j)
        {
          for (int64_t i = 0; i < 10; ++i)
          {
            ret = merge_filter.check_and_trim_row(j, i, row, is_expired);
            ASSERT_TRUE(OB_SUCCESS == ret) << "i:" << i << ",j:" << j;
            ASSERT_FALSE(is_expired);
          }
        }
        ASSERT_EQ(COLUMN_COUNT, row.get_obj_count());

        merge_filter.reset();
        tablet.set_last_do_expire_version(LAST_DO_EXPIRE_VERSION - 1);
        ret = merge_filter.init(schema, 4, &tablet, FROZEN_VERSION,
          tbsys::CTimeUtil::getTime() + 10 * 1000 * 1000);
        ASSERT_TRUE(OB_SUCCESS == ret);
        ASSERT_TRUE(merge_filter.need_filter());
        init_sstable_row(row);
        add_extra_basic_obj_result(row);
        add_composite_result(row, comp_result);
        for (int64_t j = 0; j < 4; ++j)
        {
          init_scan_param(scan_param);
          ASSERT_EQ(COLUMN_COUNT, scan_param.get_column_id_size());
          ret = merge_filter.adjust_scan_param(j, j, scan_param);
          ASSERT_TRUE(OB_SUCCESS == ret) << "j:" << j;
          if (j > 0)
          {
            ASSERT_EQ(COLUMN_COUNT, scan_param.get_column_id_size());
            ASSERT_EQ(0, scan_param.get_composite_columns_size());
          }
          else
          {
            ASSERT_EQ(COLUMN_COUNT + 1, scan_param.get_column_id_size());
            ASSERT_EQ(1, scan_param.get_composite_columns_size());
          }
          for (int64_t i = 0; i < 10; ++i)
          {
            ret = merge_filter.check_and_trim_row(j, i, row, is_expired);
            ASSERT_TRUE(OB_SUCCESS == ret);
            if (comp_result)
            {
              ASSERT_TRUE(is_expired);
            }
            else
            {
              ASSERT_FALSE(is_expired);
            }
            comp_result = !comp_result;
            if (0 == j)
            {
              ASSERT_EQ(COLUMN_COUNT, row.get_obj_count());
              add_extra_basic_obj_result(row);
              add_composite_result(row, comp_result);
              ASSERT_EQ(COLUMN_COUNT + 2, row.get_obj_count());
            }
          }
        }
        usleep(1000000);
      }
    }//end namespace chunkserver
  }//end namespace tests
}//end namespace oceanbase

int main(int argc, char** argv)
{
  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("ERROR");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
