#include <gtest/gtest.h>
#include "ob_malloc.h"
#include "ob_define.h"
#include "ob_tablet.h"
#include "ob_fileinfo_cache.h"
#include "ob_tablet_image.h"
#include "page_arena.h"
#include "test_helper.h"
#include "../common/test_rowkey_helper.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::chunkserver;
using namespace oceanbase::sstable;

static const int64_t VERSION_1 = 1;
static const int64_t VERSION_2 = 2;
static const int64_t VERSION_3 = 3;

const int64_t DISK_NUM = 3;


void create_range(
    CharArena& allocator,
    ObNewRange &range,
    uint64_t table_id,
    const int8_t flag,
    const char* sk,
    const char* ek)
{
  range.table_id_ = table_id;
  range.border_flag_.set_data(flag);
  range.start_key_ = make_rowkey(sk, &allocator);
  range.end_key_ = make_rowkey(ek, &allocator);
  if (range.border_flag_.is_min_value())
  {
    range.start_key_.set_min_row();
  }
  if (range.border_flag_.is_max_value())
  {
    range.end_key_.set_max_row();
  }

}

int write_all(ObMultiVersionTabletImage &image)
{
  int ret = OB_SUCCESS;
  for (int v = VERSION_1; v <= VERSION_2; ++v)
  {
    for (int d = 1; d <= 3; ++d)
    {
      ret = image.write(v, d);
      if (ret) return ret;
    }
  }
  return ret;
}

int read_all(ObMultiVersionTabletImage &image)
{
  int ret = OB_SUCCESS;
  const int32_t  disk_no_array[] = { 1, 2, 3 };
  ret = image.load_tablets(disk_no_array, 3, false);
  /*
  for (int v = VERSION_1; v <= VERSION_2; ++v)
  {
    for (int d = 1; d <= 3; ++d)
    {
      ret = image.read(v, d, false);
      if (ret) return ret;
    }
  }
  //image.initialize_service_index();
  */
  return ret;
}

TEST(ObMultiVersionTabletImage, test_write)
{
  FileInfoCache fic;
  ObMultiVersionTabletImage image(fic);

  fic.init(100);
  CharArena allocator;
  ObNewRange r1,r2,r3;
  create_range(allocator, r1, 1, ObBorderFlag::INCLUSIVE_START|ObBorderFlag::INCLUSIVE_END, "aoo", "foo");
  create_range(allocator, r2, 1, ObBorderFlag::INCLUSIVE_END, "foo", "mj");
  create_range(allocator, r3, 1, ObBorderFlag::INCLUSIVE_END, "mj", "oi");

  int ret = 0;
  ObTablet* tablet = NULL;
  ObSSTableId id;
  id.sstable_file_id_ = 1;
  id.sstable_file_offset_ = 0;

  ret = image.alloc_tablet_object(r1, VERSION_1, tablet);
  ASSERT_EQ(0, ret);
  tablet->set_disk_no(1);
  //tablet->set_merged(1);
  ret = tablet->add_sstable_by_id(id);
  ASSERT_EQ(0, ret);
  ret = image.add_tablet(tablet, false);
  ASSERT_EQ(0, ret);

  ret = image.alloc_tablet_object(r1, VERSION_2, tablet);
  ASSERT_EQ(0, ret);
  tablet->set_disk_no(1);
  ret = tablet->add_sstable_by_id(id);
  ASSERT_EQ(0, ret);
  ret = image.add_tablet(tablet, false);
  ASSERT_EQ(0, ret);

  ret = image.alloc_tablet_object(r2, VERSION_2, tablet);
  ASSERT_EQ(0, ret);
  id.sstable_file_id_ = 2;
  id.sstable_file_offset_ = 0;
  tablet->set_disk_no(2);
  tablet->add_sstable_by_id(id);
  ret = image.add_tablet(tablet, false);
  ASSERT_EQ(0, ret);

  ret = image.alloc_tablet_object(r3, VERSION_2, tablet);
  ASSERT_EQ(0, ret);
  id.sstable_file_id_ = 3;
  id.sstable_file_offset_ = 0;
  tablet->set_disk_no(3);
  tablet->add_sstable_by_id(id);
  ret = image.add_tablet(tablet, false);
  ASSERT_EQ(0, ret);

  ret = write_all(image);
  ASSERT_EQ(0, ret);
  fic.destroy();
}

TEST(ObMultiVersionTabletImage, test_query)
{
  FileInfoCache fic;
  ObMultiVersionTabletImage image(fic);
  fic.init(100);

  CharArena allocator;
  ObNewRange r1,r2,r3;
  create_range(allocator, r1, 1, ObBorderFlag::INCLUSIVE_START|ObBorderFlag::INCLUSIVE_END, "aoo", "foo");
  create_range(allocator, r2, 1, ObBorderFlag::INCLUSIVE_END, "foo", "mj");
  create_range(allocator, r3, 1, ObBorderFlag::INCLUSIVE_END, "mj", "oi");

  int ret = read_all(image);
  ASSERT_EQ(0, ret);


  ObTablet *tablet = NULL;
  ret = image.acquire_tablet(r2, ObMultiVersionTabletImage::SCAN_FORWARD, 0, tablet);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(true, tablet->get_range().equal(r2));
  ret = image.release_tablet(tablet);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(VERSION_2, tablet->get_data_version());

  ret = image.acquire_tablet(r1, ObMultiVersionTabletImage::SCAN_FORWARD, 0, tablet);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(true, tablet->get_range().equal(r1));
  ret = image.release_tablet(tablet);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(VERSION_1, tablet->get_data_version());

  ret = image.acquire_tablet(r1, ObMultiVersionTabletImage::SCAN_FORWARD, VERSION_1, tablet);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(true, tablet->get_range().equal(r1));
  ret = image.release_tablet(tablet);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(VERSION_1, tablet->get_data_version());

  ObNewRange query_whole_range;
  query_whole_range.table_id_ = 1;
  query_whole_range.set_whole_range();
  ret = image.acquire_tablet(query_whole_range,  ObMultiVersionTabletImage::SCAN_FORWARD, VERSION_1, tablet);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(true, tablet->get_range().equal(r1));
  ret = image.release_tablet(tablet);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(VERSION_1, tablet->get_data_version());

  ret = image.acquire_tablet_all_version(query_whole_range, ObMultiVersionTabletImage::SCAN_BACKWARD,
      ObMultiVersionTabletImage::FROM_NEWEST_INDEX, VERSION_2, tablet);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(true, tablet->get_range().equal(r3));
  ret = image.release_tablet(tablet);
  ASSERT_EQ(0, ret);

  ret = image.acquire_tablet(query_whole_range, ObMultiVersionTabletImage::SCAN_BACKWARD, VERSION_1, tablet);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(true, tablet->get_range().equal(r1));
  ret = image.release_tablet(tablet);
  ASSERT_EQ(0, ret);

  ObNewRange r4;
  create_range(allocator, r4, 1, ObBorderFlag::INCLUSIVE_START|ObBorderFlag::INCLUSIVE_END, "foo", "koo");
  ret = image.acquire_tablet_all_version(r4, ObMultiVersionTabletImage::SCAN_FORWARD,
      ObMultiVersionTabletImage::FROM_NEWEST_INDEX, 0, tablet);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(true, tablet->get_range().equal(r1));
  ASSERT_EQ(VERSION_2, tablet->get_data_version());
  ret = image.release_tablet(tablet);
  ASSERT_EQ(0, ret);

  create_range(allocator, r4, 1, ObBorderFlag::INCLUSIVE_START|ObBorderFlag::INCLUSIVE_END, "foz", "noo");
  ret = image.acquire_tablet(r4,  ObMultiVersionTabletImage::SCAN_FORWARD,0, tablet);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(true, tablet->get_range().equal(r2));
  ASSERT_EQ(VERSION_2, tablet->get_data_version());
  ret = image.release_tablet(tablet);
  ASSERT_EQ(0, ret);

  fic.destroy();
}

TEST(ObMultiVersionTabletImage, test_remove)
{
  FileInfoCache fic;
  ObMultiVersionTabletImage image(fic);
  fic.init(100);

  CharArena allocator;
  ObNewRange r1,r2,r3;
  create_range(allocator, r1, 1, ObBorderFlag::INCLUSIVE_START|ObBorderFlag::INCLUSIVE_END, "aoo", "foo");
  create_range(allocator, r2, 1, ObBorderFlag::INCLUSIVE_END, "foo", "mj");
  create_range(allocator, r3, 1, ObBorderFlag::INCLUSIVE_END, "mj", "oi");

  int ret = read_all(image);
  ASSERT_EQ(0, ret);

  int disk_no = 0;
  ObTablet *tablet= NULL;

  ret = image.remove_tablet(r1, VERSION_2, disk_no, false);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(1, disk_no);

  ret = image.acquire_tablet(r1, ObMultiVersionTabletImage::SCAN_FORWARD, VERSION_2, tablet);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(true, tablet->get_range().equal(r1));
  ASSERT_EQ(VERSION_1, tablet->get_data_version());
  ret = image.release_tablet(tablet);
  ASSERT_EQ(0, ret);

  fic.destroy();
}

TEST(ObMultiVersionTabletImage, test_upgrade)
{
  FileInfoCache fic;
  ObMultiVersionTabletImage image(fic);
  fic.init(100);

  CharArena allocator;
  ObNewRange r1,r2,r3;
  create_range(allocator, r1, 1, ObBorderFlag::INCLUSIVE_START|ObBorderFlag::INCLUSIVE_END, "aoo", "foo");
  create_range(allocator, r2, 1, ObBorderFlag::INCLUSIVE_END, "foo", "mj");
  create_range(allocator, r3, 1, ObBorderFlag::INCLUSIVE_END, "mj", "oi");

  int ret = read_all(image);
  ASSERT_EQ(0, ret);

  int disk_no = 0;
  ObTablet *tablet= NULL;

  ret = image.remove_tablet(r1, VERSION_2, disk_no, false);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(1, disk_no);

  ret = image.acquire_tablet(r1, ObMultiVersionTabletImage::SCAN_FORWARD, VERSION_1, tablet);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(true, tablet->get_range().equal(r1));
  ASSERT_EQ(VERSION_1, tablet->get_data_version());
  ret = image.release_tablet(tablet);
  ASSERT_EQ(0, ret);

  // get tablet version1 r1 for merge
  int64_t num = 2;
  ObTablet *tablets[num];
  ret = image.get_tablets_for_merge(VERSION_2, num, tablets);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(1, num);
  tablet = tablets[0];
  ASSERT_EQ(VERSION_1, tablet->get_data_version());
  ASSERT_EQ(true, tablet->get_range().equal(r1));
  ret = image.release_tablet(tablet);

  // upgrade r1 v1 to r1 v2
  ObTablet *new_tablet = NULL;
  ret = image.alloc_tablet_object(r1, VERSION_2, new_tablet);
  ASSERT_EQ(0, ret);
  new_tablet->set_disk_no(2);
  ObSSTableId id;
  id.sstable_file_id_ = 15 << 8 | 2;
  id.sstable_file_offset_ = 0;
  ret = new_tablet->add_sstable_by_id(id);
  ASSERT_EQ(0, ret);
  ret = image.upgrade_tablet(tablet, new_tablet, false);
  ASSERT_EQ(0, ret);

  ret = image.upgrade_service();
  ASSERT_EQ(0, ret);

  // query r1 v2
  ret = image.acquire_tablet(r1, ObMultiVersionTabletImage::SCAN_FORWARD, 0, tablet);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(true, tablet->get_range().equal(r1));
  ASSERT_EQ(VERSION_2, tablet->get_data_version());
  ret = image.release_tablet(tablet);
  ASSERT_EQ(0, ret);


  // query tablets for merge, should be null
  ret = image.get_tablets_for_merge(VERSION_2, num, tablets);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(0, num);


  // query tablets v2 for merge to v3
  num = 2;
  ret = image.get_tablets_for_merge(VERSION_3, num, tablets);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(2, num);
  tablet = tablets[0];
  ret = image.alloc_tablet_object(tablet->get_range(), VERSION_3, new_tablet);
  ASSERT_EQ(0, ret);
  new_tablet->set_disk_no(10);
  id.sstable_file_id_ = 15 << 8 | 10;
  id.sstable_file_offset_ = 0;
  ret = new_tablet->add_sstable_by_id(id);
  ASSERT_EQ(0, ret);
  ret = image.upgrade_tablet(tablet, new_tablet, false);
  ASSERT_EQ(0, ret);
  for (int i = 0; i < num; ++i)
  {
    image.release_tablet(tablets[i]);
  }

  ret = image.upgrade_service();
  // query v3
  ret = image.acquire_tablet(r1, ObMultiVersionTabletImage::SCAN_FORWARD, 0, tablet);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(VERSION_3, tablet->get_data_version());
  image.release_tablet(tablet);

  fic.destroy();
}

TEST(ObMultiVersionTabletImage, test_scan)
{
  FileInfoCache fic;
  ObMultiVersionTabletImage null_image(fic);
  int ret = null_image.begin_scan_tablets();
  ASSERT_EQ(OB_ITER_END, ret);

  ObMultiVersionTabletImage image(fic);
  CharArena allocator;
  ObNewRange r1,r2,r3;
  create_range(allocator, r1, 1, ObBorderFlag::INCLUSIVE_START|ObBorderFlag::INCLUSIVE_END, "aoo", "foo");
  create_range(allocator, r2, 1, ObBorderFlag::INCLUSIVE_END, "foo", "mj");
  create_range(allocator, r3, 1, ObBorderFlag::INCLUSIVE_END, "mj", "oi");

  ret = read_all(image);
  ASSERT_EQ(0, ret);

  ret = image.begin_scan_tablets();
  ObTablet* tablet = NULL;
  while (OB_SUCCESS == ret)
  {
    ret = image.get_next_tablet(tablet);
    if (OB_SUCCESS == ret) tablet->dump();
    if (NULL != tablet) image.release_tablet(tablet);
  }
  image.end_scan_tablets();


}

TEST(ObMultiVersionTabletImage, test_query_min_max)
{
  FileInfoCache fic;
  ObMultiVersionTabletImage image(fic);

  fic.init(100);
  CharArena allocator;
  ObNewRange r1,r2,r3;
  create_range(allocator, r1, 1, ObBorderFlag::INCLUSIVE_START|ObBorderFlag::INCLUSIVE_END, "aoo", "foo");
  create_range(allocator, r2, 1, ObBorderFlag::INCLUSIVE_END, "foo", "mj");
  create_range(allocator, r3, 1, ObBorderFlag::INCLUSIVE_END, "mj", "oi");

  ObNewRange rall;
  ObBorderFlag flag;
  flag.set_min_value();
  flag.set_max_value();
  create_range(allocator, rall, 1, flag.get_data(), "aoo", "zoo");

  int ret = 0;
  ObTablet* tablet = NULL;
  ObSSTableId id;
  id.sstable_file_id_ = 1;
  id.sstable_file_offset_ = 0;

  ret = image.alloc_tablet_object(rall, VERSION_1, tablet);
  ASSERT_EQ(0, ret);
  tablet->set_disk_no(1);
  ret = tablet->add_sstable_by_id(id);
  ASSERT_EQ(0, ret);
  ret = image.add_tablet(tablet, false, true);
  ASSERT_EQ(0, ret);


  ret = image.acquire_tablet(r1, ObMultiVersionTabletImage::SCAN_FORWARD, 0, tablet);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(VERSION_1, tablet->get_data_version());
  image.release_tablet(tablet);

  ret = image.acquire_tablet(rall, ObMultiVersionTabletImage::SCAN_FORWARD, 0, tablet);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(VERSION_1, tablet->get_data_version());
  tablet->get_range().hex_dump();
  image.release_tablet(tablet);

  fic.destroy();
}

TEST(ObMultiVersionTabletImage, test_upgrade_null)
{
  FileInfoCache fic;
  ObMultiVersionTabletImage image(fic);

  fic.init(100);
  CharArena allocator;
  ObNewRange r1,r2,r3;
  create_range(allocator, r1, 1, ObBorderFlag::INCLUSIVE_START|ObBorderFlag::INCLUSIVE_END, "aoo", "foo");
  create_range(allocator, r2, 1, ObBorderFlag::INCLUSIVE_END, "foo", "mj");
  create_range(allocator, r3, 1, ObBorderFlag::INCLUSIVE_END, "mj", "oi");

  ObNewRange rall;
  ObBorderFlag flag; flag.set_min_value();
  flag.set_max_value();
  create_range(allocator, rall, 1, flag.get_data(), "aoo", "zoo");

  int ret = 0;

  ASSERT_EQ(0, image.get_serving_version());



  ObTablet* tablet = NULL;
  ObSSTableId id;
  id.sstable_file_id_ = 1;
  id.sstable_file_offset_ = 0;

  ret = image.alloc_tablet_object(rall, VERSION_1, tablet);
  ASSERT_EQ(0, ret);
  tablet->set_disk_no(1);
  ret = tablet->add_sstable_by_id(id);
  ASSERT_EQ(0, ret);
  ret = image.add_tablet(tablet, false, false);
  ASSERT_EQ(0, ret);

  ObTablet *new_tablet = NULL;
  ret = image.alloc_tablet_object(rall, VERSION_2, new_tablet);
  ASSERT_EQ(0, ret);
  new_tablet->set_disk_no(2);
  id.sstable_file_id_ = 15 << 8 | 2;
  id.sstable_file_offset_ = 0;
  ret = new_tablet->add_sstable_by_id(id);
  ASSERT_EQ(0, ret);
  ret = image.upgrade_tablet(tablet, new_tablet, false);
  ASSERT_EQ(0, ret);

  /*
  ret = image.acquire_tablet(r1, 1, 0, tablet);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(VERSION_1, tablet->get_data_version());
  image.release_tablet(tablet);

  ret = image.acquire_tablet(rall, 1, 0, tablet);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(VERSION_1, tablet->get_data_version());
  tablet->get_range().hex_dump();
  image.release_tablet(tablet);
  */

  fic.destroy();
}

TEST(ObMultiVersionTabletImage, test_write_null)
{
  FileInfoCache fic;
  ObMultiVersionTabletImage image(fic);

  fic.init(100);
  CharArena allocator;
  ObNewRange r1;

  ObBorderFlag flag;
  flag.set_min_value();
  flag.set_max_value();

  create_range(allocator, r1, 1, flag.get_data(), "", "");

  int ret = 0;
  ObTablet* tablet = NULL;
  ObSSTableId id;
  id.sstable_file_id_ = 1;
  id.sstable_file_offset_ = 0;

  ret = image.alloc_tablet_object(r1, VERSION_1, tablet);
  ASSERT_EQ(0, ret);
  tablet->set_disk_no(1);
  ret = tablet->add_sstable_by_id(id);
  ASSERT_EQ(0, ret);
  ret = image.add_tablet(tablet, false);
  ASSERT_EQ(0, ret);

  image.write(VERSION_1, 1);

  fic.destroy();
}

TEST(ObMultiVersionTabletImage, test_service)
{
  FileInfoCache fic;
  ObMultiVersionTabletImage image(fic);

  fic.init(100);
  CharArena allocator;
  ObNewRange r1,r2,r3;
  create_range(allocator, r1, 1, ObBorderFlag::INCLUSIVE_START|ObBorderFlag::INCLUSIVE_END, "aoo", "foo");
  create_range(allocator, r2, 1, ObBorderFlag::INCLUSIVE_END, "foo", "mj");
  create_range(allocator, r3, 1, ObBorderFlag::INCLUSIVE_END, "mj", "oi");

  int ret = 0;
  ObTablet* tablet = NULL;
  ObSSTableId id;
  id.sstable_file_id_ = 1;
  id.sstable_file_offset_ = 0;

  ret = image.alloc_tablet_object(r1, VERSION_1, tablet);
  ASSERT_EQ(0, ret);
  tablet->set_disk_no(1);
  ret = tablet->add_sstable_by_id(id);
  ASSERT_EQ(0, ret);
  ret = image.add_tablet(tablet, false);
  ASSERT_EQ(0, ret);

  //image.initialize_service_index();

  /*
  ret = image.alloc_tablet_object(r1, VERSION_2, tablet);
  ASSERT_EQ(0, ret);
  tablet->set_disk_no(1);
  ret = tablet->add_sstable_by_id(id);
  ASSERT_EQ(0, ret);
  ret = image.add_tablet(tablet, false);
  ASSERT_EQ(0, ret);

  ret = image.alloc_tablet_object(r2, VERSION_2, tablet);
  ASSERT_EQ(0, ret);
  id.sstable_file_id_ = 2;
  id.sstable_file_offset_ = 0;
  tablet->set_disk_no(2);
  tablet->add_sstable_by_id(id);
  ret = image.add_tablet(tablet, false);
  ASSERT_EQ(0, ret);

  ret = image.alloc_tablet_object(r3, VERSION_2, tablet);
  ASSERT_EQ(0, ret);
  id.sstable_file_id_ = 3;
  id.sstable_file_offset_ = 0;
  tablet->set_disk_no(3);
  tablet->add_sstable_by_id(id);
  ret = image.add_tablet(tablet, false);
  ASSERT_EQ(0, ret);

  ret = write_all(image);
  ASSERT_EQ(0, ret);
  */
  fic.destroy();
}


TEST(ObMultiVersionTabletImage, test_query_complex2)
{
  FileInfoCache fic;
  ObMultiVersionTabletImage image(fic);

  fic.init(100);
  CharArena allocator;
  ObNewRange r1,r2,r3,r4,g1,g2,g3;
  create_range(allocator, r1, 1001, ObBorderFlag::INCLUSIVE_END, "111", "222");
  create_range(allocator, r2, 1001, ObBorderFlag::INCLUSIVE_END, "444", "666");
  create_range(allocator, r3, 1003, ObBorderFlag::INCLUSIVE_END, "777", "999");
  create_range(allocator, r4, 1003, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MAX_VALUE, "9999", "");

  create_range(allocator, g1, 1001, ObBorderFlag::MAX_VALUE, "666", "");
  create_range(allocator, g2, 1001, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MAX_VALUE, "555", "");
  create_range(allocator, g3, 1002, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MAX_VALUE, "222", "");

  int ret = 0;
  ObTablet* tablet = NULL;
  ObSSTableId id;
  id.sstable_file_id_ = 1;
  id.sstable_file_offset_ = 0;

  ret = image.alloc_tablet_object(r1, VERSION_1, tablet);
  ASSERT_EQ(0, ret);
  tablet->set_disk_no(1);
  //tablet->set_merged(1);
  ret = tablet->add_sstable_by_id(id);
  ASSERT_EQ(0, ret);
  ret = image.add_tablet(tablet, false);
  ASSERT_EQ(0, ret);


  ret = image.alloc_tablet_object(r2, VERSION_1, tablet);
  ASSERT_EQ(0, ret);
  id.sstable_file_id_ = 2;
  id.sstable_file_offset_ = 0;
  tablet->set_disk_no(2);
  tablet->add_sstable_by_id(id);
  ret = image.add_tablet(tablet, false);
  ASSERT_EQ(0, ret);

  ret = image.alloc_tablet_object(r3, VERSION_1, tablet);
  ASSERT_EQ(0, ret);
  id.sstable_file_id_ = 3;
  id.sstable_file_offset_ = 0;
  tablet->set_disk_no(3);
  tablet->add_sstable_by_id(id);
  ret = image.add_tablet(tablet, false);
  ASSERT_EQ(0, ret);

  ret = image.alloc_tablet_object(r4, VERSION_1, tablet);
  ASSERT_EQ(0, ret);
  id.sstable_file_id_ = 4;
  id.sstable_file_offset_ = 0;
  tablet->set_disk_no(4);
  tablet->add_sstable_by_id(id);
  ret = image.add_tablet(tablet, false);
  ASSERT_EQ(0, ret);

  image.prepare_for_service();
  image.dump(false);

  /// querys;;
  ret = image.acquire_tablet(r1, ObMultiVersionTabletImage::SCAN_FORWARD, 0, tablet);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(true, tablet->get_range().equal(r1));
  image.release_tablet(tablet);

  ret = image.acquire_tablet(r2, ObMultiVersionTabletImage::SCAN_FORWARD, 0, tablet);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(true, tablet->get_range().equal(r2));
  image.release_tablet(tablet);

  ret = image.acquire_tablet(r3, ObMultiVersionTabletImage::SCAN_FORWARD, 0, tablet);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(true, tablet->get_range().equal(r3));
  image.release_tablet(tablet);

  ret = image.acquire_tablet(g1, ObMultiVersionTabletImage::SCAN_FORWARD, 0, tablet);
  ASSERT_EQ(OB_CS_TABLET_NOT_EXIST, ret);

  ret = image.acquire_tablet(g2, ObMultiVersionTabletImage::SCAN_FORWARD, 0, tablet);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(true, tablet->get_range().equal(r2));
  image.release_tablet(tablet);

  ret = image.acquire_tablet(g3, ObMultiVersionTabletImage::SCAN_FORWARD, 0, tablet);
  ASSERT_EQ(OB_CS_TABLET_NOT_EXIST, ret);
}


TEST(ObMultiVersionTabletImage, test_query_complex)
{
  FileInfoCache fic;
  ObMultiVersionTabletImage image(fic);

  fic.init(100);
  CharArena allocator;
  ObNewRange r1,r2,r3,r4;
  create_range(allocator, r1, 1, ObBorderFlag::INCLUSIVE_START, "111", "222");
  create_range(allocator, r2, 1, ObBorderFlag::INCLUSIVE_START, "444", "666");
  create_range(allocator, r3, 1, ObBorderFlag::INCLUSIVE_START, "777", "999");
  create_range(allocator, r4, 1, ObBorderFlag::INCLUSIVE_START|ObBorderFlag::MAX_VALUE, "9999", "");

  int ret = 0;
  ObTablet* tablet = NULL;
  ObSSTableId id;
  id.sstable_file_id_ = 1;
  id.sstable_file_offset_ = 0;

  ret = image.alloc_tablet_object(r1, VERSION_1, tablet);
  ASSERT_EQ(0, ret);
  tablet->set_disk_no(1);
  //tablet->set_merged(1);
  ret = tablet->add_sstable_by_id(id);
  ASSERT_EQ(0, ret);
  ret = image.add_tablet(tablet, false);
  ASSERT_EQ(0, ret);


  ret = image.alloc_tablet_object(r2, VERSION_1, tablet);
  ASSERT_EQ(0, ret);
  id.sstable_file_id_ = 2;
  id.sstable_file_offset_ = 0;
  tablet->set_disk_no(2);
  tablet->add_sstable_by_id(id);
  ret = image.add_tablet(tablet, false);
  ASSERT_EQ(0, ret);

  ret = image.alloc_tablet_object(r3, VERSION_1, tablet);
  ASSERT_EQ(0, ret);
  id.sstable_file_id_ = 3;
  id.sstable_file_offset_ = 0;
  tablet->set_disk_no(3);
  tablet->add_sstable_by_id(id);
  ret = image.add_tablet(tablet, false);
  ASSERT_EQ(0, ret);

  ret = image.alloc_tablet_object(r4, VERSION_1, tablet);
  ASSERT_EQ(0, ret);
  id.sstable_file_id_ = 4;
  id.sstable_file_offset_ = 0;
  tablet->set_disk_no(4);
  tablet->add_sstable_by_id(id);
  ret = image.add_tablet(tablet, false);
  ASSERT_EQ(0, ret);

  image.prepare_for_service();
  image.dump(false);

  /// querys;;
  ret = image.acquire_tablet(r1, ObMultiVersionTabletImage::SCAN_FORWARD, 0, tablet);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(true, tablet->get_range().equal(r1));
  image.release_tablet(tablet);

  ret = image.acquire_tablet(r2, ObMultiVersionTabletImage::SCAN_FORWARD, 0, tablet);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(true, tablet->get_range().equal(r2));
  image.release_tablet(tablet);

  ret = image.acquire_tablet(r3, ObMultiVersionTabletImage::SCAN_FORWARD, 0, tablet);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(true, tablet->get_range().equal(r3));
  image.release_tablet(tablet);

  ObNewRange rq1;
  // intersect with r1;
  create_range(allocator, rq1, 1, ObBorderFlag::INCLUSIVE_START|ObBorderFlag::INCLUSIVE_END, "1111", "2222");
  ret = image.acquire_tablet(rq1, ObMultiVersionTabletImage::SCAN_FORWARD, 0, tablet);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(true, tablet->get_range().equal(r1));
  image.release_tablet(tablet);

  ObNewRange rq2;
  // in hole
  create_range(allocator, rq2, 1, ObBorderFlag::INCLUSIVE_START|ObBorderFlag::INCLUSIVE_END, "2222", "3333");
  ret = image.acquire_tablet(rq2, ObMultiVersionTabletImage::SCAN_FORWARD, 0, tablet);
  ASSERT_EQ(OB_CS_TABLET_NOT_EXIST, ret);
  ASSERT_TRUE(NULL == tablet);


  ObNewRange rq3;
  create_range(allocator, rq3, 1, ObBorderFlag::MIN_VALUE|ObBorderFlag::INCLUSIVE_END, "", "3333");
  ret = image.acquire_tablet(rq3, ObMultiVersionTabletImage::SCAN_FORWARD, 0, tablet);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(true, tablet->get_range().equal(r1));
  image.release_tablet(tablet);

  ObNewRange rq4;
  create_range(allocator, rq4, 1, ObBorderFlag::MAX_VALUE, "99999", "");
  ret = image.acquire_tablet(rq4, ObMultiVersionTabletImage::SCAN_FORWARD, 0, tablet);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(true, tablet->get_range().equal(r4));
  image.release_tablet(tablet);

  ObNewRange rq5;
  create_range(allocator, rq5, 1, ObBorderFlag::MIN_VALUE|ObBorderFlag::MAX_VALUE, "", "");
  ret = image.acquire_tablet(rq5, ObMultiVersionTabletImage::SCAN_BACKWARD, 0, tablet);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(true, tablet->get_range().equal(r4));
  image.release_tablet(tablet);


  ObNewRange rq6;
  create_range(allocator, rq6, 1, 0, "666", "777");
  ret = image.acquire_tablet(rq6, ObMultiVersionTabletImage::SCAN_FORWARD, 0, tablet);
  ASSERT_EQ(OB_CS_TABLET_NOT_EXIST, ret);

  ObNewRange rq7;
  create_range(allocator, rq7, 1, ObBorderFlag::INCLUSIVE_END, "666", "777");
  ret = image.acquire_tablet(rq7, ObMultiVersionTabletImage::SCAN_FORWARD, 0, tablet);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(true, tablet->get_range().equal(r3));
  image.release_tablet(tablet);

  fic.destroy();
}



class FooEnvironment : public testing::Environment
{
  public:
    virtual void SetUp()
    {
      //TBSYS_LOGGER.setLogLevel("ERROR");
      //ob_init_memory_pool();
      prepare_sstable_directroy(DISK_NUM);
    }
    virtual void TearDown()
    {
    }
};

int main(int argc, char **argv)
{
  testing::AddGlobalTestEnvironment(new FooEnvironment);
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
