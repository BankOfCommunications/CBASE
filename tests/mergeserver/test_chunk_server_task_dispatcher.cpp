
#include <gtest/gtest.h>
#include "common/ob_malloc.h"
#include "mergeserver/ob_ms_tablet_location_item.h"
#include "mergeserver/ob_chunk_server_task_dispatcher.h"
#include "common/ob_scan_param.h"
#include "common/ob_crc64.h"
#include "mergeserver/ob_ms_tablet_location_proxy.h"
#include "mergeserver/ob_ms_tablet_location.h"
#include "mergeserver/ob_tablet_location_range_iterator.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;
using namespace testing;

#define FUNC_BLOCK() if (true) \

TEST(ObTabletLocationRangeIterator, range_intersect)
{
  FUNC_BLOCK() // two range equal
  {
    ObTabletLocationRangeIterator iterator;

    int ret = OB_SUCCESS;
    ObRange r1;
    ObRange r2;
    ObRange r3;
    
    r1.table_id_ = 1;
    r2.table_id_ = 1;
    r3.table_id_ = 2;

    char * s1 = "1234";
    char * e1 = "5555";
    r1.start_key_.assign_ptr(s1, strlen(s1));
    r1.end_key_.assign_ptr(e1, strlen(e1));

    r1.border_flag_.set_inclusive_start();
    r1.border_flag_.set_inclusive_end();

    char * s2 = "1234";
    char * e2 = "5555";
    r2.start_key_.assign_ptr(s2, strlen(s2));
    r2.end_key_.assign_ptr(e2, strlen(e2));

    r2.border_flag_.set_inclusive_start();
    r2.border_flag_.set_inclusive_end();


    char * s3 = "1111";
    char * e3 = "6666";
    r3.start_key_.assign_ptr(s3, strlen(s3));
    r3.end_key_.assign_ptr(e3, strlen(e3));

    r3.border_flag_.set_inclusive_start();
    r3.border_flag_.set_inclusive_end();

    ObStringBuf range_buf;
    ret = iterator.range_intersect(r1, r2, r3, range_buf);
    printf("r3.start_key_ [%.*s]\n", r3.start_key_.length(), r3.start_key_.ptr());
    printf("r3.end_key_ [%.*s]\n", r3.end_key_.length(), r3.end_key_.ptr());

    EXPECT_TRUE(r3.start_key_ == ObString(0, 4, "1234"));
    EXPECT_TRUE(r3.end_key_ == ObString(0, 4, "5555"));
    EXPECT_TRUE(r3.border_flag_.inclusive_start());
    EXPECT_TRUE(r3.border_flag_.inclusive_end());
    
    EXPECT_EQ(1, r3.table_id_);
    EXPECT_EQ(0, ret);
  }

  FUNC_BLOCK()
  {
    ObTabletLocationRangeIterator iterator;

    int ret = OB_SUCCESS;
    ObRange r1;
    ObRange r2;
    ObRange r3;
    
    r1.table_id_ = 1;
    r2.table_id_ = 1;
    r3.table_id_ = 2;

    char * s1 = "0000";
    char * e1 = "5555";
    r1.start_key_.assign_ptr(s1, strlen(s1));
    r1.end_key_.assign_ptr(e1, strlen(e1));

    r1.border_flag_.set_inclusive_start();
    r1.border_flag_.set_inclusive_end();

    char * s2 = "1111";
    char * e2 = "6666";
    r2.start_key_.assign_ptr(s2, strlen(s2));
    r2.end_key_.assign_ptr(e2, strlen(e2));

    r2.border_flag_.set_inclusive_start();
    r2.border_flag_.set_inclusive_end();


    char * s3 = "1111";
    char * e3 = "6666";
    r3.start_key_.assign_ptr(s3, strlen(s3));
    r3.end_key_.assign_ptr(e3, strlen(e3));

    r3.border_flag_.set_inclusive_start();
    r3.border_flag_.set_inclusive_end();

    ObStringBuf range_buf;
    ret = iterator.range_intersect(r1, r2, r3, range_buf);
    printf("r3.start_key_ [%.*s]\n", r3.start_key_.length(), r3.start_key_.ptr());
    printf("r3.end_key_ [%.*s]\n", r3.end_key_.length(), r3.end_key_.ptr());

    EXPECT_TRUE(r3.start_key_ == ObString(0, 4, "1111"));
    EXPECT_TRUE(r3.end_key_ == ObString(0, 4, "5555"));
    EXPECT_TRUE(r3.border_flag_.inclusive_start());
    EXPECT_TRUE(r3.border_flag_.inclusive_end());
    EXPECT_EQ(1, r3.table_id_);
    EXPECT_EQ(0, ret);
  }

  FUNC_BLOCK() //min value
  {
    ObTabletLocationRangeIterator iterator;

    int ret = OB_SUCCESS;
    ObRange r1;
    ObRange r2;
    ObRange r3;
    
    r1.table_id_ = 1;
    r2.table_id_ = 1;
    r3.table_id_ = 2;

    char * s1 = "0000";
    char * e1 = "5555";
    r1.start_key_.assign_ptr(s1, strlen(s1));
    r1.end_key_.assign_ptr(e1, strlen(e1));
  
    r1.border_flag_.set_min_value();
    r1.border_flag_.set_inclusive_start();
    r1.border_flag_.set_inclusive_end();

    char * s2 = "1111";
    char * e2 = "6666";
    
    r2.border_flag_.set_min_value();
    r2.start_key_.assign_ptr(s2, strlen(s2));
    r2.end_key_.assign_ptr(e2, strlen(e2));

    r2.border_flag_.set_inclusive_start();
    r2.border_flag_.set_inclusive_end();


    char * s3 = "1111";
    char * e3 = "6666";
    r3.start_key_.assign_ptr(s3, strlen(s3));
    r3.end_key_.assign_ptr(e3, strlen(e3));

    r3.border_flag_.set_inclusive_start();
    r3.border_flag_.set_inclusive_end();

    ObStringBuf range_buf;
    ret = iterator.range_intersect(r1, r2, r3, range_buf);
    printf("r3.start_key_ [%.*s]\n", r3.start_key_.length(), r3.start_key_.ptr());
    printf("r3.end_key_ [%.*s]\n", r3.end_key_.length(), r3.end_key_.ptr());

    // EXPECT_TRUE(r3.start_key_ == ObString(0, 4, "1111"));
    EXPECT_TRUE(r3.end_key_ == ObString(0, 4, "5555"));


    EXPECT_TRUE(r3.border_flag_.is_min_value());

    //EXPECT_TRUE(r3.border_flag_.inclusive_start());
    EXPECT_TRUE(r3.border_flag_.inclusive_end());
    EXPECT_EQ(1, r3.table_id_);
    EXPECT_EQ(0, ret);
  }

  FUNC_BLOCK()
  {
    ObTabletLocationRangeIterator iterator;

    int ret = OB_SUCCESS;
    ObRange r1;
    ObRange r2;
    ObRange r3;
    
    r1.table_id_ = 1;
    r2.table_id_ = 1;
    r3.table_id_ = 2;

    char * s1 = "0000";
    char * e1 = "4444";
    r1.start_key_.assign_ptr(s1, strlen(s1));
    r1.end_key_.assign_ptr(e1, strlen(e1));

    r1.border_flag_.set_inclusive_start();
    r1.border_flag_.set_inclusive_end();

    char * s2 = "5555";
    char * e2 = "6666";
    r2.start_key_.assign_ptr(s2, strlen(s2));
    r2.end_key_.assign_ptr(e2, strlen(e2));

    r2.border_flag_.set_inclusive_start();
    r2.border_flag_.set_inclusive_end();


    char * s3 = "1111";
    char * e3 = "7777";
    r3.start_key_.assign_ptr(s3, strlen(s3));
    r3.end_key_.assign_ptr(e3, strlen(e3));

    r3.border_flag_.set_inclusive_start();
    r3.border_flag_.set_inclusive_end();

    ObStringBuf range_buf;
    ret = iterator.range_intersect(r1, r2, r3, range_buf);
    printf("r3.start_key_ [%.*s]\n", r3.start_key_.length(), r3.start_key_.ptr());
    printf("r3.end_key_ [%.*s]\n", r3.end_key_.length(), r3.end_key_.ptr());

    EXPECT_EQ(OB_EMPTY_RANGE, ret);

    EXPECT_EQ(2, r3.table_id_);
  }
  
  FUNC_BLOCK()
  {
    ObTabletLocationRangeIterator iterator;

    int ret = OB_SUCCESS;
    ObRange r1;
    ObRange r2;
    ObRange r3;
    
    r1.table_id_ = 1;
    r2.table_id_ = 1;
    r3.table_id_ = 2;

    char * s1 = "0000";
    char * e1 = "4444";
    r1.start_key_.assign_ptr(s1, strlen(s1));
    r1.end_key_.assign_ptr(e1, strlen(e1));

    r1.border_flag_.unset_inclusive_start();
    r1.border_flag_.set_inclusive_end();

    char * s2 = "0000";
    char * e2 = "6666";
    r2.start_key_.assign_ptr(s2, strlen(s2));
    r2.end_key_.assign_ptr(e2, strlen(e2));

    r2.border_flag_.set_inclusive_start();
    r2.border_flag_.set_inclusive_end();


    char * s3 = "1111";
    char * e3 = "7777";
    r3.start_key_.assign_ptr(s3, strlen(s3));
    r3.end_key_.assign_ptr(e3, strlen(e3));

    r3.border_flag_.set_inclusive_start();
    r3.border_flag_.set_inclusive_end();

    ObStringBuf range_buf;
    ret = iterator.range_intersect(r1, r2, r3, range_buf);
    printf("r3.start_key_ [%.*s]\n", r3.start_key_.length(), r3.start_key_.ptr());
    printf("r3.end_key_ [%.*s]\n", r3.end_key_.length(), r3.end_key_.ptr());

    EXPECT_FALSE(r3.border_flag_.inclusive_start());
    EXPECT_TRUE(r3.start_key_ == ObString(0, 4, "0000"));
    EXPECT_TRUE(r3.end_key_ == ObString(0, 4, "4444"));
    EXPECT_EQ(0, ret);
    EXPECT_EQ(1, r3.table_id_);
  }

  FUNC_BLOCK() // empty range
  {
    ObTabletLocationRangeIterator iterator;

    int ret = OB_SUCCESS;
    ObRange r1;
    ObRange r2;
    ObRange r3;
    
    r1.table_id_ = 1;
    r2.table_id_ = 1;
    r3.table_id_ = 2;

    char * s1 = "4444";
    char * e1 = "0000";
    r1.start_key_.assign_ptr(s1, strlen(s1));
    r1.end_key_.assign_ptr(e1, strlen(e1));

    r1.border_flag_.unset_inclusive_start();
    r1.border_flag_.set_inclusive_end();

    char * s2 = "0000";
    char * e2 = "6666";
    r2.start_key_.assign_ptr(s2, strlen(s2));
    r2.end_key_.assign_ptr(e2, strlen(e2));

    r2.border_flag_.set_inclusive_start();
    r2.border_flag_.set_inclusive_end();


    char * s3 = "1111";
    char * e3 = "7777";
    r3.start_key_.assign_ptr(s3, strlen(s3));
    r3.end_key_.assign_ptr(e3, strlen(e3));

    r3.border_flag_.set_inclusive_start();
    r3.border_flag_.set_inclusive_end();

    ObStringBuf range_buf;
    ret = iterator.range_intersect(r1, r2, r3, range_buf);
    printf("r3.start_key_ [%.*s]\n", r3.start_key_.length(), r3.start_key_.ptr());
    printf("r3.end_key_ [%.*s]\n", r3.end_key_.length(), r3.end_key_.ptr());

      EXPECT_TRUE(r3.border_flag_.inclusive_start());
    EXPECT_TRUE(r3.start_key_ == ObString(0, 4, "1111"));
    EXPECT_TRUE(r3.end_key_ == ObString(0, 4, "7777"));
    EXPECT_EQ(OB_EMPTY_RANGE, ret);
    EXPECT_EQ(2, r3.table_id_);
  }


}

TEST(ObTabletLocationList, serailize)
{
  ObTabletLocationList list;
 
  ObServer chunkserver(ObServer::IPV4, "10.232.36.33", 7077);
  ObTabletLocation location(1, chunkserver);


  char * start_key = "test_start_key";
  char * end_key = "test_end_key";

  ObRange range;
  range.table_id_ = 3;
  range.start_key_.assign_ptr(start_key, strlen(start_key));
  range.end_key_.assign_ptr(end_key, strlen(end_key));
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  
  list.set_tablet_range(range);
  list.add(location);

  int64_t buf_size = 1024 * 1024 * 2;
  char * buf = (char *)malloc(buf_size);
  int64_t pos = 0;

  int ret = list.serialize(buf, buf_size, pos);
  printf("ret[%d]\n", ret);
  EXPECT_EQ(ret, 0);


  EXPECT_EQ(pos, list.get_serialize_size());

  int64_t data_len = pos;
  pos = 0;

  ObTabletLocationList list2;
  ret = list2.deserialize(buf, data_len, pos);
  EXPECT_EQ(ret, 0);

  // list2.

  // free(buf);



  printf("%d\n", list.get_tablet_range().start_key_.length());
  printf("%d\n", list2.get_tablet_range().start_key_.length());

  EXPECT_TRUE( list.get_tablet_range().start_key_ == list2.get_tablet_range().start_key_ );

  // list2.get_tablet_range().start_key_.ptr();

  // printf("%s",list2.get_tablet_range().start_key_.ptr());

  // EXPECT_EQ(list, list2);
}


TEST(ObChunkServerTaskDispatcher, select_cs)
{
  ObChunkServerTaskDispatcher * dispatcher = ObChunkServerTaskDispatcher::get_instance();
  dispatcher->set_local_ip(37);

  ObScanParam scan_param;

  char * table_name = "collect";
  ObString str_table_name;
  str_table_name.assign_ptr(table_name, strlen(table_name));

  char * start_key = "test_start_key";
  char * end_key = "test_end_key";

  ObRange range;
  range.table_id_ = 3;
  range.start_key_.assign_ptr(start_key, strlen(start_key));
  range.end_key_.assign_ptr(end_key, strlen(end_key));
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();

  scan_param.set(3, str_table_name, range);

  ObChunkServer chunkserver_list[3];
  for(int i=0;i<3;i++)
  {
    chunkserver_list[i].status_ = ObChunkServer::UNREQUESTED;
  }

  int pos = dispatcher->select_cs(chunkserver_list, 3, -1, range); 

  EXPECT_EQ(pos, 2);

  pos = dispatcher->select_cs(chunkserver_list, 3, 0, range);
  EXPECT_EQ(pos, 1);

  pos = dispatcher->select_cs(chunkserver_list, 3, 1, range);
  EXPECT_EQ(pos, 0);

  pos = dispatcher->select_cs(chunkserver_list, 3, 1, range);

  EXPECT_EQ(pos, OB_ENTRY_NOT_EXIST);

}

TEST(ObTabletLocationIterator, next)
{
  ObTabletLocationCache location_cache;

  location_cache.init(1024 * 1024 * 8, 100, 2000);
 
  
  ObTabletLocation location(1, ObServer(ObServer::IPV4, "10.232.36.33", 7077));

  ObRange range;
  range.table_id_ = 3;
 
  char * start_key = "0";
  char * end_key = "10";

  range.start_key_.assign_ptr(start_key, strlen(start_key));
  range.end_key_.assign_ptr(end_key, strlen(end_key));
  range.border_flag_.unset_inclusive_start();
  range.border_flag_.set_inclusive_end();
  range.border_flag_.set_min_value();
  
  ObTabletLocationList list1;
  list1.set_tablet_range(range);
  list1.add(location);
  location_cache.set(range, list1);
  
  start_key = "10";
  end_key = "20";
  range.start_key_.assign_ptr(start_key, strlen(start_key));
  range.end_key_.assign_ptr(end_key, strlen(end_key));
  range.border_flag_.unset_inclusive_start();
  range.border_flag_.set_inclusive_end();
  range.border_flag_.unset_min_value();


  ObTabletLocationList list2;
  list2.set_tablet_range(range);
  list2.add(location);
  location_cache.set(range, list2);
  
  start_key = "20";
  end_key = "30";
  range.start_key_.assign_ptr(start_key, strlen(start_key));
  range.end_key_.assign_ptr(end_key, strlen(end_key));
  range.border_flag_.unset_inclusive_start();
  range.border_flag_.set_inclusive_end();

  ObTabletLocationList list3;
  list3.set_tablet_range(range);
  list3.add(location);
  location_cache.set(range, list3);

  start_key = "30";
  end_key = "40";
  range.start_key_.assign_ptr(start_key, strlen(start_key));
  range.end_key_.assign_ptr(end_key, strlen(end_key));
  range.border_flag_.unset_inclusive_start();
  range.border_flag_.unset_inclusive_end();
  range.border_flag_.set_max_value();

  ObTabletLocationList list4;
  list4.set_tablet_range(range);
  list4.add(location);
  location_cache.set(range, list4);
  
  ObServer mergeserver(ObServer::IPV4, "10.232.36.33", 7077);

  ObMergerLocationCacheProxy cache_proxy(mergeserver, (ObMergerRootRpcProxy *)NULL, &location_cache);
  
  ObRange scan_range;
  ObScanParam scan_param;
  
  ObChunkServer chunkserver_out;
  // chunkserver_out.status_ = ObChunkServer::UNREQUESTED;

  ObRange tablet_range_out;
  int32_t replica_count = 1;

  ObStringBuf range_rowkey_buf;

  ObTabletLocationRangeIterator iterator;

  int ret = OB_SUCCESS;

  //==============================
  // test 0 empty
  //==============================
  scan_range.start_key_.assign_ptr("1", 1);
  scan_range.end_key_.assign_ptr("0", 1);
  scan_range.border_flag_.set_inclusive_start();
  scan_range.border_flag_.set_inclusive_end();
  scan_range.border_flag_.unset_max_value();
  scan_range.border_flag_.unset_min_value();

  scan_param.set(3, ObString(0, strlen("table"), "table"), scan_range); 
  scan_param.set_scan_direction(ObScanParam::FORWARD);

  iterator.initialize(&cache_proxy, &scan_param, &range_rowkey_buf);

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(OB_ITER_END, ret);

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(OB_ITER_END, ret);

 
  //==============================
  // test 1
  //==============================
  scan_range.start_key_.assign_ptr("0", 1);
  scan_range.end_key_.assign_ptr("1", 1);
  scan_range.border_flag_.set_inclusive_start();
  scan_range.border_flag_.set_inclusive_end();
  scan_range.border_flag_.unset_max_value();
  scan_range.border_flag_.unset_min_value();

  scan_param.set(3, ObString(0, strlen("table"), "table"), scan_range); 
  scan_param.set_scan_direction(ObScanParam::FORWARD);

  iterator.initialize(&cache_proxy, &scan_param, &range_rowkey_buf);
  
  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(0, ret);
  EXPECT_TRUE(tablet_range_out.start_key_ == ObString(0, 1, "0"));
  EXPECT_TRUE(tablet_range_out.end_key_ == ObString(0, 1, "1"));

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(OB_ITER_END, ret);

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(OB_ITER_END, ret);

  //==============================
  // test 2
  //==============================
  scan_range.start_key_.assign_ptr("1", 1);
  scan_range.end_key_.assign_ptr("11", 2);
  scan_range.border_flag_.set_inclusive_start();
  scan_range.border_flag_.set_inclusive_end();
  scan_range.border_flag_.unset_max_value();
  scan_range.border_flag_.unset_min_value();

  scan_param.set(3, ObString(0, strlen("table"), "table"), scan_range); 
  scan_param.set_scan_direction(ObScanParam::FORWARD);
  
  iterator.initialize(&cache_proxy, &scan_param, &range_rowkey_buf);

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(ret, OB_SUCCESS);

  EXPECT_TRUE(tablet_range_out.end_key_ == ObString(0, 2, "10"));
  
  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(0, ret);

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(ret, OB_ITER_END);


  //===============================
  // test 3
  //===============================
  scan_range.start_key_.assign_ptr("1", 1);
  scan_range.end_key_.assign_ptr("11", 2);
  scan_range.border_flag_.set_max_value();

  scan_param.set(3, ObString(0, strlen("table"), "table"), scan_range); 
  scan_param.set_scan_direction(ObScanParam::FORWARD);
  
  iterator.initialize(&cache_proxy, &scan_param, &range_rowkey_buf);

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(ret, OB_SUCCESS);
  EXPECT_TRUE(tablet_range_out.end_key_ == ObString(0, 2, "10"));
  
  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(ret, OB_SUCCESS);
  EXPECT_TRUE(tablet_range_out.end_key_ == ObString(0, 2, "20"));

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(ret, OB_SUCCESS);
  EXPECT_TRUE(tablet_range_out.end_key_ == ObString(0, 2, "30"));

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(0, ret);
  EXPECT_TRUE(tablet_range_out.border_flag_.is_max_value());
  
  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(ret, OB_ITER_END);

  //===============================
  // test 3.1
  //===============================
  scan_range.start_key_.assign_ptr("1", 1);
  scan_range.end_key_.assign_ptr("11", 2);
  scan_range.border_flag_.set_max_value();
  scan_range.border_flag_.set_min_value();

  scan_param.set(3, ObString(0, strlen("table"), "table"), scan_range); 
  scan_param.set_scan_direction(ObScanParam::FORWARD);
  
  iterator.initialize(&cache_proxy, &scan_param, &range_rowkey_buf);

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(ret, OB_SUCCESS);
  EXPECT_TRUE(tablet_range_out.end_key_ == ObString(0, 2, "10"));
  
  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(ret, OB_SUCCESS);
  EXPECT_TRUE(tablet_range_out.end_key_ == ObString(0, 2, "20"));

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(ret, OB_SUCCESS);
  EXPECT_TRUE(tablet_range_out.end_key_ == ObString(0, 2, "30"));

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(0, ret);
  EXPECT_TRUE(tablet_range_out.border_flag_.is_max_value());
  
  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(ret, OB_ITER_END);

  //===============================
  // test 3.2
  //===============================
  scan_range.start_key_.assign_ptr("10", 2);
  scan_range.end_key_.assign_ptr("11", 2);
  scan_range.border_flag_.unset_inclusive_start();
  scan_range.border_flag_.set_inclusive_end();
  scan_range.border_flag_.unset_max_value();
  scan_range.border_flag_.unset_min_value();

  scan_param.set(3, ObString(0, strlen("table"), "table"), scan_range); 
  scan_param.set_scan_direction(ObScanParam::FORWARD);
  
  iterator.initialize(&cache_proxy, &scan_param, &range_rowkey_buf);

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(0, ret);
  EXPECT_TRUE(tablet_range_out.end_key_ == ObString(0, 2, "11"));

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(OB_ITER_END, ret);
  
  //===============================
  // test 3.3
  //===============================
  scan_range.start_key_.assign_ptr("10", 2);
  scan_range.end_key_.assign_ptr("20", 2);
  scan_range.border_flag_.unset_inclusive_start();
  scan_range.border_flag_.set_inclusive_end();
  scan_range.border_flag_.unset_max_value();
  scan_range.border_flag_.unset_min_value();

  scan_param.set(3, ObString(0, strlen("table"), "table"), scan_range); 
  scan_param.set_scan_direction(ObScanParam::FORWARD);
  
  iterator.initialize(&cache_proxy, &scan_param, &range_rowkey_buf);

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(0, ret);
  EXPECT_TRUE(tablet_range_out.end_key_ == ObString(0, 2, "20"));

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(OB_ITER_END, ret);

  //===============================
  // test 3.4
  //===============================
  scan_range.start_key_.assign_ptr("10", 2);
  scan_range.end_key_.assign_ptr("20\0", 3);
  scan_range.border_flag_.unset_inclusive_start();
  scan_range.border_flag_.set_inclusive_end();
  //scan_range.border_flag_.unset_inclusive_end();
  scan_range.border_flag_.unset_max_value();
  scan_range.border_flag_.unset_min_value();

  scan_param.set(3, ObString(0, strlen("table"), "table"), scan_range); 
  scan_param.set_scan_direction(ObScanParam::FORWARD);
  
  iterator.initialize(&cache_proxy, &scan_param, &range_rowkey_buf);

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(ret, OB_SUCCESS);
  EXPECT_TRUE(tablet_range_out.end_key_ == ObString(0, 2, "20"));

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(0, ret);
  EXPECT_TRUE(tablet_range_out.end_key_ == ObString(0, 3, "20\0"));

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(OB_ITER_END, ret);

  //==============================
  // test 4
  //==============================
  scan_range.start_key_.assign_ptr("1", 1);
  scan_range.end_key_.assign_ptr("11", 2);
  scan_range.border_flag_.unset_max_value();
  scan_range.border_flag_.unset_min_value();

  scan_param.set(3, ObString(0, strlen("table"), "table"), scan_range);
  scan_param.set_scan_direction(ObScanParam::BACKWARD);
  
  iterator.initialize(&cache_proxy, &scan_param, &range_rowkey_buf);

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(ret, OB_SUCCESS);
  EXPECT_TRUE(tablet_range_out.end_key_ == ObString(0, 2, "11"));

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(0, ret);
  EXPECT_TRUE(tablet_range_out.end_key_ == ObString(0, 2, "10"));

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(OB_ITER_END, ret);


  //==============================
  // test 5
  //==============================
  scan_range.start_key_.assign_ptr("1", 1);
  scan_range.end_key_.assign_ptr("11", 2);
  scan_range.border_flag_.unset_max_value();
  scan_range.border_flag_.set_min_value();

  scan_param.set(3, ObString(0, strlen("table"), "table"), scan_range);
  scan_param.set_scan_direction(ObScanParam::BACKWARD);
  
  iterator.initialize(&cache_proxy, &scan_param, &range_rowkey_buf);

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(ret, OB_SUCCESS);
  EXPECT_TRUE(tablet_range_out.end_key_ == ObString(0, 2, "11"));

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(0, ret);
  EXPECT_TRUE(tablet_range_out.end_key_ == ObString(0, 2, "10"));
  EXPECT_TRUE(tablet_range_out.border_flag_.is_min_value());

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(OB_ITER_END, ret);

  //=============================
  // test 6
  //=============================
  scan_range.start_key_.assign_ptr("1", 1);
  scan_range.end_key_.assign_ptr("11", 2);
  scan_range.border_flag_.set_max_value();
  scan_range.border_flag_.set_min_value();

  scan_param.set(3, ObString(0, strlen("table"), "table"), scan_range);
  scan_param.set_scan_direction(ObScanParam::BACKWARD);
  
  iterator.initialize(&cache_proxy, &scan_param, &range_rowkey_buf);

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(ret, OB_SUCCESS);
  EXPECT_TRUE(tablet_range_out.border_flag_.is_max_value());

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(ret, OB_SUCCESS);
  EXPECT_TRUE(tablet_range_out.end_key_ == ObString(0, 2, "30"));

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(ret, OB_SUCCESS);
  EXPECT_TRUE(tablet_range_out.end_key_ == ObString(0, 2, "20"));

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(0, ret);
  EXPECT_TRUE(tablet_range_out.end_key_ == ObString(0, 2, "10"));
  EXPECT_TRUE(tablet_range_out.border_flag_.is_min_value());

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(OB_ITER_END, ret);

  //=============================
  // test 7
  //=============================
  scan_range.start_key_.assign_ptr("1", 1);
  scan_range.end_key_.assign_ptr("11", 2);
  scan_range.border_flag_.set_max_value();
  scan_range.border_flag_.unset_min_value();

  scan_param.set(3, ObString(0, strlen("table"), "table"), scan_range);
  scan_param.set_scan_direction(ObScanParam::BACKWARD);
  
  iterator.initialize(&cache_proxy, &scan_param, &range_rowkey_buf);

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(ret, OB_SUCCESS);
  EXPECT_TRUE(tablet_range_out.border_flag_.is_max_value());

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(ret, OB_SUCCESS);
  EXPECT_TRUE(tablet_range_out.end_key_ == ObString(0, 2, "30"));

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(ret, OB_SUCCESS);
  EXPECT_TRUE(tablet_range_out.end_key_ == ObString(0, 2, "20"));

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(0, ret);
  EXPECT_TRUE(tablet_range_out.end_key_ == ObString(0, 2, "10"));
  EXPECT_FALSE(tablet_range_out.border_flag_.is_min_value());

  ret = iterator.next(&chunkserver_out, replica_count, tablet_range_out);
  EXPECT_EQ(OB_ITER_END, ret);


}

TEST(ObChunkServerTaskDispatcher, get_tablet_locations)
{
  ObTabletLocationCache location_cache;


  location_cache.init(1024 * 1024 * 8, 100, 2000);

  ObTabletLocationList list;
 
  ObServer chunkserver(ObServer::IPV4, "10.232.36.33", 7077);
  ObTabletLocation location(1, chunkserver);


  char * start_key = "000000";
  char * end_key = "100000";

  ObRange range;
  range.table_id_ = 3;
  range.start_key_.assign_ptr(start_key, strlen(start_key));
  range.end_key_.assign_ptr(end_key, strlen(end_key));
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  
  list.set_tablet_range(range);
  list.add(location);


  location_cache.set(range, list);

  ObServer mergeserver(ObServer::IPV4, "10.232.36.33", 7077);

  ObMergerLocationCacheProxy cache_proxy(mergeserver, (ObMergerRootRpcProxy *)NULL, &location_cache);

  
  ObChunkServer chunkserver_out;
  chunkserver_out.status_ = ObChunkServer::UNREQUESTED;

  ObCellInfo cell;
  cell.row_key_ = ObString(0, 6, "000001");
  cell.table_id_ = 3;

  ObRange range_out;
  
  int32_t replica_count = 1;
  cache_proxy.get_tablet_location_and_range(&chunkserver_out, replica_count, cell, range_out);

  EXPECT_TRUE(range.start_key_ == range_out.start_key_);
  EXPECT_TRUE(range.end_key_ == range_out.end_key_);

}


int main(int argc, char **argv)
{
  ob_init_crc64_table(OB_DEFAULT_CRC64_POLYNOM);
  ob_init_memory_pool();
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

