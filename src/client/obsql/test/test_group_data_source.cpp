/*
 *
 * This program is free software; you can redistribute it and/or modify 
 * it under the terms of the GNU General Public License version 2 as 
 * published by the Free Software Foundation.
 *
 * test_group_data_source.cpp is for what ...
 *
 * Version: ***: test_group_data_source.cpp  Wed Jan 23 10:59:08 2013 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want 
 *
 */
#include <gtest/gtest.h>
#include "ob_sql_group_data_source.h"
#include "ob_sql_struct.h"
#include "ob_sql_global.h"
#include "ob_sql_util.h"
#include "ob_sql_cluster_config.h"
#include "ob_sql_conn_recycle.h"
#include "test_sql_base.h"
#include "common/ob_malloc.h"

using namespace oceanbase::common;
class ObSQLGDSTest: public ObSQLBaseTest
{
  public:
    ObSQLGDSTest(){};
    virtual ~ObSQLGDSTest(){};
    virtual void SetUp();
  //private:
    void test_gds_init();
    void test_gds_update_delete_ms();
    void test_gds_update_add_ms();
    void test_gds_update_delete_cluster();
    void test_gds_update_add_cluster();
    void test_gds_update_mix();
    void test_gds_get_master();
};

void ObSQLGDSTest::SetUp()
{
  ObSQLBaseTest::SetUp();
  g_inited = 1;//skip internal init call by mysql_init
}

void ObSQLGDSTest::test_gds_init()
{
  int ret = OB_SQL_SUCCESS;
  ret = update_group_ds(g_config_using, gds_);
  fprintf(stderr, "gds cluster size is %d\n", gds_->csize_);
  ASSERT_EQ(OB_SQL_SUCCESS, ret);
  ASSERT_EQ(20, gds_->max_conn_);
  //ASSERT_EQ(3, g_config_using->cluster_size_);
  //ASSERT_EQ(3, gds_->csize_);
  ASSERT_EQ(gds_->size_, gds_->csize_);
  int num = 0;
  int dsnum = 0;
  for(; num < gds_->csize_; ++num)
  {
    ObClusterInfo *cluster = gds_->clusters_ + num;
    ASSERT_EQ(num + 1, cluster->size_);
    ASSERT_EQ(num + 1, cluster->csize_);
    ASSERT_EQ(num, static_cast<int>(cluster->cluster_id_));
    if (0 == num)
    {
      ASSERT_EQ(1, cluster->is_master_);
    }
    else
    {
      ASSERT_EQ(2, cluster->is_master_);
    }
    //ASSERT rootserver
    ASSERT_EQ(ips[num%3], cluster->rs_.ip_);
    ASSERT_EQ(3456, static_cast<int>(cluster->rs_.port_));
    ASSERT_EQ(3 + num*10, static_cast<int>(cluster->rs_.version_));
    ASSERT_EQ(4 + num*10, static_cast<int>(cluster->rs_.percent_));
    ASSERT_EQ(5 + num*10, static_cast<int>(cluster->rs_.master_));
    
    //ASSERT datasource
    for(; dsnum < cluster->csize_; ++dsnum)
    {
      ObDataSource *ds = cluster->dslist_ + dsnum;
      ASSERT_EQ(ds->cluster_, cluster);
      ASSERT_EQ(ips[dsnum%3], ds->server_.ip_);
      ASSERT_EQ(3456, static_cast<int>(ds->server_.port_));
      ASSERT_EQ(100 + dsnum*100 + num*10, static_cast<int>(ds->server_.version_));
      ASSERT_EQ(1000 + dsnum*100 + num*10, static_cast<int>(ds->server_.percent_));
      ASSERT_EQ(10000 + dsnum*100 + num*10, static_cast<int>(ds->server_.master_));
      ASSERT_EQ(20, ds->conn_list_.free_list_.size_);
      ASSERT_EQ(0, ds->conn_list_.used_list_.size_);
    }
  }
}

void ObSQLGDSTest::test_gds_update_delete_ms()
{
  ASSERT_EQ(OB_SQL_SUCCESS, update_group_ds(g_config_using, gds_));
  //constrct new config and call update()
  g_config_update->cluster_size_ = 3;
  g_config_update->max_conn_size_ = 20;
  g_config_update->min_conn_size_ = 10;
  g_config_update->ms_table_inited_ = 0;
  g_config_update->master_cluster_id_ = 1;
  int cidx = 0;
  int sidx = 0;
  for (; cidx < g_config_update->cluster_size_; ++cidx)
  {
    ObSQLClusterConfig *cluster = g_config_update->clusters_ + cidx;
    if (cidx == 0)
    {
      cluster->cluster_type_ = MASTER;
    }
    else
    {
      cluster->cluster_type_ = SLAVE;
    }
    cluster->cluster_id_ = cidx;
    cluster->flow_weight_ = 2;
    cluster->server_num_ = 2;
    cluster->server_.ip_ = ips[cidx%3];
    cluster->server_.port_ = 3456;
    cluster->server_.version_ = 3 + cidx*10;
    cluster->server_.percent_ = 4 + cidx*10;
    cluster->server_.master_ = 5 + cidx*10;
    sidx = 0;
    for(; sidx < cluster->server_num_; sidx++)
    {
      ObServerInfo *ms = cluster->merge_server_ + sidx;
      if (1== cidx && 1 == sidx)
      {
        ms->ip_ = ips[(sidx+1)%3];
      }
      else
      {
        ms->ip_ = ips[sidx%3];
      }
      ms->port_ = 3456;
      ms->version_ = 100 + sidx*100 + cidx*10;
      ms->percent_ = 1000 + sidx*100 + cidx*10;
      ms->master_ = 10000 + sidx*100 + cidx*10;
    }
  }
  
  //ASSERT update gds success
  ASSERT_EQ(OB_SQL_SUCCESS, do_update());
  ASSERT_EQ(20, gds_->max_conn_);
  ASSERT_EQ(3, gds_->csize_);
  ASSERT_EQ(gds_->size_, gds_->csize_);
  int num = 0;
  int dsnum = 0;
  for(; num < gds_->csize_; ++num)
  {
    ObClusterInfo *cluster = gds_->clusters_ + num;
    //ASSERT_EQ(2, cluster->size_);
    ASSERT_EQ(2, cluster->csize_);
    fprintf(stderr, "cluster size is %d\n", cluster->size_);
    fprintf(stderr, "cluster csize is %d\n", cluster->csize_);
    ASSERT_EQ(num, static_cast<int>(cluster->cluster_id_));
    if (0 == num)
    {
      ASSERT_EQ(1, cluster->is_master_);
    }
    else
    {
      ASSERT_EQ(2, cluster->is_master_);
    }
    //ASSERT rootserver
    ASSERT_EQ(ips[num%3], cluster->rs_.ip_);
    ASSERT_EQ(3456, static_cast<int>(cluster->rs_.port_));
    ASSERT_EQ(3 + num*10, static_cast<int>(cluster->rs_.version_));
    ASSERT_EQ(4 + num*10, static_cast<int>(cluster->rs_.percent_));
    ASSERT_EQ(5 + num*10, static_cast<int>(cluster->rs_.master_));
    
    dsnum = 0;
    //ASSERT datasource
    for(; dsnum < cluster->csize_; ++dsnum)
    {
      ObDataSource *ds = cluster->dslist_ + dsnum;
      ASSERT_EQ(ds->cluster_, cluster);
      if (1 == num && 1 == dsnum)
      {
        ASSERT_EQ(ips[(dsnum + 1)%3], ds->server_.ip_);
      }
      else
      {
        ASSERT_EQ(ips[dsnum%3], ds->server_.ip_);
      }
      ASSERT_EQ(3456, static_cast<int>(ds->server_.port_));
      ASSERT_EQ(100 + dsnum*100 + num*10, static_cast<int>(ds->server_.version_));
      ASSERT_EQ(1000 + dsnum*100 + num*10, static_cast<int>(ds->server_.percent_));
      ASSERT_EQ(10000 + dsnum*100 + num*10, static_cast<int>(ds->server_.master_));
      ASSERT_EQ(20, ds->conn_list_.free_list_.size_);
      ASSERT_EQ(0, ds->conn_list_.used_list_.size_);
    }
  }
  ASSERT_EQ(2, g_delete_ms_list.size_);
}

void ObSQLGDSTest::test_gds_update_add_ms()
{
  
}

void ObSQLGDSTest::test_gds_update_delete_cluster()
{
  ASSERT_EQ(0, g_delete_ms_list.size_);
  ASSERT_EQ(OB_SQL_SUCCESS, update_group_ds(g_config_using, gds_));
  //constrct new config and call update()
  g_config_update->cluster_size_ = 2;
  g_config_update->max_conn_size_ = 20;
  g_config_update->min_conn_size_ = 10;
  g_config_update->ms_table_inited_ = 0;
  g_config_update->master_cluster_id_ = 1;
  int cidx = 0;
  int sidx = 0;
  for (; cidx < g_config_update->cluster_size_; ++cidx)
  {
    ObSQLClusterConfig *cluster = g_config_update->clusters_ + cidx;
    if (cidx == 0)
    {
      cluster->cluster_type_ = MASTER;
      cluster->cluster_id_ = cidx;
    }
    else
    {
      cluster->cluster_type_ = SLAVE;
      cluster->cluster_id_ = cidx + 1;
    }

    cluster->flow_weight_ = 2;
    cluster->server_num_ = (int16_t)(cidx + 1);
    cluster->server_.ip_ = ips[cidx%3];
    cluster->server_.port_ = 3456;
    cluster->server_.version_ = 3 + cidx*10;
    cluster->server_.percent_ = 4 + cidx*10;
    cluster->server_.master_ = 5 + cidx*10;
    sidx = 0;
    for(; sidx < cidx + 1; sidx++)
    {
      ObServerInfo *ms = cluster->merge_server_ + sidx;
      ms->ip_ = ips[sidx%3];
      ms->port_ = 3456;
      ms->version_ = 100 + sidx*100 + cidx*10;
      ms->percent_ = 1000 + sidx*100 + cidx*10;
      ms->master_ = 10000 + sidx*100 + cidx*10;
    }
  }
  
  //ASSERT update gds success
  dump_config(g_config_using);
  ASSERT_EQ(OB_SQL_SUCCESS, do_update());
  dump_config(g_config_using);
  ASSERT_EQ(20, gds_->max_conn_);
  ASSERT_EQ(2, gds_->csize_);
  ASSERT_EQ(gds_->size_, gds_->csize_);
  int num = 0;
  int dsnum = 0;
  for(; num < gds_->csize_; ++num)
  {
    ObClusterInfo *cluster = gds_->clusters_ + num;
    if (0 == num)
    {
      ASSERT_EQ(1, cluster->csize_);
      ASSERT_EQ(num, static_cast<int>(cluster->cluster_id_));
      ASSERT_EQ(1, cluster->is_master_);
      ASSERT_EQ(ips[num%3], cluster->rs_.ip_);
      ASSERT_EQ(3456, static_cast<int>(cluster->rs_.port_));
      ASSERT_EQ(3 + num*10, static_cast<int>(cluster->rs_.version_));
      ASSERT_EQ(4 + num*10, static_cast<int>(cluster->rs_.percent_));
      ASSERT_EQ(5 + num*10, static_cast<int>(cluster->rs_.master_));
    }
    else
    {
//      ASSERT_EQ(2, cluster->csize_);
      ASSERT_EQ(num + 1, static_cast<int>(cluster->cluster_id_));
      ASSERT_EQ(2, cluster->is_master_);
      ASSERT_EQ(ips[(num+1)%3], cluster->rs_.ip_);
      ASSERT_EQ(3456, static_cast<int>(cluster->rs_.port_));
      ASSERT_EQ(3 + (num + 1)*10, static_cast<int>(cluster->rs_.version_));
      ASSERT_EQ(4 + (num + 1)*10, static_cast<int>(cluster->rs_.percent_));
      ASSERT_EQ(5 + (num + 1)*10, static_cast<int>(cluster->rs_.master_));
    }

    dsnum = 0;
    //ASSERT datasource
    for(; dsnum < cluster->csize_; ++dsnum)
    {
      ObDataSource *ds = cluster->dslist_ + dsnum;
      ASSERT_EQ(ds->cluster_, cluster);
      ASSERT_EQ(ips[dsnum%3], ds->server_.ip_);
      ASSERT_EQ(3456, static_cast<int>(ds->server_.port_));
      if (0 == num)
      {
        ASSERT_EQ(100 + dsnum*100 + num*10, static_cast<int>(ds->server_.version_));
        ASSERT_EQ(1000 + dsnum*100 + num*10, static_cast<int>(ds->server_.percent_));
        ASSERT_EQ(10000 + dsnum*100 + num*10, static_cast<int>(ds->server_.master_));
      }
      else
      {
        ASSERT_EQ(100 + dsnum*100 + (num+1)*10, static_cast<int>(ds->server_.version_));
        ASSERT_EQ(1000 + dsnum*100 + (num+1)*10, static_cast<int>(ds->server_.percent_));
        ASSERT_EQ(10000 + dsnum*100 + (num+1)*10, static_cast<int>(ds->server_.master_));
      }
      fprintf(stderr, "cluster is %d, dsnum is %d\n", num, dsnum);
      ASSERT_EQ(20, ds->conn_list_.free_list_.size_);
      ASSERT_EQ(0, ds->conn_list_.used_list_.size_);
    }
  }
  ASSERT_EQ(3, g_delete_ms_list.size_);
  dump_delete_ms_conn();
}

void ObSQLGDSTest::test_gds_update_add_cluster()
{
  
}

void ObSQLGDSTest::test_gds_update_mix()
{
  
}

void ObSQLGDSTest::test_gds_get_master()
{
  //int ret = OB_SQL_SUCCESS;
  ASSERT_EQ(OB_SQL_SUCCESS, update_group_ds(g_config_using, gds_));
  ASSERT_EQ(gds_->clusters_ + 0, get_master_cluster(gds_));
}

TEST_F(ObSQLGDSTest, init)
{
  test_gds_init();
}

TEST_F(ObSQLGDSTest, update_ms)
{
  test_gds_update_delete_ms();
}

TEST_F(ObSQLGDSTest, update_cluster)
{
  test_gds_update_delete_cluster();
}

TEST_F(ObSQLGDSTest, get_master)
{
  test_gds_get_master();
}

int main(int argc, char* argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  ob_init_memory_pool();
  int ret = RUN_ALL_TESTS();
  return ret;
}
