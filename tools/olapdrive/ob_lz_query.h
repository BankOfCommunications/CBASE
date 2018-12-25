/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_lz_query.h for define abstract lz query classs. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_OLAPDRIVE_LZ_QUERY_H
#define OCEANBASE_OLAPDRIVE_LZ_QUERY_H

#include <tbsys.h>
#include "client/ob_client.h"
#include "common/ob_tsi_factory.h"
#include "ob_olapdrive_param.h"
#include "ob_olapdrive_meta.h"
#include "ob_olapdrive_stat.h"
#include "ob_olapdrive_schema.h"
#include "ob_lz_common.h"

namespace oceanbase 
{ 
  namespace olapdrive
  {
    static const int64_t BATCH_SCAN_COUNT = 5;
    static const int64_t FILTER_BUF_SIZE = 512;
    static const int64_t WARN_RESPONSE_TIME = 1000000; //1s

    struct ObThreadRowkeyObjs
    {
      common::ObObj start_rowkey_objs_[MAX_OLAPDRIVE_ROWKEY_COLUMN_COUNT];
      common::ObObj end_rowkey_objs_[MAX_OLAPDRIVE_ROWKEY_COLUMN_COUNT];
    };

    struct ObBatchScanInfo
    {
      int64_t adgroup_id_array_[BATCH_SCAN_COUNT];
      int64_t adgroup_cnt_;
      int64_t bidword_id_array_[BATCH_SCAN_COUNT];
      int64_t bidword_cnt_;
      int64_t creative_id_array_[BATCH_SCAN_COUNT];
      int64_t creative_cnt_;

      ObBatchScanInfo()
      {
        reset();
      }

      void reset()
      {
        memset(this, 0, sizeof(*this));
      }

      void display() const
      {
        int64_t pos = 0;
        char buf[512];

        for (int64_t i = 0; i < adgroup_cnt_; ++i)
        {
          if (i == adgroup_cnt_  - 1)
          {
            pos += sprintf(buf + pos, "adgroup_id_%ld=%ld \n", 
                           i, adgroup_id_array_[i]);
          }
          else
          {
            pos += sprintf(buf + pos, "adgroup_id_%ld=%ld, ", 
                           i, adgroup_id_array_[i]);
          }
        }

        for (int64_t i = 0; i < bidword_cnt_; ++i)
        {
          if (i == bidword_cnt_ - 1)
          {
            pos += sprintf(buf + pos, "bidword_id_%ld=%ld \n", 
                           i, bidword_id_array_[i]);
          }
          else
          {
            pos += sprintf(buf + pos, "bidword_id_%ld=%ld, ", 
                           i, bidword_id_array_[i]);
          }
        }

        for (int64_t i = 0; i < creative_cnt_; ++i)
        {
          if (i == creative_cnt_ - 1)
          {
            pos += sprintf(buf + pos, "creative_id_%ld=%ld \n", 
                           i, creative_id_array_[i]);
          }
          else
          {
            pos += sprintf(buf + pos, "creative_id_%ld=%ld, ", 
                           i, creative_id_array_[i]);
          }
        }

        if (pos > 0)
        {
          fprintf(stderr, "%s", buf);
        }
      }
    };

    struct ObScanExtraInfo
    {
      ObBatchScanInfo batch_scan_info_;
      int64_t platform_id_;
      int64_t pvtype_id_;
      int64_t shop_type_;
      int64_t orderby_col_idx_;
      int64_t adgroup_id_;
      int64_t bidword_id_;
      char filter_str[FILTER_BUF_SIZE];

      void reset()
      {
        batch_scan_info_.reset();
        platform_id_ = 0;
        pvtype_id_ = 0;
        shop_type_ = 0;
        orderby_col_idx_ = 0;
        adgroup_id_ = 0;
        bidword_id_ = 0;
      }

      void display() const
      {
        fprintf(stderr, "platform_id_: %ld \n"
                        "pvtype_id_: %ld \n"
                        "shop_type_: %ld \n"
                        "orderby_col_idx_: %ld \n"
                        "adgroup_id_: %ld \n"
                        "bidword_id_: %ld \n",
                platform_id_, pvtype_id_, shop_type_, orderby_col_idx_,
                adgroup_id_, bidword_id_);
        batch_scan_info_.display();
      }
    };

    struct ObScanInfo
    {
      ObCampaign campaign_;
      ObBatchScanInfo batch_scan_info_;
      int64_t platform_id_;
      int64_t pvtype_id_;
      int64_t shop_type_;
      int64_t orderby_col_idx_;
      int64_t adgroup_id_;
      int64_t start_date_;
      int64_t day_count_;
      int64_t bidword_id_;

      ObScanInfo()
      {
        campaign_.campaign_id_ = -1;
        platform_id_ = -1;
        pvtype_id_ = -1;
        shop_type_ = -1;
        orderby_col_idx_ = -1;
        adgroup_id_ = -1;
        start_date_ = -1;
        day_count_ = -1;
        bidword_id_ = -1;
        batch_scan_info_.reset();
      }

      void display() const
      {
        fprintf(stderr, "campaign_id=%ld, adgroup_id=%ld, "
                        "platform_id_=%ld, pvtype_id_=%ld, "
                        "shop_type_=%ld, orderby_col_idx_=%ld, "
                        "start_date=%ld, day_count=%ld, "
                        "bidword_id_=%ld\n",
                campaign_.campaign_id_, adgroup_id_, platform_id_, 
                pvtype_id_, shop_type_, orderby_col_idx_, start_date_, 
                day_count_, bidword_id_);
        batch_scan_info_.display();
      }
    };

    class ObLzQuery
    {
    public:
      ObLzQuery(client::ObClient& client, ObOlapdriveMeta& meta,
                ObOlapdriveSchema& schema, ObOlapdriveParam& param,
                ObOlapdriveStat& stat);
      virtual ~ObLzQuery();

    public:
      virtual int prepare(common::ObScanParam& scan_param);
      virtual int add_special_column(common::ObScanParam& scan_param);
      virtual int check(const common::ObScanner& scanner, const int64_t time);
      virtual int scan(common::ObScanParam& scan_param, common::ObScanner& scanner);
      virtual const char* get_test_name();

    protected:
      int init();
      int display_scanner(const common::ObScanner& scanner) const;

      int64_t tv_to_microseconds(const timeval& tp);
      int64_t get_cur_microseconds_time();

      int64_t get_random_unit_id();
      int64_t get_random_adgroup_id();
      bool is_val_existent(int64_t* array, const int64_t item_cnt, 
                           const int64_t value);
      int get_random_adgroup_id_array(const ObCampaign& campaign, 
                                      int64_t* adgroup_id_array, 
                                      int64_t& adgroup_cnt);
      int get_random_bidword_id(int64_t& bidword_id);
      int get_random_bidword_id_array(const ObCampaign& campaign,
                                      const int64_t* adgroup_id_array, 
                                      const int64_t adgroup_cnt,
                                      int64_t* bidword_id_array,
                                      int64_t& bidword_cnt);
      int64_t get_not_equl_val_colum_idx();
      int64_t get_random_start_date(int64_t& day_count);
      void get_start_rowkey_struct(ObLZRowkey& rowkey, int64_t& day_count);
      void get_end_rowkey_struct(const ObLZRowkey& start_rowkey, const int64_t day_count, 
                                 ObLZRowkey& end_rowkey);
      int reset_extra_info();

      void init_version_range(common::ObVersionRange& ver_range);
      int build_rowkey(const ObLZRowkey& rowkey, common::ObObj* rowkey_buf, 
                       common::ObRowkey& ret_rowkey);
      int add_scan_range(common::ObScanParam& scan_param);
      void set_read_param(common::ObScanParam& scan_param);
      int add_basic_scan_column(common::ObScanParam& scan_param);
      int build_basic_scan_param(common::ObScanParam& scan_param);
      int add_basic_aggreate_column(common::ObScanParam& scan_param);
      int add_basic_composite_column(common::ObScanParam& scan_param);
      int build_groupby_param(common::ObScanParam& scan_param);
      int add_thedate_groupby_orderby(common::ObScanParam& scan_param);
      int build_batch_scan_filter(char* buf, const int64_t buf_len, int64_t& pos, 
                                  const ObBatchScanInfo& batch_scan_info);

      int get_scan_info(ObScanInfo& scan_info);
      int get_cur_scan_campaign(ObCampaign& campaign);
      int get_cur_scan_adgroup(const ObScanInfo& scan_info, 
                               ObAdgroup& adgroup);
      int64_t calc_row_count(const int64_t end_val, 
                             const int64_t start_val,
                             const int64_t interval);
      int64_t calc_sum_val(const int64_t end_val,
                           const int64_t start_val, 
                           const int64_t interval,
                           const int64_t day_count);
      int check_row_and_cell_num(const common::ObScanner& scanner, 
                                 const int64_t exp_row_num, 
                                 const int64_t exp_cell_num);
      int check_cell(const common::ObCellInfo& cell_info, 
                     const int64_t column_index,
                     const int64_t exp_val);
      int check_one_cell(const common::ObCellInfo& cell_info,
                         const int64_t groupby_col_cnt,
                         const int64_t column_index,
                         const int64_t exp_val,
                         const int64_t sum_val);
      void print_brief_result(const char* test_name, 
                              const common::ObScanner& scanner,
                              const ObScanInfo& scan_info, 
                              const int64_t time,
                              const int64_t org_row_cnt);
      void print_detail_result(const common::ObScanner& scanner);
      bool is_orderby_equal_val_column(const int64_t orderby_col_idx);
      int64_t calc_adgroup_shoptype_sum_val(const ObAdgroup& adgroup, 
                                            const int64_t day_count,
                                            const int64_t shop_type);
      int check_basic_effect_column(const common::ObCellInfo& cell_info, 
                                    const int index, const int64_t sum_val);

    private:
      static const int64_t MAX_SCAN_TYPE = 32;
      static const int64_t START_ROWKEY_NO = 0;
      static const int64_t END_ROWKEY_NO = 1;
       
    protected:
      DISALLOW_COPY_AND_ASSIGN(ObLzQuery);

      client::ObClient& client_;
      ObOlapdriveMeta& meta_;
      ObOlapdriveSchema& schema_;
      ObOlapdriveParam& param_;
      ObOlapdriveStat& stat_;
      ObKeyMeta* rowkey_meta_;
      int64_t log_level_;
    };
  } // end namespace olapdrive
} // end namespace oceanbase

#endif //OCEANBASE_OLAPDRIVE_LZ_QUERY_H
