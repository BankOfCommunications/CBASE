/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_write_worker.h for define write worker thread. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_OLAPDRIVE_WRITE_WORKER_H
#define OCEANBASE_OLAPDRIVE_WRITE_WORKER_H

#include <tbsys.h>
#include "client/ob_client.h"
#include "ob_olapdrive_param.h"
#include "ob_olapdrive_meta.h"
#include "ob_olapdrive_stat.h"
#include "ob_olapdrive_schema.h"
#include "ob_lz_common.h"

namespace oceanbase 
{ 
  namespace olapdrive
  {
    struct ObDist
    {
      int64_t start_cnt_;
      int64_t end_cnt_;
      int64_t prop_; //x% * 10000, 10000 times
    };

    struct ObCampaignRowDist
    {
      ObDist dist_[MAX_DIST_COUNT];
      int64_t dist_cnt_;
    };

    struct ObCampaignAdgroupDist
    {
      ObDist dist_[MAX_DIST_COUNT];
      int64_t dist_cnt_;
    };

    struct ObDateStatus
    {
      void reset()
      {
        write_day_cnt_ = 0;
        cur_day_cnt_ = 0;
        wrote_row_cnt_ = 0;
      }

      void display() const
      {
        fprintf(stderr, "\twrite_day_cnt_: %ld \n"
                        "\tcur_day_cnt_: %ld \n"
                        "\twrote_row_cnt_: %ld \n\n",
                write_day_cnt_, cur_day_cnt_, wrote_row_cnt_);
      }

      int64_t write_day_cnt_;
      int64_t cur_day_cnt_;
      int64_t wrote_row_cnt_;
    };

    struct ObCampaignStatus
    {
      void reset()
      {
        cur_campaign_.reset();
        campaign_row_cnt_ = 0;
        cur_campaign_row_cnt_ = 0;
        campaign_adgroup_cnt_ = 0;
        cur_campaign_adgroup_cnt_ = 0;
      }

      void display() const
      {
        cur_campaign_.display();
        fprintf(stderr, "\tcampaign_row_cnt_: %ld \n"
                        "\tcur_campaign_row_cnt_: %ld \n"
                        "\tcampaign_adgroup_cnt_: %ld \n"
                        "\tcur_campaign_adgroup_cnt_:%ld \n\n",
                campaign_row_cnt_, cur_campaign_row_cnt_, 
                campaign_adgroup_cnt_, cur_campaign_adgroup_cnt_);
      }

      ObCampaign cur_campaign_;
      int64_t campaign_row_cnt_;
      int64_t cur_campaign_row_cnt_;
      int64_t campaign_adgroup_cnt_;
      int64_t cur_campaign_adgroup_cnt_;
    };

    struct ObAdgroupStatus
    {
      void reset()
      {
        cur_adgroup_.reset();
        adgroup_row_cnt_ = 0;
        cur_adgroup_row_cnt_ = 0;
      }

      void display() const
      {
        cur_adgroup_.display();
        fprintf(stderr, "\tadgroup_row_cnt_: %ld \n"
                        "\tcur_adgroup_row_cnt_: %ld \n\n",
                adgroup_row_cnt_, cur_adgroup_row_cnt_);
      }

      ObAdgroup cur_adgroup_;
      int64_t adgroup_row_cnt_;
      int64_t cur_adgroup_row_cnt_;
    };

    class ObWriteWorker : public tbsys::CDefaultRunnable
    {
    public:
      ObWriteWorker(client::ObClient& client, ObOlapdriveMeta& meta,
                    ObOlapdriveSchema& schema, ObOlapdriveParam& param,
                    ObOlapdriveStat& stat);
      ~ObWriteWorker();

    public:
      virtual void run(tbsys::CThread* thread, void* arg);

    private:
      int init();
      void init_default_row_dist(ObCampaignRowDist& row_dist);
      void init_default_adgroup_dist(ObCampaignAdgroupDist& adgroup_dist);

      int do_write_work();
      int write_next_row();

      int do_date_change();
      int update_prev_date();
      int update_rowkey_meta();

      int do_campaign_change(bool update_prev = true);
      void calc_campaign_first_time();
      void assign_existent_campaign();
      int update_prev_campaign();

      int do_adgroup_change(bool update_prev = true);
      void calc_adgroup_first_time();
      int update_prev_adgroup();
      int do_key_type_change();

      bool is_date_changed() const;
      bool is_campaign_changed() const;
      bool is_adgroup_changed() const;
      bool is_key_type_changed() const;
      bool is_first_date() const;

      int build_lz_rowkey();
      int build_mutator();
      int set_row_cells(const common::ObColumnSchemaV2& column_schema,
                        const int64_t column_idx, common::ObObj& obj_val);
      int set_obj(common::ObObj& obj, const common::ObObjType type, 
                  const int64_t value);
      int do_insert();
      int batch_apply();

      void display_internal_status();

    private:
      static const int64_t DEFAULT_INSERT_ROW_COUNT_ONCE = 2000;

      DISALLOW_COPY_AND_ASSIGN(ObWriteWorker);

      client::ObClient& client_;
      ObOlapdriveMeta& meta_;
      ObOlapdriveSchema& schema_;
      ObOlapdriveParam& param_;
      ObOlapdriveStat& stat_;
      ObKeyMeta* rowkey_meta_;
      common::ObMutator mutator_;
      int64_t mutator_row_cnt_;

      ObDateStatus date_status_;
      ObLZRowkey cur_rowkey_;
      common::ObRowkey cur_lz_rowkey_;
      common::ObObj rowkey_[MAX_OLAPDRIVE_ROWKEY_COLUMN_COUNT];

      ObCampaignStatus campaign_status_;
      ObAdgroupStatus adgroup_status_;

      ObCampaignRowDist row_dist_;       //row distribution is the same as adgroup
      ObCampaignAdgroupDist adgroup_dist_;
    };
  } // end namespace olapdrive,
} // end namespace oceanbase

#endif //OCEANBASE_OLAPDRIVE_WRITE_WORKER_H
